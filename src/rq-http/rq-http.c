//-----------------------------------------------------------------------------
// rq-http
//	Service that accepts Http connections and passes control to particular
//	queues.
//-----------------------------------------------------------------------------


// includes
// #include <asm-generic/errno.h>
#include <assert.h>
#include <errno.h>
#include <event.h>
#include <event2/listener.h>
#include <expbuf.h>
#include <linklist.h>
#include <risp.h>
#include <rq.h>
#include <rq-blacklist.h>
#include <rq-http.h>
#include <rq-http-config.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <unistd.h>

#if (RQ_HTTP_VERSION != 0x00000300)
	#error "Compiling against incorrect version of rq-http.h"
#endif

#if (RQ_HTTP_CONFIG_VERSION < 0x00011500)
	#error "Requires librq-http-config v1.15 or higher."
#endif



#define PACKAGE						"rq-http"
#define VERSION						"1.0"


#define DEFAULT_EXPIRES 300
#define DEFAULT_BUFSIZE	4096

typedef struct {
	struct event_base *evbase;
	rq_service_t *rqsvc;
	list_t *servers;
	risp_t *risp;

	expbuf_t *readbuf;
	int conncount;
	int maxconns;

	struct event *sigint_event;
	struct event *sighup_event;

	rq_blacklist_t *blacklist;
	rq_hcfg_t *cfg;
} control_t;


typedef struct {
	struct evconnlistener *listener;
	list_t *clients;
	control_t *control;
} server_t;


typedef struct {
	char *name;
	char *value;
} header_t;


typedef struct {
	evutil_socket_t handle;
	struct event *read_event;
	struct event *write_event;
	server_t *server;
	expbuf_t *pending;
	expbuf_t *outbuffer;
	int out_sent;
	
	rq_blacklist_id_t blacklist_id;
	enum {
		bl_unchecked,
		bl_checking,
		bl_accept,
		bl_deny
	} blacklist_result;

	rq_hcfg_id_t cfg_id;
	enum {
		cfg_unchecked,
		cfg_checking,
		cfg_checked
	} cfg_result;

	// data used to process the incoming requests.
	enum {
		state_starting,			/* havent processed anything yet, start at the first line. */
		state_headers,      /* processing headers. */
		state_data,         /* have receieved 'length', must process data. */
		state_done,         /* finished receiving everything */
		state_sent          /* sent request to queue */
	} state;

	char *method;
	char *path;
	char *leftover;
	char *version;
	expbuf_t *params;
	char *host;
	int length;
	char *queue;

	list_t *headers;		/// header_t

	expbuf_t *filedata;
	expbuf_t *content_type;
} client_t;



//-----------------------------------------------------------------------------
// Pre-declare our handlers, because often they need to interact with functions
// that are used to invoke them.
static void accept_conn_cb(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx);
static void read_handler(int fd, short int flags, void *arg);


//-----------------------------------------------------------------------------
// Create and init our controller object.   The  control object will have
// everything in it that is needed to run this server.  It is in this object
// because we need to pass a pointer to the handler that will be doing the work.
static void init_control(control_t *control)
{
	assert(control != NULL);

	control->evbase = NULL;
	control->rqsvc = NULL;
	control->sigint_event = NULL;
	control->sighup_event = NULL;
	control->conncount = 0;
	control->maxconns = 1024;
	control->blacklist = NULL;
	control->risp = NULL;
	
	control->readbuf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(control->readbuf, DEFAULT_BUFSIZE);
}

static void cleanup_control(control_t *control)
{
	assert(control != NULL);

	assert(control->readbuf);
	expbuf_free(control->readbuf);
	free(control->readbuf);
	control->readbuf = NULL;

	if (control->blacklist) {
		rq_blacklist_free(control->blacklist);
		free(control->blacklist);
		control->blacklist = NULL;
	}

	assert (control->cfg);
	rq_hcfg_free(control->cfg);
	free(control->cfg);
	control->cfg = NULL;

	assert(control->risp == NULL);
	assert(control->conncount == 0);
	assert(control->rqsvc == NULL);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
	assert(control->evbase == NULL);
}


//-----------------------------------------------------------------------------
// Initialise the server object.
static void server_init(server_t *server, control_t *control)
{
	assert(server);
	assert(control);

	server->control = control;

	assert(control->maxconns > 0);
	assert(control->conncount == 0);

	server->clients = (list_t *) malloc(sizeof(list_t));
	ll_init(server->clients);
}


//-----------------------------------------------------------------------------
// Listen for socket connections on a particular interface.
static void server_listen(server_t *server, char *interface)
{
	struct sockaddr_in sin;
	int len;
	
	assert(server);
	assert(interface);

	memset(&sin, 0, sizeof(sin));
// 	sin.sin_family = AF_INET;
	len = sizeof(sin);

	assert(sizeof(struct sockaddr_in) == sizeof(struct sockaddr));
	if (evutil_parse_sockaddr_port(interface, (struct sockaddr *)&sin, &len) != 0) {
		assert(0);
	}
	else {

		assert(server->listener == NULL);
    assert(server->control);
    assert(server->control->evbase);

    server->listener = evconnlistener_new_bind(
    	server->control->evbase,
    	accept_conn_cb,
    	server,
			LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
			-1,
      (struct sockaddr*)&sin,
      sizeof(sin)
		);
		assert(server->listener);
	}	
}




static void init_servers(control_t *control)
{
	server_t *server;
	char *str;
	char *copy;
	char *next;
	char *argument;
	
	assert(control);
	assert(control->servers == NULL);
	
	control->servers = (list_t *) malloc(sizeof(list_t));
	ll_init(control->servers);

	assert(control->rqsvc);
	str = rq_svc_getoption(control->rqsvc, 'l');
	if (str == NULL) {
		fprintf(stderr, "Require -l interface parameters.\n");
	}

	// make a copy of the supplied string, because we will be splitting it into
	// its key/value pairs. We dont want to mangle the string that was supplied.
	assert(str);
	copy = strdup(str);
	assert(copy);

	next = copy;
	while (next != NULL && *next != '\0') {
		argument = strsep(&next, ",");
		if (argument) {
		
			// remove spaces from the begining of the key.
			while(*argument==' ') { argument++; }
			
			if (strlen(argument) > 0) {
				server = (server_t *) malloc(sizeof(server_t));
				server_init(server, control);
				ll_push_head(control->servers, server);
		
				server_listen(server, argument);
			}
		}
	}
	
	free(copy);
}


//-----------------------------------------------------------------------------
// Cleanup the server object.
static void server_free(server_t *server)
{
	assert(server);

	assert(server->clients);
	assert(ll_count(server->clients) == 0);
	ll_free(server->clients);
	free(server->clients);
	server->clients = NULL;

	if (server->listener) {
		evconnlistener_free(server->listener);
		server->listener = NULL;
	}

	assert(server->control);
	server->control = NULL;
}


static void cleanup_servers(control_t *control)
{
	server_t *server;
	
	assert(control);
	assert(control->servers);

	while ((server = ll_pop_head(control->servers))) {
		server_free(server);
		free(server);
	}

	ll_free(control->servers);
	free(control->servers);
	control->servers = NULL;
}



static void blacklist_handler(rq_blacklist_status_t status, void *arg)
{
	client_t *client = (client_t *) arg;
	assert(client);

	assert(client->blacklist_result == bl_checking);
	if (status == BLACKLIST_ACCEPT) {
		client->blacklist_result = bl_accept;
	}
	else {
		client->blacklist_result = bl_deny;
	}
	client->blacklist_id = 0;
}



//-----------------------------------------------------------------------------
// Initialise the client structure.
static void client_init (
	client_t *client,
	server_t *server,
	evutil_socket_t handle,
	struct sockaddr *address,
	int socklen)
{
	assert(client);
	assert(server);

	assert(handle > 0);
	client->handle = handle;
	
	fprintf(stderr, "New client - handle=%d\n", handle);
	
	client->read_event = NULL;
	client->write_event = NULL;
	client->server = server;

	client->pending = NULL;

	// add the client to the list for the server.
	assert(server->clients);
	ll_push_tail(server->clients, client);

	client->method = NULL;
	client->path = NULL;
	client->leftover = NULL;
	client->version = NULL;
	client->params = NULL;
	client->host = NULL;
	client->state = state_starting;
	client->length = 0;

	client->headers = (list_t *) malloc(sizeof(list_t));
	ll_init(client->headers);

	// assign fd to client object.
	assert(server->control);
	assert(server->control->evbase);
	assert(client->handle > 0);
	client->read_event = event_new(
		server->control->evbase,
		client->handle,
		EV_READ|EV_PERSIST,
		read_handler,
		client);
	assert(client->read_event);
	event_add(client->read_event, NULL);

	client->cfg_id = 0;
	client->cfg_result = cfg_unchecked;
	client->queue = NULL;

	client->blacklist_result = bl_unchecked;
	if (server->control->blacklist) {
		client->blacklist_result = bl_checking;
		client->blacklist_id = rq_blacklist_check(
			server->control->blacklist,
			address,
			socklen,
			blacklist_handler,
			client);
	}
	else {
		client->blacklist_id = 0;
	}

	client->filedata = NULL;
	client->content_type = NULL;
	client->outbuffer = NULL;
	client->out_sent = 0;
}


//-----------------------------------------------------------------------------
// accept an http connection.  Create the client object, and then attach the
// file handle to it.
static void accept_conn_cb(
	struct evconnlistener *listener,
	evutil_socket_t fd,
	struct sockaddr *address,
	int socklen,
	void *ctx)
{
	client_t *client;
	server_t *server = (server_t *) ctx;

	assert(listener);
	assert(fd > 0);
	assert(address && socklen > 0);
	assert(ctx);
	assert(server);

	// create client object.
	// TODO: We should be pulling these client objects out of a mempool.
	client = (client_t *) malloc(sizeof(client_t));
	client_init(client, server, fd, address, socklen);
}




//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	// need to initiate an RQ shutdown.
	assert(control);
	assert(control->rqsvc);
	assert(control->rqsvc->rq);
	rq_svc_shutdown(control->rqsvc);

	// delete the signal events.
	assert(control->sigint_event);
	event_free(control->sigint_event);
	control->sigint_event = NULL;

	assert(control->sighup_event);
	event_free(control->sighup_event);
	control->sighup_event = NULL;
}


//-----------------------------------------------------------------------------
// When SIGHUP is received, we need to re-load the config database.  At the
// same time, we should flush all caches and buffers to reduce the system's
// memory footprint.   It should be as close to a complete app reset as
// possible.
static void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
//  	control_t *control = (control_t *) arg;

	assert(arg);

	// clear out all cached objects.
	assert(0);

	// reload the config database file.
	assert(0);

}



//-----------------------------------------------------------------------------
// Free the resources used by the client object.
static void client_free(client_t *client)
{
	header_t *header;
	assert(client);

	fprintf(stderr, "client_free: handle=%d\n", client->handle);

	if (client->outbuffer) {
		expbuf_clear(client->outbuffer);
		expbuf_free(client->outbuffer);
		free(client->outbuffer);
		client->outbuffer = NULL;
	}
		
	assert(client->filedata == NULL);
	assert(client->content_type == NULL);

	if (client->read_event) {
		event_free(client->read_event);
		client->read_event = NULL;
	}
	
	if (client->write_event) {
		event_free(client->write_event);
		client->write_event = NULL;
	}

	if (client->handle != INVALID_HANDLE) {
		EVUTIL_CLOSESOCKET(client->handle);
		client->handle = INVALID_HANDLE;
	}

	if (client->method)   { free(client->method);   client->method = NULL;   }
	if (client->path)     { free(client->path);     client->path = NULL;     }
	if (client->leftover) { free(client->leftover); client->leftover = NULL; }
	if (client->version)  { free(client->version);  client->version = NULL;  }
	if (client->host)     { free(client->host);     client->host = NULL;     }

	if (client->params) {
		assert(client->server);
		assert(client->server->control);
		assert(client->server->control->rqsvc);
		assert(client->server->control->rqsvc->rq);
		assert(client->server->control->rqsvc->rq->bufpool);
		expbuf_clear(client->params);
		expbuf_pool_return(client->server->control->rqsvc->rq->bufpool, client->params);
		client->params = NULL;
	}

	assert(client->headers);
	while((header = ll_pop_head(client->headers))) {
		assert(header->name);
		assert(header->value);
		free(header->name);
		free(header->value);
		free(header);
	}
	ll_free(client->headers);
	free(client->headers);
	client->headers = NULL;
	
	if (client->pending) {
		assert(client->server);
		assert(client->server->control);
		assert(client->server->control->rqsvc);
		assert(client->server->control->rqsvc->rq);
		assert(client->server->control->rqsvc->rq->bufpool);
		expbuf_clear(client->pending);
		expbuf_pool_return(client->server->control->rqsvc->rq->bufpool, client->pending);
		client->pending = NULL;
	}
	
	if (client->blacklist_id > 0) {
		assert(client->server);
		assert(client->server->control);
		assert(client->server->control->blacklist);
		assert(client->blacklist_result == bl_checking);
		rq_blacklist_cancel(client->server->control->blacklist, client->blacklist_id);
		client->blacklist_id = 0;
		client->blacklist_result = bl_unchecked;
	}

	// remove the client from the server list.
	assert(client->server);
	assert(client->server->clients);
	assert(ll_count(client->server->clients) > 0);
	ll_remove(client->server->clients, client);

	client->server = NULL;

	if (client->queue) {
		free(client->queue);
		client->queue = NULL;
	}
}


static void client_reset(client_t *client)
{
	header_t *header;

	assert(client);
	assert(client->handle > 0);
	assert(client->read_event);
	assert(client->write_event == NULL);
	assert(client->server);
	
	fprintf(stderr, "Resetting client: fd=%d\n", client->handle);
	
	if (client->pending) {
		assert(BUF_LENGTH(client->pending) == 0);
	}

// 	assert(client->blacklist_id == 0);
// 	client->blacklist_result = bl_unchecked;

	assert(client->cfg_id == 0);
	client->cfg_result = cfg_unchecked;

	client->state = state_starting;
	
	if (client->method)   { free(client->method);   client->method = NULL;   }
	if (client->path)     { free(client->path);     client->path = NULL;     }
	if (client->leftover) { free(client->leftover); client->leftover = NULL; }
	if (client->version)  { free(client->version);  client->version = NULL;  }
	if (client->host)     { free(client->host);     client->host = NULL;     }
	
	if (client->params) {
		expbuf_clear(client->params);
		expbuf_free(client->params);
		free(client->params);
		client->params = NULL;
	}
	
	assert(client->length == 0);
	
	if (client->queue) {
		free(client->queue);
		client->queue = NULL;
	}

	// clear the headers, but keep the list.
	assert(client->headers);
	while ((header = ll_pop_head(client->headers))) {
		assert(header->name);
		assert(header->value);

		free(header->name);
		free(header->value);
		free(header);
	}

	assert(client->filedata == NULL);
	assert(client->content_type == NULL);

	assert(client->read_event);
	assert(client->write_event == NULL);
}


static void cmdInvalid(void *ptr, void *data, risp_length_t len)
{
	// this callback is called if we have an invalid command.  We shouldn't be receiving any invalid commands.
	unsigned char *cast;

	assert(ptr != NULL);
	assert(data != NULL);
	assert(len > 0);
	
	cast = (unsigned char *) data;
	printf("Received invalid (%d)): [%d, %d, %d]\n", len, cast[0], cast[1], cast[2]);
	assert(0);
}


static void cmdClear(client_t *client) {
	assert(client);

	if (client->filedata) {
		expbuf_clear(client->filedata);
		expbuf_free(client->filedata);
		free(client->filedata);
		client->filedata = NULL;
	}
	assert(client->filedata == NULL);
}



//-----------------------------------------------------------------------------
// when the write event fires, we will try to write everything to the socket.
// If everything has been sent, then we will remove the write_event, and the
// outbuffer.
static void write_handler(int fd, short int flags, void *arg)
{
	client_t *client;
	int res;
	int length;
	
	assert(fd > 0);
	assert(arg);

	client = arg;

	assert(client->write_event);
	assert(client->outbuffer);
	assert(client->out_sent < BUF_LENGTH(client->outbuffer));

	length = BUF_LENGTH(client->outbuffer) - client->out_sent;

	fprintf(stderr, "write_handler: length=%d, sent=%d, tosend=%d\n", BUF_LENGTH(client->outbuffer), client->out_sent, length);
	
	res = send(client->handle, BUF_DATA(client->outbuffer)+client->out_sent, length, 0);
	if (res > 0) {
		assert(res <= length);
		client->out_sent += res;

		assert(client->out_sent <= BUF_LENGTH(client->outbuffer));
		if (client->out_sent == BUF_LENGTH(client->outbuffer)) {
			// we've sent everything....

			fprintf(stderr, "write_handler: All sent.\n");

			// everything has been sent from the outbuffer, so we can clear it.
			expbuf_clear(client->outbuffer);
			expbuf_free(client->outbuffer);
			free(client->outbuffer);
			client->outbuffer = NULL;
			client->out_sent = 0;

			// all data has been sent, so we clear the write event.
			assert(client->write_event);
			event_free(client->write_event);
			client->write_event = NULL;
			assert(client->read_event);
			
			// reset the client in case it sends another request.
			client_reset(client);
		}
	}
	else 	if (res == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
		// the connection has closed, so we need to clean up.
		client_free(client);
	}
}



//-----------------------------------------------------------------------------
// We've gotten a reply from the http consumer.  We need to take that data and
// form a reply that we send.
static void cmdReply(client_t *client) {
	assert(client);
	assert(client->filedata);
	assert(client->content_type);

	// all output needs to go in the outbuffer.
	assert(client->outbuffer == NULL);
	assert(client->out_sent == 0);
	client->outbuffer = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(client->outbuffer, 1024);

	expbuf_print(client->outbuffer, "HTTP/1.1 200 OK\r\n");
	expbuf_print(client->outbuffer, "Content-Length: %d\r\n", BUF_LENGTH(client->filedata));
	expbuf_print(client->outbuffer, "Keep-Alive: timeout=5, max=100\r\n");
	expbuf_print(client->outbuffer, "Connection: Keep-Alive\r\n");
	expbuf_print(client->outbuffer, "Content-Type: %s\r\n\r\n", expbuf_string(client->content_type));

	expbuf_add(client->outbuffer, BUF_DATA(client->filedata), BUF_LENGTH(client->filedata));
// 	expbuf_print(client->outbuffer, "\r\n");

// Date: Thu, 03 Sep 2009 21:49:33 GMT
// Server: Apache/2.2.11 (Unix)
// Last-Modified: Thu, 03 Sep 2009 21:47:59 GMT
// ETag: "758017-95-472b3564641c0"
// Accept-Ranges: bytes
// Vary: Accept-Encoding,User-Agent


	// create the write_event (with persist).  We will not try to send it
	// directly to the socket.  We will wait until the write_event fires.
	assert(client->write_event == NULL);
	assert(client->server);
	assert(client->server->control);
	assert(client->server->control->rqsvc);
	assert(client->server->control->rqsvc->rq);
	assert(client->server->control->rqsvc->rq->evbase);
	assert(client->handle > 0);
	client->write_event = event_new(client->server->control->rqsvc->rq->evbase, client->handle, EV_WRITE | EV_PERSIST, write_handler, client);
	event_add(client->write_event, NULL);

	// PERF: Put this back in the bufpool.
	assert(client->filedata);
	expbuf_clear(client->filedata);
	expbuf_free(client->filedata);
	free(client->filedata);
	client->filedata = NULL;

	assert(client->content_type);
	expbuf_clear(client->content_type);
	expbuf_free(client->content_type);
	free(client->content_type);
	client->content_type = NULL;	
}


static void cmdContentType(client_t *client, risp_length_t length, risp_char_t *data)
{
	assert(client);
	assert(length >= 0);
	assert(data != NULL);

	// save the host info somewhere
	assert(client->content_type == NULL);
	client->content_type = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(client->content_type, length+1);
	expbuf_set(client->content_type, data, length);
}



static void cmdFile(client_t *client, risp_length_t length, risp_char_t *data)
{
	assert(client);
	assert(length >= 0);
	assert(data != NULL);

	// PERF: get the filedata buffer from the buff pool.

	// save the host info somewhere
	assert(client->filedata == NULL);
	client->filedata = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(client->filedata, length);
	expbuf_set(client->filedata, data, length);
}



static void http_handler(rq_message_t *msg)
{
	client_t *client;
	int processed;

	assert(msg);
	client = msg->arg;
	assert(client);

	assert(client->server);
	assert(client->server->control);
	assert(client->server->control->risp);
	assert(msg->data);
	assert(BUF_LENGTH(msg->data));
	processed = risp_process(client->server->control->risp, client, BUF_LENGTH(msg->data), (risp_char_t *) BUF_DATA(msg->data));
	assert(processed == BUF_LENGTH(msg->data));
}



//-----------------------------------------------------------------------------
// This function is used to send the request to the queue.  By this time we
// should have obtained the queue from the config service (or local cache), and
// loaded all the headers and data.
static void send_request(client_t *client)
{
	rq_message_t *msg;

	assert(client);
	assert(client->handle > 0);
	assert(client->server);
	
	assert(client->blacklist_result != bl_deny);
	assert(client->cfg_result == cfg_checked);
	assert(client->state == state_done);

	assert(client->method);
	assert(client->path);
	assert(client->version);
	assert(client->host);
	assert(client->headers);
	assert(ll_count(client->headers));

	// do we have queue?
	assert(client->queue);

	// get a new message object.
	assert(client->server);
	assert(client->server->control);
	assert(client->server->control->rqsvc);
	assert(client->server->control->rqsvc->rq);
	msg = rq_msg_new(client->server->control->rqsvc->rq, NULL);
	assert(msg);
	assert(msg->data);

	// apply the queue that we are sending a request for.
	rq_msg_setqueue(msg, client->queue);

	// build the command payload.
	rq_msg_addcmd(msg, HTTP_CMD_CLEAR);
	
	assert(client->method);
	assert(client->method[0] != '\0');
	if (client->method[0] == 'G' || client->method[0] == 'g') {
		rq_msg_addcmd(msg, HTTP_CMD_METHOD_GET);
	}
	else if (client->method[0] == 'P' || client->method[0] == 'p') {
		rq_msg_addcmd(msg, HTTP_CMD_METHOD_POST);
	}
	else if (client->method[0] == 'H' || client->method[0] == 'h') {
		rq_msg_addcmd(msg, HTTP_CMD_METHOD_HEAD);
	}
	
                                            // short string (160 to 192)
// #define HTTP_CMD_REMOTE_HOST      161
// #define HTTP_CMD_LANGUAGE         162
                                            // string (192 to 223)

	assert(client->host);
	rq_msg_addcmd_str(msg, HTTP_CMD_HOST, strlen(client->host), client->host);

	if (client->leftover) {
		rq_msg_addcmd_str(msg, HTTP_CMD_PATH, strlen(client->leftover), client->leftover);
	}
	else {
		rq_msg_addcmd_str(msg, HTTP_CMD_PATH, 1, "/");
	}

	// Send the params.
	if (client->params) {
		assert(BUF_LENGTH(client->params) > 0);
		rq_msg_addcmd_str(msg, HTTP_CMD_PARAMS, BUF_LENGTH(client->params), BUF_DATA(client->params));
	}
	
	rq_msg_addcmd(msg, HTTP_CMD_EXECUTE);

	fprintf(stderr, "sending HTTP request to '%s'.  data.len=%d\n", client->queue, BUF_LENGTH(msg->data));

	// TODO: set a timeout on the request.

	// message has been prepared, so send it.
	// TODO: add fail handler.
	rq_send(msg, http_handler, NULL, client);
	msg = NULL;

	client->state = state_sent;
}



static void proc_request(client_t *client, char *line)
{
	char *next;
	char *arg;
	char *fullpath;
	int len;
	
	assert(client);
	assert(line);

	assert(client->state == state_starting);
	assert(client->method == NULL);
	assert(client->path == NULL);
	assert(client->version == NULL);

	/// TODO: this code will assert when receiving malformed requests, it needs
	///       to be made more solid so that it closes the connection if it gets
	///       something odd.

	fullpath = NULL;
	next = line;
	while (next != NULL && *next != '\0') {
		assert(client->version == NULL);
		arg = strsep(&next, " ");
		if (client->method == NULL) {
			client->method = strdup(arg);			// TODO: would it be better to use mempool instead?
			assert(client->path == NULL);
			assert(client->version == NULL);
		}
		else if (fullpath == NULL) {
			fullpath = strdup(arg);						// TODO: would it be better to use mempool instead?
			assert(client->version == NULL);
		}
		else {
			client->version = strdup(arg);
		}
	}

	assert(client->method);
	assert(client->path == NULL);
	assert(client->version);

	fprintf(stderr, "\nMethod: %s\n", client->method);
	fprintf(stderr, "FullPath: %s\n", fullpath);
	fprintf(stderr, "Version: %s\n", client->version);

	// process the path param that was supplied.
	assert(fullpath);
	next = fullpath;
	while (next != NULL && *next != '\0') {
		assert(client->params == NULL);
		arg = strsep(&next, "?");
		if (client->path == NULL) {
			client->path = strdup(arg);
			assert(client->params == NULL);
		}
		else {
			len = strlen(arg);
			assert(len > 0);

			assert(client->server);
			assert(client->server->control);
			assert(client->server->control->rqsvc);
			assert(client->server->control->rqsvc->rq);
			assert(client->server->control->rqsvc->rq->bufpool);
			client->params = expbuf_pool_new(client->server->control->rqsvc->rq->bufpool, len);
			expbuf_set(client->params, arg, len);
		}
	}

	assert(client->path);

	assert(fullpath);
	free(fullpath);
	fullpath = NULL;

	fprintf(stderr, "Path: %s\n", client->path);
	if (client->params) fprintf(stderr, "Params: %s\n", expbuf_string(client->params));

	client->state = state_headers;
}


static void proc_header(client_t *client, char *line)
{
	char *next;
	char *arg;
	char *tmp;
	header_t *header;
	
	assert(client);
	assert(line);

	assert(client->state == state_headers);
	assert(client->method);
	assert(client->path);
	assert(client->version);
	assert(client->headers);
 
	/// TODO: this code will assert when receiving malformed requests, it needs
	///       to be made more solid so that it closes the connection if it gets
	///       something odd.

	// we will be processing each headers line.  We will be creating a new
	// header_t element for each one, and then adding it to the list.  We will
	// be seperating each line by ':' and then stripping out any spaces at the
	// start of the value.

// 	fprintf(stderr, "LINE: %s\n", line);


	header = NULL;
	next = line;
	arg = strsep(&next, ":");
	assert(arg);
	if (arg[0] == '\r' || arg[0] == '\n') {
		// we've reached the end of the headers.
		if (client->length > 0) { client->state = state_data; }
		else { client->state = state_done; }
	}
	else {
		header = (header_t *) malloc(sizeof(header_t));
		header->name = strdup(arg);
		assert(next != NULL && *next != '\0');

		if (*next == ' ') next++;
		assert(*next != '\0');
		header->value = strdup(next);
		assert(header->value);

		ll_push_tail(client->headers, header);

		// now we need to check to see if the header is one of the special ones we care about.
		if (strcasecmp(header->name, "host") == 0) {
			assert(client->host == NULL);

			// the host value can also contain the port that was sent... so we need to strip that bit out.
			tmp = strdup(header->value);
			next = tmp;
			arg = strsep(&next, ":");
			assert(arg);

			client->host = strdup(arg);
			free(tmp);

			printf("Host found: %s\n", client->host);
		}
		else if (strcasecmp(header->name, "length") == 0) {
			assert(client->length == 0);
			client->length = atoi(header->value);
			printf("Length found: %d\n", client->length);
		}
	}
}


// When config data is returned, this 
static void config_handler(
	const char *queue, const char *path, const char *leftover, const char *redirect, void *arg)
{
	client_t *client = arg;
	char *str;

	assert(client);

	fprintf(stderr, "config_handler: queue=%s, path=%s, leftover=%s, redirect=%s\n",
		queue, path, leftover, redirect);
	
	assert(client->cfg_result == cfg_checking);
	client->cfg_result = cfg_checked;
	client->cfg_id = 0;

	if (redirect) {
	
		assert(queue == NULL);
		assert(path == NULL);
		assert(leftover == NULL);

		fprintf(stderr, "REDIRECT received: %s\n", redirect);
	
		// send the redirect back to the client.  We dont want to bother
		// processing more http headers or whatever either.
		assert(0);
	}
	else if (queue == NULL) {
		// we dont have a service for that request.

		fprintf(stderr, "NOT FOUND.\n");

		// all output needs to go in the outbuffer.
		assert(client->outbuffer == NULL);
		assert(client->out_sent == 0);
		client->outbuffer = (expbuf_t *) malloc(sizeof(expbuf_t));
		expbuf_init(client->outbuffer, 1024);

		str = "404 - File not found.\r\n";
	
		assert(client->method);
		expbuf_print(client->outbuffer, "%s 404 Not Found\r\n", client->version);
		expbuf_print(client->outbuffer, "Content-Length: %d\r\n\r\n", strlen(str));
		expbuf_print(client->outbuffer, str);
		
// Date: Mon, 07 Sep 2009 22:08:39 GMT
// Server: Apache/2.2.11 (Unix)
// Vary: accept-language,accept-charset,Accept-Encoding,User-Agent
// Accept-Ranges: bytes
// Keep-Alive: timeout=5, max=100
// Connection: Keep-Alive
// Transfer-Encoding: chunked
// Content-Type: text/html; charset=iso-8859-1
// Content-Language: en
		
		// create the write_event (with persist).  We will not try to send it
		// directly to the socket.  We will wait until the write_event fires.
	
		assert(client->write_event == NULL);
		assert(client->server);
		assert(client->server->control);
		assert(client->server->control->rqsvc);
		assert(client->server->control->rqsvc->rq);
		assert(client->server->control->rqsvc->rq->evbase);
		client->write_event = event_new(
			client->server->control->rqsvc->rq->evbase,
			client->handle,
			EV_WRITE | EV_PERSIST,
			write_handler,
			client);
		event_add(client->write_event, NULL);

		// what state do we need so that we aren't processing this request any more?
		// even though we haven't sent the request, we have essentially done so.  When write handler finishes, it will reset the client.
// 		client->state = state_sent;
	}
	else {
		// store the queue,
		assert(redirect == NULL);
		assert(arg);

		assert(client->queue == NULL);
		assert(queue);
		assert(strlen(queue) < 256);
		client->queue = strdup(queue);

		assert(client->leftover == NULL);
		if (leftover) {
			client->leftover = strdup(leftover);
		}

		// if we are at a state where we have finished processing headers and
		// data, then we do a send straight away.
		if (client->state == state_done) {
			fprintf(stderr, "config_handler.  sending request to queue=%s\n", client->queue);
			send_request(client);
		}
	}
}



static void read_handler(int fd, short int flags, void *arg)
{
	client_t *client = (client_t *) arg;
	expbuf_t *in;
	int res;
	control_t *control;
	char *next;
	char *line;
	char *leftover;
	int len;

	assert(fd >= 0);
	assert(flags != 0);
	assert(client);
	assert(client->handle == fd);

	assert(client->server);
	assert(client->server->control);
	control = client->server->control;
	
	assert(control->readbuf);
	in = control->readbuf;

	// check to see if we have a blacklist result yet.
	if (client->blacklist_result == bl_deny) {

		// send a standard error reply back to the client.
		assert(0);

		// close the client connection.
		assert(0);

		// send stats information about the blocked connection.
		assert(0);

		// send log entry about the blocked request.
		assert(0);
		
		return;
	}


	// read data from the socket.
	assert(BUF_LENGTH(in) == 0 && BUF_MAX(in) > 0 && BUF_DATA(in) != NULL);
	res = read(fd, BUF_DATA(in), BUF_MAX(in));
	if (res > 0) {

		fprintf(stderr, "read %d bytes.\n", res);
	
		// got some data.
		assert(res <= BUF_MAX(in));
		BUF_LENGTH(in) = res;
	
		// if we got the max of the buffer, then we should increase the size of the buffer.
		if (res == BUF_MAX(in)) {
			expbuf_shrink(in, DEFAULT_BUFSIZE);	// give the buffer some extra kb.
			assert(BUF_MAX(in) > BUF_LENGTH(in));
		}
	}
	else {
		// the connection was closed, or there was an error.
		fprintf(stderr, "connection closed while reading.\n");

		// free the client resources.
		client_free(client);
		client = NULL;
		
		assert(BUF_LENGTH(control->readbuf) == 0);
		return;
	}
	
	// if we have some pending data, then add in to it, then point 'in' to it.
	if (client->pending) {
		assert(BUF_LENGTH(client->pending) > 0);
		expbuf_add(client->pending, BUF_DATA(in), BUF_LENGTH(in));
		expbuf_clear(in);
		in = client->pending;
	}

	// the data we are using can be modified, so we dont need to make a copy of it first.
	leftover = NULL;
	assert(in);
	assert(BUF_LENGTH(in) > 0);
	next = expbuf_string(in);
	while (next != NULL && *next != '\0') {
		assert(leftover == NULL);
		assert(client->state != state_done);
		line = strsep(&next, "\n");
		len = strlen(line);
		if (len > 0) {
			if (line[strlen(line)-1] != '\r') {
				// we have a line that is not complete,
				assert(next == NULL);
				assert(leftover == NULL);
				leftover = strdup(line);
				fprintf(stderr, "INCOMPLETE: %s\n", line);
			}
			else {
				assert(line[strlen(line)-1] == '\r');
// 				fprintf(stderr, "LINE: %s\n", line);

				// we have a line, so we need to process it.

// 				fprintf(stderr, "Current state: %d\n", client->state);
								
				switch (client->state) {
				
					case state_starting:			/* havent processed anything yet */
						proc_request(client, line);
						break;
						
					case state_headers:			/* have processed the first line, now processing headers */
						proc_header(client, line);
						break;
						
					case state_data:      /* have finished getting all the headers, anything else is data. */
						assert(0);
						break;
						
					case state_done:         /* have receieved 'length' amount of data. */
						assert(0);
						break;

					case state_sent:
						// If we have 'sent' state, then why are we processing more data?
						// What is there left to process?
						fprintf(stderr, "received after sent: \"%s\"\n", line);
						break;

					default:
						fprintf(stderr, "Incorrect state: %d\n", client->state);
						assert(0);
						break;
				}

				// if we have path and host, and have not already made the config
				// request, we need to make the config request.
				if (client->cfg_result == cfg_unchecked && client->host && client->path) {
					assert(client->cfg_id == 0);
					assert(client->server);
					assert(client->server->control);
					assert(client->server->control->cfg);
					client->cfg_result = cfg_checking;
					client->cfg_id = rq_hcfg_lookup(client->server->control->cfg, client->host, client->path, config_handler, client);
					assert(client->cfg_id > 0 || (client->cfg_id == 0 && client->cfg_result == cfg_checked));
				}
			}
		}
	}
	expbuf_clear(in);

	if (leftover) {
		len = strlen(leftover);
	
		if (client->pending == NULL) {
			assert(client->server);
			assert(client->server->control);
			assert(client->server->control->rqsvc);
			assert(client->server->control->rqsvc->rq);
			assert(client->server->control->rqsvc->rq->bufpool);
			client->pending = expbuf_pool_new(client->server->control->rqsvc->rq->bufpool, len);
			assert(client->pending);
		}
		/// add leftover to the pending queue.
		expbuf_add(client->pending, leftover, len);
	}
	else {
		if (client->pending) {

			// if we dont have any more data in the pending buffer, then we are not
			// likely to need to add any more to it, so we need to return it to the
			// bufpool.
			if (BUF_LENGTH(client->pending) == 0) {
				assert(client->server);
				assert(client->server->control);
				assert(client->server->control->rqsvc);
				assert(client->server->control->rqsvc->rq);
				assert(client->server->control->rqsvc->rq->bufpool);
				expbuf_pool_return(client->server->control->rqsvc->rq->bufpool, client->pending);
			}
		}
	}

	// if we have received a result from a config lookup, and we have finished
	// processing data, then we need to send all the info to the queue that we
	// received.
	if (client->state == state_done && client->cfg_result == cfg_checked && client->queue) {
		send_request(client);
	}
	
	
	assert(BUF_LENGTH(client->server->control->readbuf) == 0);
}




//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	control_t      *control  = NULL;
	rq_service_t   *service;
	char *queue;

///============================================================================
/// Initialization.
///============================================================================

	// create the 'control' object that will be passed to all the handlers so
	// that they have access to the information that they require.
	control = (control_t *) malloc(sizeof(control_t));
	init_control(control);

	// create new service object.
	service = rq_svc_new();
	control->rqsvc = service;

	// add the command-line options that are specific to this service.
	rq_svc_setname(service, PACKAGE " " VERSION);
	rq_svc_setoption(service, 'l', "interface:port", "interface to listen on for HTTP requests.");
	rq_svc_setoption(service, 'b', "blacklist-queue", "Queue to send blacklist requests.");
	rq_svc_setoption(service, 'C', "config-queue", "Queue to http-config requests.");
	rq_svc_process_args(service, argc, argv);
	rq_svc_initdaemon(service);

	
	assert(control->evbase == NULL);
	control->evbase = event_base_new();
	assert(control->evbase);
	rq_svc_setevbase(service, control->evbase);

	// initialise signal handlers.
	assert(control);
	assert(control->evbase);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
	control->sigint_event = evsignal_new(control->evbase, SIGINT, sigint_handler, control);
	control->sighup_event = evsignal_new(control->evbase, SIGHUP, sighup_handler, control);
	event_add(control->sigint_event, NULL);
	event_add(control->sighup_event, NULL);


	// TODO: If we lose connection to all controllers, we should stop accepting
	//       connections on the http side until we are connected??


	// Tell the rq subsystem to connect to the rq servers.  It gets its info
	// from the common paramaters that it expects.
	if (rq_svc_connect(service, NULL, NULL, NULL) != 0) {
		fprintf(stderr, "Require a controller connection.\n");
		exit(1);
	}

	// initialise the blacklist library and control structure.
	queue = rq_svc_getoption(service, 'b');
	if (queue) {
		assert(control->blacklist == NULL);
		assert(control->rqsvc);
		assert(control->rqsvc->rq);
		control->blacklist = (rq_blacklist_t *) malloc(sizeof(rq_blacklist_t));
		rq_blacklist_init(control->blacklist, control->rqsvc->rq, queue, DEFAULT_EXPIRES);
	}
	
	queue = rq_svc_getoption(service, 'C');
	if (queue) {
		assert(control->cfg == NULL);
		assert(control->rqsvc);
		assert(control->rqsvc->rq);
		control->cfg = (rq_hcfg_t *) malloc(sizeof(rq_hcfg_t));
		rq_hcfg_init(control->cfg, control->rqsvc->rq, queue, DEFAULT_EXPIRES);
	}
	else {
		fprintf(stderr, "Require http-config queue (-C).\n");
	}

	assert(control);
	assert(control->risp == NULL);
	control->risp = risp_init();
	assert(control->risp != NULL);
	risp_add_invalid(control->risp, cmdInvalid);
	risp_add_command(control->risp, HTTP_CMD_CLEAR, 	     &cmdClear);
	risp_add_command(control->risp, HTTP_CMD_FILE,         &cmdFile);
	risp_add_command(control->risp, HTTP_CMD_CONTENT_TYPE, &cmdContentType);
 	risp_add_command(control->risp, HTTP_CMD_REPLY,        &cmdReply);


	
	// initialise the servers that we listen on.
	init_servers(control);
	

///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the processing loop.  This function will not return until there is
	// nothing more to do and the service has shutdown.  Therefore everything
	// needs to be setup and running before this point.  Once inside the
	// rq_process function, everything is initiated by the RQ event system.
	assert(control != NULL);
	assert(control->evbase);
	event_base_loop(control->evbase, 0);

///============================================================================
/// Shutdown
///============================================================================

	assert(control);
	assert(control->evbase);
	event_base_free(control->evbase);
	control->evbase = NULL;

	// cleanup the servers objects.
	cleanup_servers(control);
	assert(0);

	// the rq service sub-system has no real way of knowing when the event-base
	// has been cleared, so we need to tell it.
	rq_svc_setevbase(service, NULL);

	// make sure signal handlers have been cleared.
	assert(control);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);

	assert(control);
	assert(control->risp);
	risp_shutdown(control->risp);
	control->risp = NULL;

	// we are done, cleanup what is left in the control structure.
	cleanup_control(control);
	free(control);

	rq_svc_cleanup(service);

	return 0;
}


