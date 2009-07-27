//-----------------------------------------------------------------------------
// rq-http
//	Service that accepts Http connections and passes control to particular
//	queues.
//-----------------------------------------------------------------------------


#include <rq-http.h>

// includes
#include <assert.h>
#include <event.h>
#include <event2/listener.h>
#include <expbuf.h>
#include <linklist.h>
#include <rq.h>
#include <rq-blacklist.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if (RQ_HTTP_VERSION != 0x00000100)
	#error "Compiling against incorrect version of rq-http.h"
#endif


#define PACKAGE						"rq-http"
#define VERSION						"1.0"


typedef struct {
	struct event_base *evbase;
	rq_service_t      *rqsvc;
	list_t            *servers;

	expbuf_t *readbuf;
	int conncount;
	int maxconns;

	struct event *sigint_event;
	struct event *sighup_event;

	rq_blacklist_t *blacklist;
} control_t;


typedef struct {
	struct evconnlistener *listener;
	list_t *clients;
	control_t *control;
} server_t;


typedef struct {
	evutil_socket_t handle;
	struct event *read_event;
	struct event *write_event;
	server_t *server;
	expbuf_t *pending;
	rq_blacklist_id_t blacklist_id;
} client_t;


// Pre-declare our handlers, because often they need to interact with functions that are used to invoke them.
static void accept_conn_cb(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx);
static void read_handler(int fd, short int flags, void *arg);



//-----------------------------------------------------------------------------
// Create and init our controller object.   The  control object will have
// everything in it that is needed to run this server.  It is in this object
// because we need to pass a pointer to the handler that will be doing the work.
static void init_control(control_t *control)
{
	assert(control != NULL);

	control->rqsvc = NULL;
	control->sigint_event = NULL;
	control->sighup_event = NULL;
	control->conncount = 0;
	control->maxconns = 1024;
	control->blacklist = NULL;
	
	control->readbuf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(control->readbuf, 2048);
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

	assert(control->conncount == 0);
	assert(control->rqsvc == NULL);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
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
	sin.sin_family = AF_INET;
	len = sizeof(sin);

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
      (struct sockaddr*)&sin, sizeof(sin));
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
			while(*argument==' ' && *argument!='\0') { argument++; }
			
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

	// what do we do when we get a blacklist callback?
	assert(0);
}



//-----------------------------------------------------------------------------
// Initialise the client structure.
static void client_init(client_t *client, server_t *server, evutil_socket_t handle, struct sockaddr *address, int socklen)
{
	assert(client);
	assert(server);


	assert(handle > 0);
	client->handle = handle;
	
	client->read_event = NULL;
	client->write_event = NULL;
	client->server = server;

	client->pending = NULL;

	// add the client to the list for the server.
	assert(server->clients);
	ll_push_tail(server->clients, client);

	// assign fd to client object.
	assert(server->control);
	assert(server->control->evbase);
	client->read_event = event_new(server->control->evbase, handle, EV_READ|EV_PERSIST, read_handler, client);
	assert(client->read_event);
	event_add(client->read_event, NULL);

// 	client->blacklist_state = 1;
	if (server->control->blacklist) {
		client->blacklist_id = rq_blacklist_check(server->control->blacklist, address, socklen, blacklist_handler, client);
	}
	else {
		client->blacklist_id = 0;
	}
}


//-----------------------------------------------------------------------------
// accept an http connection.  Create the client object, and then attach the
// file handle to it.
static void accept_conn_cb(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx)
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
	rq_shutdown(control->rqsvc->rq);

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
	assert(client);

	if (client->handle != INVALID_HANDLE) {
		EVUTIL_CLOSESOCKET(client->handle);
		client->handle = INVALID_HANDLE;
	}

	if (client->read_event) {
		event_free(client->read_event);
		client->read_event = NULL;
	}
	
	if (client->write_event) {
		event_free(client->write_event);
		client->write_event = NULL;
	}

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
	
	if (client->blacklist_id) {
		assert(client->server);
		assert(client->server->control);
		assert(client->server->control->blacklist);
		rq_blacklist_cancel(client->server->control->blacklist, client->blacklist_id);
		client->blacklist_id = 0;
	}
	
	client->server = NULL;
}


static void read_handler(int fd, short int flags, void *arg)
{
	client_t *client = (client_t *) arg;
	expbuf_t *in;
	int res;
	control_t *control;

	assert(fd >= 0);
	assert(flags != 0);
	assert(client);
	assert(client->handle == fd);

	assert(client->server);
	assert(client->server->control);
	control = client->server->control;
	
	assert(control->readbuf);
	in = control->readbuf;

	/// read data from the socket.
	assert(BUF_LENGTH(in) == 0 && BUF_MAX(in) > 0 && BUF_DATA(in) != NULL);
	res = read(fd, BUF_DATA(in), BUF_MAX(in));
	if (res > 0) {
		/// got some data.
		assert(res <= BUF_MAX(in));
		BUF_LENGTH(in) = res;
	
		// if we got the max of the buffer, then we should increase the size of the buffer.
		if (res == BUF_MAX(in)) {
			expbuf_shrink(in, 1024);	// give the buffer an extra kb.
			assert(BUF_MAX(in) > BUF_LENGTH(in));
		}
	}
	else {
		/// the connection was closed, or there was an error.

		// remove the client from the list in the server object.
		assert(0);

		// free the client resources.
		client_free(client);
		
		assert(0);

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

	/// process the data that we have not yet processed.
	assert(0);
	
// 	#error "incomplete"

// params->query = strdup(uri);
//   if (params->query) {
 
//     argument = params->query;
 
    /* We already know that there has to be a ? */
//     strsep(&argument, "?");
 
//     next = argument;
//     while (next != NULL && *next != '\0') {
//       argument = strsep(&next, "&");
	



	
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
	rq_svc_setoption(service, 'b', "queue", "Queue to send blacklist requests.");
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

	// we are done, cleanup what is left in the control structure.
	cleanup_control(control);
	free(control);

	rq_svc_cleanup(service);

	return 0;
}


