//-----------------------------------------------------------------------------
// rq-http
//	Service that accepts Http connections and passes control to particular
//	queues.
//-----------------------------------------------------------------------------


#include <rq-http.h>

// includes
#include <assert.h>
#include <event.h>
#include <expbuf.h>
#include <linklist.h>
#include <rq.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define PACKAGE						"rq-http"
#define VERSION						"1.0"



typedef struct {
	// running params
	short verbose;
	short daemonize;
	char *username;
	char *pid_file;

	// connections to the controllers.
	list_t *controllers;

	// TODO: do we want this as a simple string, or as a list?
	list_t *interfaces;

	// unique settings.
	char *configfile;
	char *logqueue;
	char *statsqueue;
	char *gzipqueue;
	char *blacklist;
} settings_t;



typedef struct {
	struct event_base *evbase;
	rq_t              *rq;
	settings_t        *settings;
	list_t            *servers;
	expbuf_pool_t     *bufpool;

	expbuf_t *readbuf;
	int conncount;

	struct event *sigint_event;
	struct event *sighup_event;
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
	blacklist_id_t blacklist_id;
} client_t;



//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-f <filename>       Sqlite3 config file.\n");
	printf("-l <interface:port> interface to listen on.\n");
	printf("\n");
	printf("-c <ip:port>        Controller to connect to.\n");
	printf("\n");
	printf("-d                  run as a daemon\n");
	printf("-P <file>           save PID in <file>, only used with -d option\n");
	printf("-u <username>       assume identity of <username> (only when run as root)\n");
	printf("\n");
	printf("-v                  verbose (print errors/warnings while in event loop)\n");
	printf("-h                  print this help and exit\n");
	return;
}







//-----------------------------------------------------------------------------
// Create and init our controller object.   The  control object will have
// everything in it that is needed to run this server.  It is in this object
// because we need to pass a pointer to the handler that will be doing the work.
static void init_control(control_t *control)
{
	assert(control != NULL);

	control->rq = NULL;
	control->settings = NULL;
	control->bufpool = NULL;

	control->sigint_event = NULL;
	control->sighup_event = NULL;
	control->sigusr1_event = NULL;
	control->sigusr2_event = NULL;

	control->conncount = 0;
	
	control->readbuf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(control->readbuf, 2048);
}

static void cleanup_control(control_t *control)
{
	assert(control != NULL);

	assert(control->conncount == 0);

	assert(control->readbuf);
	expbuf_free(control->readbuf);
	free(control->readbuf);
	control->readbuf = NULL;

	assert(control->bufpool == NULL);
	assert(control->settings == NULL);
	assert(control->rq == NULL);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
	assert(control->sigusr1_event == NULL);
	assert(control->sigusr2_event == NULL);

	
}

static void init_settings(control_t *control)
{
	assert(control);

	assert(control->settings == NULL);

	control->settings = (settings_t *) malloc(sizeof(settings_t));
	assert(control->settings);

	settings_init(control->settings);
}

static void cleanup_settings(control_t *control)
{
	assert(control);

	assert(control->settings);
	settings_free(control->settings);
	free(control->settings);
	control->settings = NULL;
}

//-----------------------------------------------------------------------------
// Check the settings that we have received and generate an error if we dont
// have enough.
static void check_settings(control_t *control)
{
	assert(control);
	assert(control->settings);

	assert(control->settings->controllers);
	if (ll_count(control->settings->controllers) == 0) {
		fprintf(stderr, "Need at least one controller specified.\n");
		exit(EXIT_FAILURE);
	}
	
	assert(control->settings->interfaces);
	if (ll_count(control->settings->interfaces) == 0) {
		fprintf(stderr, "Need at least one interface specified.\n");
		exit(EXIT_FAILURE);
	}
	
	if (control->settings->configfile == NULL) {
		fprintf(stderr, "Need to specify a path for the config db file.\n");
		exit(EXIT_FAILURE);
	}
}

//-----------------------------------------------------------------------------
// If we need to run as a daemon, then do so, dropping privs to a specific
// username if it was specified, and creating a pid file, if that was
// specified.
static void init_daemon(control_t *control)
{
	assert(control);
	assert(control->settings);

	if (control->settings->daemonize) {
		if (rq_daemon(control->settings->username, control->settings->pid_file, control->settings->verbose) != 0) {
			fprintf(stderr, "failed to daemon() in order to daemonize\n");
			exit(EXIT_FAILURE);
		}
	}
}
	
// remove the PID file if we're a daemon
static void cleanup_daemon(control_t *control)
{
	assert(control);
	assert(control->settings);
	
	if (control->settings->daemonize && control->settings->pid_file) {
		assert(control->settings->pid_file[0] != 0);
		unlink(control->settings->pid_file);
	}
}

static void init_events(control_t *control)
{
	assert(control->evbase == NULL);
	control->evbase = event_base_new();
	assert(control->evbase);
}

static void cleanup_events(control_t *control)
{
	assert(control);
	assert(control->evbase);

	event_base_free(control->evbase);
	control->evbase = NULL;
}


static void init_rq(control_t *control)
{
	assert(control);
	assert(control->rq == NULL);

	control->rq = (rq_t *) malloc(sizeof(rq_t));
	rq_init(control->rq);

	assert(control->evbase);
	assert(control->rq);
	rq_setevbase(control->rq, control->evbase);
}

static void cleanup_rq(control_t *control)
{
	assert(control);
	assert(control->rq);

	rq_cleanup(control->rq);
	free(control->rq);
	control->rq = NULL;
}


static void init_signals(control_t *control)
{
	assert(control);
	assert(control->evbase);
	
	assert(control->sigint_event == NULL);
	control->sigint_event = evsignal_new(control->evbase, SIGINT, sigint_handler, control);
	assert(control->sigint_event);
	event_add(control->sigint_event, NULL);

	assert(control->sighup_event == NULL);
	control->sighup_event = evsignal_new(control->evbase, SIGINT, sighup_handler, control);
	assert(control->sighup_event);
	event_add(control->sighup_event, NULL);

	assert(control->sigusr1_event == NULL);
	control->sigusr1_event = evsignal_new(control->evbase, SIGUSR1, sigusr1_handler, control);
	assert(control->sigusr1_event);
	event_add(control->sigusr1_event, NULL);

	assert(control->sigusr2_event == NULL);
	control->sigusr2_event = evsignal_new(control->evbase, SIGUSR2, sigusr2_handler, control);
	assert(control->sigusr2_event);
	event_add(control->sigusr2_event, NULL);
}

static void cleanup_signals(control_t *control)
{
	assert(control);
	assert(control->sigint_event == NULL);
	assert(control->sigusr1_event == NULL);
	assert(control->sigusr2_event == NULL);
}

//-----------------------------------------------------------------------------
// add the controller details to the rq library so taht it can manage
// connecting to the controller.
static void init_controllers(control_t *control)
{
	char *str;
	
	assert(control);
	assert(control->settings);
	assert(control->settings->controllers);
	assert(control->rq);

	while ((str = ll_pop_head(control->settings->controllers))) {
		rq_addcontroller(control->rq, str);
		free(str);
	}
}

static void cleanup_controllers(control_t *control)
{
	assert(control);
	assert(control->settings);
	assert(control->settings->controllers);
	assert(ll_count(control->settings->controllers) == 0);
}

//-----------------------------------------------------------------------------
static void init_config(control_t *control)
{
	assert(control);
	assert(control->settings);
	assert(control->settings->configfile);
	assert(control->config == NULL);

	control->config = (config_t *) malloc(sizeof(config_t));
	assert(control->config);
	config_init(control->config);
	if (config_load(control->config, control->settings->configfile) < 0) {
		fprintf(stderr, "Errors loading config file: %s\n", control->settings->configfile);
		exit(EXIT_FAILURE);
	}
}

//-----------------------------------------------------------------------------
static void cleanup_config(control_t *control)
{
	assert(control);
	assert(control->config);
	config_free(control->config);
	free(control->config);
	control->config = NULL;
}

static void init_servers(control_t *control)
{
	server_t *server;
	char *str;
	
	assert(control);
	assert(control->servers == NULL);
	
	control->servers = (list_t *) malloc(sizeof(list_t));
	ll_init(control->servers);

	// we have some specific interfaces, so we will bind to each of them only.
	assert(control->settings->interfaces);
	assert(ll_count(control->settings->interfaces) > 0);
	while ((str = ll_pop_tail(control->settings->interfaces))) {
		server = (server_t *) malloc(sizeof(server_t));
		server_init(server, control);
		ll_push_head(control->servers, server);

		server_listen(server, str);
		free(str);
	}
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


static void init_bufpool(control_t *control)
{
	assert(control);
	assert(control->bufpool == NULL);

	control->bufpool = (expbuf_pool_t *) malloc(sizeof(expbuf_pool_t));
	expbuf_pool_init(control->bufpool);
}

static void cleanup_bufpool(control_t *control)
{
	assert(control);
	assert(control->bufpool);

	expbuf_pool_free(control->bufpool);
	free(control->bufpool);
	control->bufpool = NULL;
}

//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Initialise the server object.
void server_init(server_t *server, control_t *control)
{
	assert(server);
	assert(control);

	server->control = control;

	assert(control->settings->maxconns > 0);
	assert(control->conncount == 0);

	server->clients = (list_t *) malloc(sizeof(list_t));
	ll_init(server->clients);
}


//-----------------------------------------------------------------------------
// Cleanup the server object.
void server_free(server_t *server)
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
// Listen for socket connections on a particular interface.
void server_listen(server_t *server, char *interface)
{
	struct sockaddr saddr;
	int len;
	
	assert(server);
	assert(interface);

	memset(&saddr, 0, sizeof(saddr));
	sin.sin_family = AF_INET;

	if (evutil_parse_sockaddr_port(interface, &saddr, &len) != 0) {
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
	
	assert(0);
}

void settings_init(settings_t *ptr)
{
	assert(ptr != NULL);

	ptr->verbose = false;
	ptr->daemonize = false;
	ptr->username = NULL;
	ptr->pid_file = NULL;

	ptr->configfile = NULL;
	ptr->logqueue = NULL;
	ptr->statsqueue = NULL;
	ptr->gzipqueue = NULL;
	ptr->blacklist = NULL;

	ptr->controllers = (list_t *) malloc(sizeof(list_t));
	ll_init(ptr->controllers);

	ptr->interfaces = (list_t *) malloc(sizeof(list_t));
	ll_init(ptr->interfaces);
	
}

void settings_free(settings_t *ptr)
{
	assert(ptr != NULL);

	if (ptr->configfile) { free(ptr->configfile); ptr->configfile = NULL; }
	if (ptr->logqueue)   { free(ptr->logqueue);   ptr->logqueue = NULL; }
	if (ptr->statsqueue) { free(ptr->statsqueue); ptr->statsqueue = NULL; }
	if (ptr->gzipqueue)  { free(ptr->gzipqueue);  ptr->gzipqueue = NULL; }
	if (ptr->blacklist)  { free(ptr->blacklist);  ptr->blacklist = NULL; }

	assert(ptr->interfaces);
	ll_free(ptr->interfaces);
	free(ptr->interfaces);
	ptr->interfaces = NULL;

	assert(ptr->controllers);
	ll_free(ptr->controllers);
	free(ptr->controllers);
	ptr->controllers = NULL;
}


//-----------------------------------------------------------------------------
void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// need to initiate an RQ shutdown.
	assert(control->rq);
	rq_shutdown(control->rq);

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
void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// clear out all cached objects.
	assert(0);

	// reload the config database file.
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
	assert(client->control->evbase);
	client->read_event = event_new(client->control->evbase, handle, EV_READ|EV_PERSIST, read_handler, client);
	assert(client->read_event);
	event_add(client->read_event, NULL);

	client->blacklist_state = 1;
	if (server->control->blacklist) {
		blacklist_id = rq_blacklist_check(server->control->blacklist, address, socklen, blacklist_handler, client);
	}
	else {
		blacklist_id = 0;
	}
}

//-----------------------------------------------------------------------------
// Free the resources used by the client object.
void client_free(client_t *client)
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
		assert(client->server->control->bufpool);
		expbuf_clear(client->pending);
		expbuf_pool_return(client->server->control->bufpool, client->pending);
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
	client_t *client = (rq_client_t *) arg;
	expbuf_t *in;
	int res;

	assert(fd >= 0);
	assert(flags != 0);
	assert(client);
	assert(client->rq);
	assert(client->handle == fd);

	assert(client->control);
	assert(client->control->readbuf);
	in = client->control->readbuf;

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
		assert(BUF_LENGTH(client->control->readbuf) == 0);
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
	



	
	assert(BUF_LENGTH(client->control->readbuf) == 0);
}





//-----------------------------------------------------------------------------







void process_args(settings_t *settings, int argc, char **argv)
{
	int c;
	
	while (-1 != (c = getopt(argc, argv,
			"h"   /* help */
			"v"   /* verbose */
			"d:"  /* start as daemmon */
			"u:"  /* username to run as */
			"P:"  /* pidfile to store pid to */
			"c:"	/* controller to connect to */
			"f:"  /* sqlite3 configuration filename */
			"l:"  /* interface to listen on for http requests */
		))) {
		switch (c) {

			case 'l':
				assert(settings->interfaces);
				ll_push_tail(settings->interfaces, strdup(optarg));
				break;

			case 'f':
				assert(settings->configfile == NULL);
				settings->configfile = strdup(optarg);
				assert(settings->configfile);
				break;

			case 'c':
				assert(settings->controllers);
				ll_push_tail(settings->controllers, strdup(optarg));
				break;

			case 'h':
				usage();
				exit(EXIT_SUCCESS);
				break;
			case 'v':
				settings->verbose++;
				break;
			case 'd':
				assert(settings->daemonize == false);
				settings->daemonize = true;
				break;
			case 'u':
				assert(settings->username == NULL);
				settings->username = strdup(optarg);
				assert(settings->username != NULL);
				break;
			case 'P':
				assert(settings->pid_file == NULL);
				settings->pid_file = strdup(optarg);
				assert(settings->pid_file != NULL);
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				exit(EXIT_FAILURE);
				break;
		}
	}
}




//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	control_t      *control  = NULL;

///============================================================================
/// Initialization.
///============================================================================

	control = (control_t *) malloc(sizeof(control_t));

	init_control(control);
	init_settings(control);

	process_args(control->settings, argc, argv);

	check_settings(control);
	init_daemon(control);
	init_events(control);
	
	init_rq(control);
	init_signals(control);
	init_controllers(control);
	init_config(control);
	init_servers(control);
	init_bufpool(control);

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

	cleanup_events(control);

	cleanup_bufpool(control);
	cleanup_servers(control);
	cleanup_config(control);
	cleanup_controllers(control);
	cleanup_signals(control);
	cleanup_rq(control);

	cleanup_daemon(control);
	cleanup_settings(control);
	cleanup_control(control);

	free(control);

	return 0;
}


