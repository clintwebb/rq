//-----------------------------------------------------------------------------
// RISP Server
// Example code that demonstrate how to develop a server that communicates thru 
// a RISP protocol.
//-----------------------------------------------------------------------------



#include "commands.h"
#include "controllers.h"
#include "daemon.h"
#include "queue.h"
#include "server.h"
#include "settings.h"
#include "signals.h"
#include "stats.h"
#include "system_data.h"

// includes
#include <assert.h>
#include <event.h>
#include <expbufpool.h>
#include <rq.h>
#include <mempool.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>



#define PACKAGE						"rqd"
#define VERSION						"1.0"




//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
static void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-p <num>      TCP port to listen on (default: %d)\n", RQ_DEFAULT_PORT);
	printf("-i <ip_addr>  interface to listen on, default is INADDR_ANY\n");
	printf("-C <num>      max simultaneous connections, default is 1024\n");
	printf("-S <ip:port>  Controller to connect to. (can be used more than once)\n");
	printf("-l <file>     Local log file\n");
	printf("\n");
	printf("-D            run as a daemon\n");
	printf("-P <file>     save PID in <file>, only used with -d option\n");
	printf("-U <username> assume identity of <username> (only when run as root)\n");
	printf("\n");
	printf("-v            verbose (print errors/warnings while in event loop)\n");
	printf("-h            print this help and exit\n");
	return;
}

static void get_options(settings_t *settings, int argc, char **argv)
{
	int c;
	
	// process arguments 
	while ((c = getopt(argc, argv,
		"h"   /* help */
		"v"   /* verbose */
		
		"C:"  /* max number of connections */
		"D:"  /* run as daemon */
		"U:"  /* user to run as */
		"P:"  /* pid file */
		"S:"	/* Server to connect to, can be supplied more than once. */
		
		"i:"  /* interfaces to bind to */
		"p:"  /* port to listen on. */
		"l:"  /* logfile. */
	)) != -1) {
		switch (c) {

			/// common daemon options.
		
			case 'h':
				usage();
				exit(EXIT_SUCCESS);
				break;
			case 'v':
				settings->verbose++;
				break;
			case 'C':
				settings->maxconns = atoi(optarg);
				assert(settings->maxconns > 0);
				break;
			case 'S':
				ll_push_tail(settings->controllers, strdup(optarg));
				break;
			case 'D':
				assert(settings->daemonize == false);
				settings->daemonize = true;
				break;
			case 'U':
				settings->username = optarg;
				break;
			case 'P':
				settings->pid_file = optarg;
				break;

			/// instance specific daemon options.

			case 'p':
				settings->port = atoi(optarg);
				break;
			
			case 'l':
				settings->logfile = optarg;
				break;

			case 'i':
				ll_push_tail(settings->interfaces, strdup(optarg));
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				exit(EXIT_FAILURE);
		}
	}
}

// initialise our system data object.
static void init_sysdata(system_data_t *sysdata)
{
	assert(sysdata);
	
	sysdata->evbase        = NULL;
	sysdata->bufpool       = NULL;
	sysdata->settings      = NULL;
	sysdata->servers       = NULL;
	sysdata->stats         = NULL;
	sysdata->risp          = NULL;
	sysdata->queues        = NULL;
	sysdata->msgpool       = NULL;
	sysdata->sighup_event  = NULL;
	sysdata->sigint_event  = NULL;
	sysdata->sigusr1_event = NULL;
	sysdata->sigusr2_event = NULL;
	sysdata->nodelist      = NULL;
	sysdata->controllers   = NULL;
	sysdata->logging       = NULL;
	sysdata->in_buf        = NULL;
	sysdata->build_buf     = NULL;
}

static void cleanup_sysdata(system_data_t *sysdata)
{
	assert(sysdata);
	
	assert(sysdata->evbase == NULL);
	assert(sysdata->bufpool == NULL);
	assert(sysdata->settings == NULL);
	assert(sysdata->servers == NULL);
	assert(sysdata->stats == NULL);
	assert(sysdata->risp == NULL);
	assert(sysdata->queues == NULL);
	assert(sysdata->msgpool == NULL);
	assert(sysdata->sighup_event == NULL);
	assert(sysdata->sigint_event == NULL);
	assert(sysdata->sigusr1_event == NULL);
	assert(sysdata->sigusr2_event == NULL);
	assert(sysdata->nodelist == NULL);
	assert(sysdata->controllers == NULL);
	assert(sysdata->logging == NULL);
	assert(sysdata->in_buf == NULL);
	assert(sysdata->build_buf == NULL);
}

// init settings
static void init_settings(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->settings == NULL);
	
	sysdata->settings = (settings_t *) malloc(sizeof(settings_t));
	assert(sysdata->settings);
	settings_init(sysdata->settings);
}

// cleanup the settings object.
static void cleanup_settings(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->settings);

	settings_cleanup(sysdata->settings);
	free(sysdata->settings);
	sysdata->settings = NULL;
}

// If needed, increase rlimits to allow as many connections as needed.
static void init_maxconns(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->settings);
	assert(sysdata->settings->maxconns > 0);
 	rq_set_maxconns(sysdata->settings->maxconns);
}

static void cleanup_maxconns(system_data_t *sysdata)
{
	assert(sysdata);

	// there is nothing really we need to do to cleanup maxconns.
}

// if we are supplied with a username, drop privs to it.  This will only
// work if we are running as root, and is really only needed when running as
// a daemon.
static void init_daemon(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->settings);
	if (sysdata->settings->daemonize != false) {
		rq_daemon(sysdata->settings->username, sysdata->settings->pid_file, sysdata->settings->verbose);
	}
}

// remove the PID file if we're a daemon
static void cleanup_daemon(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->settings);
	
	if (sysdata->settings->daemonize && sysdata->settings->pid_file != NULL) {
		unlink(sysdata->settings->pid_file);
	}
}

// initialize main thread libevent instance
static void init_events(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->evbase == NULL);
	
	sysdata->evbase = event_base_new();
	assert(sysdata->evbase);
}

// clear the event base pointer, because no more events can be set.
static void cleanup_events(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->evbase);

	event_base_free(sysdata->evbase);
	sysdata->evbase = NULL;
}

// setup the logging system.
static void init_logging(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->logging == NULL);
	assert(sysdata->evbase);
	
	sysdata->logging = (logging_t *) malloc(sizeof(logging_t));
	log_init(sysdata->logging, sysdata->settings->logfile, sysdata->settings->verbose);
	log_buffered(sysdata->logging, sysdata->evbase);
}

static void cleanup_logging(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->logging);

	// normally before freeing the log, you would set it in direct mode to
	// ensure that it flushes any leftover entries out... but that would
	// already have been done.
	
	log_free(sysdata->logging);
	free(sysdata->logging);
	sysdata->logging = NULL;
}


// handle SIGINT
static void init_signals(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->evbase);

	assert(sysdata->sighup_event == NULL);
	sysdata->sighup_event = evsignal_new(sysdata->evbase, SIGHUP, sighup_handler, sysdata);
	assert(sysdata->sighup_event);
	event_add(sysdata->sighup_event, NULL);

	assert(sysdata->sigint_event == NULL);
	sysdata->sigint_event = evsignal_new(sysdata->evbase, SIGINT, sigint_handler, sysdata);
	assert(sysdata->sigint_event);
	event_add(sysdata->sigint_event, NULL);

	assert(sysdata->sigusr1_event == NULL);
	sysdata->sigusr1_event = evsignal_new(sysdata->evbase, SIGUSR1, sigusr1_handler, sysdata);
	assert(sysdata->sigusr1_event);
	event_add(sysdata->sigusr1_event, NULL);

	assert(sysdata->sigusr2_event == NULL);
	sysdata->sigusr2_event = evsignal_new(sysdata->evbase, SIGUSR2, sigusr2_handler, sysdata);
	assert(sysdata->sigusr2_event);
	event_add(sysdata->sigusr2_event, NULL);
}

static void cleanup_signals(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->evbase == NULL);
	assert(sysdata->sighup_event == NULL);
	assert(sysdata->sigint_event == NULL);
}

// Creating our buffer pool, and common buffers.
static void init_buffers(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->bufpool == NULL);
	
	sysdata->bufpool = (expbuf_pool_t *) malloc(sizeof(expbuf_pool_t));
	expbuf_pool_init(sysdata->bufpool, 0);

	assert(sysdata->in_buf == NULL);
	sysdata->in_buf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(sysdata->in_buf, DEFAULT_BUFFSIZE);

	assert(sysdata->build_buf == NULL);
	sysdata->build_buf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(sysdata->build_buf, 0);
}

// cleanup the expanding buffer pool.
// free our common buffers.
static void cleanup_buffers(system_data_t *sysdata)
{
	assert(sysdata);

	assert(sysdata->bufpool);
	expbuf_pool_free(sysdata->bufpool);
	free(sysdata->bufpool);
	sysdata->bufpool = NULL;

	assert(sysdata->in_buf && sysdata->build_buf);
	expbuf_free(sysdata->in_buf);
	expbuf_free(sysdata->build_buf);
	free(sysdata->in_buf);
	free(sysdata->build_buf);
	sysdata->in_buf = NULL;
	sysdata->build_buf = NULL;
}


// create and init the 'server' structure.
static void init_servers(system_data_t *sysdata)
{
	server_t *server;
	char *str;
	
	assert(sysdata);
	assert(sysdata->servers == NULL);
	
	sysdata->servers = (list_t *) malloc(sizeof(list_t));
	ll_init(sysdata->servers);

	assert(sysdata->settings->port > 0);
	assert(sysdata->settings->interfaces);
	if (ll_count(sysdata->settings->interfaces) == 0) {

		// no interfaces were specified, so we need to bind to all of them.
		server = (server_t *) malloc(sizeof(server_t));
		server_init(server, sysdata);
		ll_push_head(sysdata->servers, server);
		server_listen(server, sysdata->settings->port, NULL);
	}
	else {

		// we have some specific interfaces, so we will bind to each of them only.
		while ((str = ll_pop_tail(sysdata->settings->interfaces))) {
			server = (server_t *) malloc(sizeof(server_t));
			server_init(server, sysdata);
			ll_push_head(sysdata->servers, server);
			
			server_listen(server, sysdata->settings->port, str);
			free(str);
		}
	}
}

// cleanup 'server'
static void cleanup_servers(system_data_t *sysdata)
{
	server_t *server;
	
	assert(sysdata);
	assert(sysdata->servers);

	while ((server = ll_pop_head(sysdata->servers))) {
		server_free(server);
		free(server);
	}

	ll_free(sysdata->servers);
	free(sysdata->servers);
	sysdata->servers = NULL;
}

static void init_stats(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->stats == NULL);
	
	sysdata->stats = (stats_t *) malloc(sizeof(stats_t));
	stats_init(sysdata->stats);
	sysdata->stats->sysdata = sysdata;
	stats_start(sysdata->stats);
}

// cleanup stats objects.
static void cleanup_stats(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->stats);
	assert(sysdata->stats->stats_event == NULL);
	
	free(sysdata->stats);
	sysdata->stats = NULL;
}

// Initialise the risp system.
static void init_risp(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->risp == NULL);
	
	sysdata->risp = risp_init();
	assert(sysdata->risp);
	command_init(sysdata->risp);
}

// cleanup risp library.
static void cleanup_risp(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->risp);

	risp_shutdown(sysdata->risp);
	sysdata->risp = NULL;
}

// create the nodelist list.
static void init_nodes(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->nodelist == NULL);
	
	sysdata->nodelist = (list_t *) malloc(sizeof(list_t));
	ll_init(sysdata->nodelist);
}

// nodelist should already be empty, otherwise how did we break out of the loop?
static void cleanup_nodes(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->nodelist);

	assert(ll_count(sysdata->nodelist) == 0);
	ll_free(sysdata->nodelist);
	free(sysdata->nodelist);
	sysdata->nodelist = NULL;
}

// Create the message pool.
static void init_msgpool(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->msgpool == NULL);
	
	sysdata->msgpool = (mempool_t *) malloc(sizeof(mempool_t));
	mempool_init(sysdata->msgpool);
	
}

// Cleanup the message pool.
static void cleanup_msgpool(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->msgpool);
	
	mempool_free(sysdata->msgpool);
	free(sysdata->msgpool);
	sysdata->msgpool = NULL;
}

// initialise the empty linked-list of queues.
static void init_queues(system_data_t *sysdata)
{
	assert(sysdata);
	assert(sysdata->queues == NULL);
	
	sysdata->queues = (list_t *) malloc(sizeof(list_t));
	ll_init(sysdata->queues);
	assert(sysdata->queues);
}

// The queue list would not be empty, but the queues themselves should already be cleared as part of the server shutdown event.
static void cleanup_queues(system_data_t *sysdata)
{
 	queue_t *q;
 	
	assert(sysdata);
	assert(sysdata->queues);

	while ((q = ll_pop_head(sysdata->queues))) {
		queue_free(q);
		free(q);
	}
	ll_free(sysdata->queues);
	free(sysdata->queues);
	sysdata->queues = NULL;
}

static void init_controllers(system_data_t *sysdata)
{
	char         *str;
	controller_t *ct;
	
	assert(sysdata);
	assert(sysdata->controllers == NULL);
	
	sysdata->controllers = (list_t *) malloc(sizeof(list_t));
	ll_init(sysdata->controllers);
	
	// now that everything else is configured, connect to other controllers.
	assert(sysdata->controllers);
	while ((str = ll_pop_head(sysdata->settings->controllers))) {
		ct = (controller_t *) malloc(sizeof(controller_t));
		controller_init(ct, str);
		ct->sysdata = sysdata;

		logger(sysdata->logging, 1, "Connecting to controller: %s.", str);
		controller_connect(ct);
		ll_push_tail(sysdata->controllers, ct);
		ct = NULL;
	}
}

// cleanup the list of controllers.
static void cleanup_controllers(system_data_t *sysdata)
{
	controller_t *ct;

	assert(sysdata);
	assert(sysdata->controllers);

	while ((ct = ll_pop_head(sysdata->controllers))) {
		controller_free(ct);
		free(ct);
	}
	ll_free(sysdata->controllers);
	free(sysdata->controllers);
	sysdata->controllers = NULL;
}




//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	system_data_t   sysdata;

///============================================================================
/// Initialization.
///============================================================================

	init_sysdata(&sysdata);
	init_settings(&sysdata);

	get_options(sysdata.settings, argc, argv);
	
	init_maxconns(&sysdata);
	init_daemon(&sysdata);
	init_events(&sysdata);
	init_logging(&sysdata);

	logger(sysdata.logging, 1, "System starting up");

	init_signals(&sysdata);
	init_buffers(&sysdata);
	init_servers(&sysdata);
	init_stats(&sysdata);
	init_risp(&sysdata);
	init_nodes(&sysdata);
	init_msgpool(&sysdata);
	init_queues(&sysdata);
	init_controllers(&sysdata);


///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the event loop.
	logger(sysdata.logging, 1, "Starting Event Loop");
	assert(sysdata.evbase);
	event_base_loop(sysdata.evbase, 0);
	logger(sysdata.logging, 1, "Shutdown preparations complete.  Shutting down now.");


///============================================================================
/// Shutdown
///============================================================================

	cleanup_events(&sysdata);
	cleanup_controllers(&sysdata);
	cleanup_queues(&sysdata);
	cleanup_msgpool(&sysdata);
	cleanup_nodes(&sysdata);
	cleanup_risp(&sysdata);
	cleanup_stats(&sysdata);
	cleanup_servers(&sysdata);
	cleanup_buffers(&sysdata);
	cleanup_signals(&sysdata);
	
	logger(sysdata.logging, 1, "Shutdown complete.\n");

	cleanup_logging(&sysdata);
	cleanup_daemon(&sysdata);
	cleanup_maxconns(&sysdata);
	cleanup_settings(&sysdata);
	cleanup_sysdata(&sysdata);
	
	// good-bye.
	return 0;
}


