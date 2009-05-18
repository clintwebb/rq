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
void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-p <num>      TCP port to listen on (default: %d)\n", RQ_DEFAULT_PORT);
	printf("-l <ip_addr>  interface to listen on, default is INDRR_ANY\n");
	printf("-c <num>      max simultaneous connections, default is 1024\n");
	printf("\n");
	printf("-a <ip_addr>  Controller to connect to.\n");
	printf("-A <num>      Port to use when connecting to the other controller.\n");
	printf("-b <ip_addr>  Backup Controller to connect to.\n");
	printf("-B <num>      Port to use when connecting to the backup controller.\n");
	printf("\n");
	printf("-d            run as a daemon\n");
	printf("-P <file>     save PID in <file>, only used with -d option\n");
	printf("-u <username> assume identity of <username> (only when run as root)\n");
	printf("\n");
	printf("-v            verbose (print errors/warnings while in event loop)\n");
	printf("-h            print this help and exit\n");
	return;
}






void get_options(settings_t *settings, int argc, char **argv)
{
	int c;
	
	// process arguments 
	while ((c = getopt(argc, argv,
		"hv"	/* help, verbose */
		
		"C:"  /* max number of connections */
		"D:"  /* run as daemon */
		"U:"  /* user to run as */
		"P:"  /* pid file */
		"S:"	/* Server to connect to, can be supplied more than once. */
		
		"i:"  /* interfaces to bind to */
		"p:"  /* port to listen on. */
		"l:"  /*  logfile */
	)) != -1) {
		switch (c) {

			/// common daemon options.
		
			case 'h':
				usage();
				exit(EXIT_SUCCESS);
			case 'v':
				settings->verbose++;
				break;
			case 'C':
				settings->maxconns = atoi(optarg);
				assert(settings->maxconns > 0);
				break;
			case 'S':
				ll_push_tail(&settings->controllers, optarg);
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
				if (settings->interfaces >= MAX_INTERFACES) {
					fprintf(stderr, "Too many interfaces specified.  Only %d allowed.\n", MAX_INTERFACES);
					exit(EXIT_FAILURE);
				}
				settings->interface[settings->interfaces] = strdup(optarg);
				settings->interfaces++;
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				exit(EXIT_FAILURE);
		}
	}
}




//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	settings_t     *settings = NULL;
	server_t       *server   = NULL;
	stats_t        *stats    = NULL;
	queue_t        *q;
	char           *str;
	controller_t   *ct;

	system_data_t   sysdata;
	int i;

///============================================================================
/// Initialization.
///============================================================================

	// initialise our system data object.
	sysdata.evbase       = NULL;
	sysdata.bufpool      = NULL;
	sysdata.verbose      = 0;
	sysdata.settings     = NULL;
	sysdata.server       = NULL;
	sysdata.stats        = NULL;
	sysdata.risp         = NULL;
	sysdata.queues       = NULL;
	sysdata.msgpool      = NULL;
	sysdata.sighup_event = NULL;
	sysdata.sigint_event = NULL;
	sysdata.nodelist     = NULL;
	sysdata.controllers  = NULL;
	sysdata.stats_event  = NULL;


	// init settings
	settings = (settings_t *) malloc(sizeof(settings_t));
	assert(settings != NULL);
	settings_init(settings);
	sysdata.settings = settings;

	// set stderr non-buffering (for running under, say, daemontools)
	get_options(settings, argc, argv);
	sysdata.verbose = settings->verbose;

	if (settings->verbose) printf("Finished processing command-line args\n");

	// If needed, increase rlimits to allow as many connections as needed.
	if (settings->verbose) printf("Settings Max connections: %d\n", settings->maxconns);
	assert(settings->maxconns > 0);
 	rq_set_maxconns(settings->maxconns);

	// if we are supplied with a username, drop privs to it.  This will only 
	// work if we are running as root, and is really only needed when running as 
	// a daemon.
	if (settings->daemonize != false) {
		if (settings->verbose) printf("Dropping privs and changing username: '%s'\n", settings->username);
		if (rq_daemon(settings->username, settings->pid_file, settings->verbose) != 0) {
			usage();
			exit(EXIT_FAILURE);
		}
	}

	// initialize main thread libevent instance
	if (settings->verbose) printf("Initialising the event system.\n");
	sysdata.evbase = event_init();


	// handle SIGINT
	assert(sysdata.evbase);
	sysdata.sighup_event = evsignal_new(sysdata.evbase, SIGHUP, sighup_handler, &sysdata);
	sysdata.sigint_event = evsignal_new(sysdata.evbase, SIGINT, sigint_handler, &sysdata);
	event_add(sysdata.sighup_event, NULL);
	event_add(sysdata.sigint_event, NULL);


	// Creating our buffer pool.
	if (settings->verbose) printf("Creating the Buffer pool.\n");
	sysdata.bufpool = (expbuf_pool_t *) malloc(sizeof(expbuf_pool_t));
	expbuf_pool_init(sysdata.bufpool, 0);

	// create our common buffers.
	sysdata.in_buf = (expbuf_t *) malloc(sizeof(expbuf_t));
	sysdata.build_buf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(sysdata.in_buf, DEFAULT_BUFFSIZE);
	expbuf_init(sysdata.build_buf, 0);

	// create and init the 'server' structure.
	if (settings->verbose) printf("Starting server listener on port %d.\n", settings->port);
	server = (server_t *) malloc(sizeof(server_t));
	assert(server != NULL);
	server_init(server, &sysdata);
	assert(server->sysdata == &sysdata);
	sysdata.server = server;
	assert(settings->maxconns > 0);
	server->maxconns = settings->maxconns;
	
	assert(settings->port > 0);
	if (settings->interfaces == 0) {
		assert(settings->interface[0] == NULL);
		server_listen(server, settings->port, NULL);
	}
	else {
		for (i=0; i<settings->interfaces; i++) {
			assert(settings->interface[i] != NULL);
			server_listen(server, settings->port, settings->interface[i]);
		}
	}

	assert(sysdata.server);

	// initialise clock event.  The clock event is used to keep up our node 
	// network.  If we dont have enough connections, we will need to make some 
	// requests.  
	// create the timeout structure, and the timeout event.   This is used to 
	// perform certain things spread over time.   Such as indexing the 
	// 'complete' paths that we have, and ensuring that the 'chunks' parts are 
	// valid.
	if (settings->verbose) printf("Setting up Stats action.\n");

	stats = (stats_t *) malloc(sizeof(stats_t));
	stats_init(stats);
	if (settings->logfile != NULL) {
		/// TODO: This will do for now, but we really need a system that will log
		///       files until they are a certain size, and then start a new one...
		///       Or maybe a new log every day... something better than this.
		stats->logfile = fopen(settings->logfile, "a");
		assert(stats->logfile != NULL);
	}
	sysdata.stats = stats;
	stats->sysdata = &sysdata;
	stats_start(stats);



	// Initialise the risp system.
	sysdata.risp = risp_init();
	assert(sysdata.risp != NULL);
	risp_add_invalid(sysdata.risp, &cmdInvalid);
	risp_add_command(sysdata.risp, RQ_CMD_CLEAR, 	      &cmdClear);
	risp_add_command(sysdata.risp, RQ_CMD_EXECUTE,      &cmdExecute);
	risp_add_command(sysdata.risp, RQ_CMD_PING,         &cmdPing);
	risp_add_command(sysdata.risp, RQ_CMD_PONG,         &cmdPong);
	risp_add_command(sysdata.risp, RQ_CMD_REQUEST,      &cmdRequest);
	risp_add_command(sysdata.risp, RQ_CMD_REPLY,        &cmdReply);
	risp_add_command(sysdata.risp, RQ_CMD_RECEIVED,     &cmdReceived);
	risp_add_command(sysdata.risp, RQ_CMD_DELIVERED,    &cmdDelivered);
	risp_add_command(sysdata.risp, RQ_CMD_BROADCAST,    &cmdBroadcast);
	risp_add_command(sysdata.risp, RQ_CMD_NOREPLY,      &cmdNoReply);
	risp_add_command(sysdata.risp, RQ_CMD_CONSUME,      &cmdConsume);
	risp_add_command(sysdata.risp, RQ_CMD_CANCEL_QUEUE, &cmdCancelQueue);
	risp_add_command(sysdata.risp, RQ_CMD_CLOSING,      &cmdClosing);
	risp_add_command(sysdata.risp, RQ_CMD_EXCLUSIVE,    &cmdExclusive);
	risp_add_command(sysdata.risp, RQ_CMD_QUEUEID,      &cmdQueueID);
	risp_add_command(sysdata.risp, RQ_CMD_ID,           &cmdId);
	risp_add_command(sysdata.risp, RQ_CMD_TIMEOUT,      &cmdTimeout);
	risp_add_command(sysdata.risp, RQ_CMD_MAX,          &cmdMax);
	risp_add_command(sysdata.risp, RQ_CMD_PRIORITY,     &cmdPriority);
	risp_add_command(sysdata.risp, RQ_CMD_QUEUE,        &cmdQueue);
	risp_add_command(sysdata.risp, RQ_CMD_PAYLOAD,      &cmdPayload);

	// create the nodelist list.
	sysdata.nodelist = (list_t *) malloc(sizeof(list_t));
	ll_init(sysdata.nodelist);
	
	sysdata.controllers = (list_t *) malloc(sizeof(list_t));
	ll_init(sysdata.controllers);


	// Create the message pool.
	sysdata.msgpool = (mempool_t *) malloc(sizeof(mempool_t));
	mempool_init(sysdata.msgpool);
	assert(sysdata.msgpool);

	// initialise the empty linked-list of queues.
	sysdata.queues = (list_t *) malloc(sizeof(list_t));
	ll_init(sysdata.queues);
	assert(sysdata.queues);
	
	// now that everything else is configured, connect to other controllers.
	assert(sysdata.controllers);
	while ((str = ll_pop_head(&settings->controllers))) {
		ct = (controller_t *) malloc(sizeof(controller_t));
		controller_init(ct, str);
		ct->sysdata = &sysdata;

		if (settings->verbose>1) printf("Connecting to controller: %s.\n", str);
		controller_connect(ct);
		ll_push_tail(sysdata.controllers, ct);
		ct = NULL;
	}

///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the event loop.
	if (settings->verbose) printf("Starting Event Loop\n\n");
	assert(sysdata.evbase);
	event_base_loop(sysdata.evbase, 0);
	if (settings->verbose) printf("Shutdown preparations complete.  Shutting down now.\n");


///============================================================================
/// Shutdown
///============================================================================

	// clear the event base pointer, because no more events can be set.
	sysdata.evbase = NULL;

	// Cleanup the message pool.
	assert(sysdata.msgpool);
	mempool_free(sysdata.msgpool);
	free(sysdata.msgpool);
	sysdata.msgpool = NULL;

	// cleanup 'server'
	server_cleanup(server);
	free(server);
	server = NULL;
	sysdata.server = NULL;

	// The queue list would not be empty, but the queues themselves should already be cleared as part of the server shutdown event.
	assert(sysdata.queues);
	while ((q = ll_pop_head(sysdata.queues))) {
		queue_free(q);
		free(q);
	}
	ll_free(sysdata.queues);
	free(sysdata.queues);
	sysdata.queues = NULL;

	// nodelist should already be empty, otherwise how did we break out of the loop?
	assert(sysdata.nodelist);
	assert(ll_count(sysdata.nodelist) == 0);
	printf("nodelist\n");
	ll_free(sysdata.nodelist);
	free(sysdata.nodelist);
	sysdata.nodelist = NULL;
	
	// cleanup risp library.
	risp_shutdown(sysdata.risp);
	sysdata.risp = NULL;
    
	// free our common buffers.
	assert(sysdata.in_buf && sysdata.build_buf);
	expbuf_free(sysdata.in_buf);
	expbuf_free(sysdata.build_buf);
	free(sysdata.in_buf);
	free(sysdata.build_buf);
	sysdata.in_buf = NULL;
	sysdata.build_buf = NULL;

	if (sysdata.verbose) printf("\n\nExiting.\n");
    
	// remove the PID file if we're a daemon
	if (settings->daemonize && settings->pid_file != NULL) {
		if (settings->verbose) printf("Removing pid file: %s\n", settings->pid_file);
		unlink(settings->pid_file);
	}

	// cleanup stats objects.
	assert(stats != NULL);
	if (stats->logfile != NULL) {
		fclose(stats->logfile);
	}
	free(stats);
	stats = NULL;
	sysdata.stats = NULL;

	// cleanup the expanding buffer pool.
	assert(sysdata.bufpool != NULL);
	expbuf_pool_free(sysdata.bufpool);
	free(sysdata.bufpool);
	sysdata.bufpool = NULL;


	// cleanup the list of controllers.
	assert(sysdata.controllers);
	while ((ct = ll_pop_head(sysdata.controllers))) {
		controller_free(ct);
		free(ct);
	}
	printf("controllers\n");
	ll_free(sysdata.controllers);
	free(sysdata.controllers);
	sysdata.controllers = NULL;
	



	// cleanup the settings object.
	assert(settings != NULL);
	settings_cleanup(settings);
	free(settings);
	settings = NULL;
	sysdata.settings = NULL;
	
	if (sysdata.verbose) printf("\nExited.\n");

	// good-bye.
	return 0;
}


