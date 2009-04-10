//-----------------------------------------------------------------------------
// RISP Server
// Example code that demonstrate how to develop a server that communicates thru 
// a RISP protocol.
//-----------------------------------------------------------------------------



#include "actions.h"
#include "commands.h"
#include "daemon.h"
#include "server.h"
#include "settings.h"
#include "stats.h"
#include "system_data.h"

// includes
#include <assert.h>
#include <evactions.h>
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
// Global variables.
struct event_base *main_event_base = NULL;



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



//-----------------------------------------------------------------------------
// Handle the signal.  Any signal we receive can only mean that we need to
// exit.  We could initiate the shutdown action from here, but we really want
// to make sure this function exits as quickly as possible, because we dont
// want another signal to arrive while we are still processing one.  Therefore,
// we merely break out of the loop and we use that to initiate things.
static void sig_handler(const int sig) {
	printf("SIGINT handled.\n");
	assert(main_event_base != NULL);
	event_base_loopbreak(main_event_base);
}



void get_options(settings_t *settings, int argc, char **argv)
{
	int c;
	
	// process arguments 
	while ((c = getopt(argc, argv, "p:k:c:hvd:u:P:l:s:a:A:b:B:L:")) != -1) {
		switch (c) {
			case 'p':
				settings->port = atoi(optarg);
				assert(settings->port > 0);
				break;
			case 'c':
				settings->maxconns = atoi(optarg);
				assert(settings->maxconns > 0);
				break;
			
			case 'a':
				assert(settings->primary == NULL);
				settings->primary = optarg;
				assert(settings->primary != NULL);
				assert(settings->primary[0] != '\0');
			case 'A':
				settings->priport = atoi(optarg);
				assert(settings->priport > 0);
				break;

			case 'b':
				assert(settings->secondary == NULL);
				settings->secondary = optarg;
				assert(settings->secondary != NULL);
				assert(settings->secondary[0] != '\0');
			case 'B':
				settings->secport = atoi(optarg);
				assert(settings->secport > 0);
				break;

			case 'L':
				assert(settings->logfile == NULL);
				settings->logfile = optarg;
				break;

			case 'h':
				usage();
				exit(EXIT_SUCCESS);
			case 'v':
				settings->verbose++;
				break;
			case 'd':
				assert(settings->daemonize == false);
				settings->daemonize = true;
				break;
			case 'u':
				assert(settings->username == NULL);
				settings->username = optarg;
				assert(settings->username != NULL);
				assert(settings->username[0] != '\0');
				break;
			case 'P':
				assert(settings->pid_file == NULL);
				settings->pid_file = optarg;
				assert(settings->pid_file != NULL);
				assert(settings->pid_file[0] != '\0');
				break;
			case 'l':
				assert(settings->interfaces >= 0);

				if (settings->interfaces >= MAX_INTERFACES) {
					fprintf(stderr, "Too many interfaces specified.  Only %d allowed.\n", MAX_INTERFACES);
					exit(EXIT_FAILURE);
				}
					
				assert(settings->interface[settings->interfaces] == NULL);

				settings->interface[settings->interfaces] = strdup(optarg);
				settings->interfaces++;

				assert(settings->interfaces > 0);
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
	action_t       *action   = NULL;

	system_data_t   sysdata;
	int i;

///============================================================================
/// Initialization.
///============================================================================

	// initialise our system data object.
	sysdata.evbase    = NULL;
	sysdata.bufpool   = NULL;
	sysdata.verbose   = 0;
	sysdata.settings  = NULL;
	sysdata.server    = NULL;
	sysdata.stats     = NULL;
	sysdata.actpool   = NULL;
	sysdata.risp      = NULL;
	sysdata.queues    = NULL;
	sysdata.msgpool   = NULL;


	// handle SIGINT 
	signal(SIGINT, sig_handler);
	
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
	main_event_base = event_init();
	sysdata.evbase = main_event_base;

	// Creating our buffer pool.
	if (settings->verbose) printf("Creating the Buffer pool.\n");
	sysdata.bufpool = (expbuf_pool_t *) malloc(sizeof(expbuf_pool_t));
	expbuf_pool_init(sysdata.bufpool, 0);

	// Creating our actions pool.
	assert(sysdata.evbase != NULL);
	if (settings->verbose) printf("Creating the Action pool.\n");
	sysdata.actpool = (action_pool_t *) malloc(sizeof(action_pool_t));
	action_pool_init(sysdata.actpool, sysdata.evbase, &sysdata);

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

	assert(sysdata.actpool != NULL);
	assert(sysdata.server != NULL);

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

	// setup an action to output the stats every second.
	assert(sysdata.actpool != NULL);
	action = action_pool_new(sysdata.actpool);
	action_set(action, 1000, ah_stats, stats);
	action = NULL;


	// Initialise the risp system.
	sysdata.risp = risp_init();
	assert(sysdata.risp != NULL);
	risp_add_invalid(sysdata.risp, &cmdInvalid);
	risp_add_command(sysdata.risp, RQ_CMD_CLEAR, 	      &cmdClear);
	risp_add_command(sysdata.risp, RQ_CMD_EXECUTE,      &cmdExecute);
	risp_add_command(sysdata.risp, RQ_CMD_REQUEST,      &cmdRequest);
	risp_add_command(sysdata.risp, RQ_CMD_REPLY,        &cmdReply);
	risp_add_command(sysdata.risp, RQ_CMD_RECEIVED,     &cmdReceived);
	risp_add_command(sysdata.risp, RQ_CMD_DELIVERED,    &cmdDelivered);
	risp_add_command(sysdata.risp, RQ_CMD_BROADCAST,    &cmdBroadcast);
	risp_add_command(sysdata.risp, RQ_CMD_NOREPLY,      &cmdNoReply);
	risp_add_command(sysdata.risp, RQ_CMD_CONSUME,      &cmdConsume);
	risp_add_command(sysdata.risp, RQ_CMD_CANCEL_QUEUE, &cmdCancelQueue);
	risp_add_command(sysdata.risp, RQ_CMD_EXCLUSIVE,    &cmdExclusive);
	risp_add_command(sysdata.risp, RQ_CMD_ID,           &cmdId);
	risp_add_command(sysdata.risp, RQ_CMD_TIMEOUT,      &cmdTimeout);
	risp_add_command(sysdata.risp, RQ_CMD_MAX,          &cmdMax);
	risp_add_command(sysdata.risp, RQ_CMD_PRIORITY,     &cmdPriority);
	risp_add_command(sysdata.risp, RQ_CMD_QUEUE,        &cmdQueue);
	risp_add_command(sysdata.risp, RQ_CMD_PAYLOAD,      &cmdPayload);


	// connect to other controller.
	if (settings->primary != NULL) {
		// when the connection is established, we need to send a QUEUELIST command
		// to the other controller so that it can give us a list of QUEUES that it
		// is consuming.

		assert(0);
	}

	// Create the message pool.
	sysdata.msgpool = (mempool_t *) malloc(sizeof(mempool_t));
	mempool_init(sysdata.msgpool);
	assert(sysdata.msgpool);


	// initialise the empty linked-list of queues.
	sysdata.queues = (list_t *) malloc(sizeof(list_t));
	ll_init(sysdata.queues);
	assert(sysdata.queues);
	
///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the event loop.
	if (settings->verbose) printf("Starting Event Loop\n\n");
	event_base_loop(main_event_base, 0);
    
	// The event loop was exited, this means that we received an interrupt
	// signal.  We need to create an appropriate action object.
	assert(sysdata.actpool != NULL);
	action = action_pool_new(sysdata.actpool);
	action_set(action, 0, ah_server_shutdown, NULL);
	action = NULL;
	if (settings->verbose) printf("Initiated Shutdown procedure.\n");

	// In order to complete the shutdown, we need to continue the event loop.
	event_base_loop(main_event_base, 0);
	if (settings->verbose) printf("Shutdown preparations complete.  Shutting down now.\n");


///============================================================================
/// Shutdown
///============================================================================

	sysdata.evbase = NULL;

	// Cleanup the message pool.
	assert(sysdata.msgpool);
	mempool_free(sysdata.msgpool);
	free(sysdata.msgpool);
	sysdata.msgpool = NULL;

	// cleanup 'server', which should cleanup all the 'nodes'
	server_cleanup(server);
	free(server);
	server = NULL;
	sysdata.server = NULL;

	// The queue list should already be cleared as part of the server shutdown event.
	assert(sysdata.queues);
	ll_free(sysdata.queues);
	free(sysdata.queues);
	sysdata.queues = NULL;
	
	// cleanup risp library.
	risp_shutdown(sysdata.risp);
	sysdata.risp = NULL;
    
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

	// cleanup the action pool.
	assert(sysdata.actpool != NULL);
	action_pool_free(sysdata.actpool);
	free(sysdata.actpool);
	sysdata.actpool = NULL;

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


