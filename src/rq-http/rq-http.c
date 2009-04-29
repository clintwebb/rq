//-----------------------------------------------------------------------------
// RISP Server
// Example code that demonstrate how to develop a server that communicates thru 
// a RISP protocol.
//-----------------------------------------------------------------------------


#include "server.h"
#include "settings.h"

// includes
#include <assert.h>
#include <event.h>
#include <expbuf.h>
#include <risp.h>
#include <rispbuf.h>
#include <rq.h>
#include <rq-http.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define PACKAGE						"rq-http"
#define VERSION						"1.0"




// #define LOG_DATA_MASK_LEVEL 1
// #define LOG_DATA_MASK_TIME  2
// #define LOG_DATA_MASK_TEXT  4


// typedef struct {
// 	rq_t              rq;
// 	settings_t        *settings;
// 	risp_t            *risp;
	
	// data received

// 	unsigned int mask;
// 	int level, time;
// 	expbuf_t text;

// 	rq_message_t *req;
// } control_t;



//-----------------------------------------------------------------------------
// Global variables.
static struct event_base *_main_event_base = NULL;
static short int _shutdown = 0;
static short int _reload = 0;



//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-c <file>         sqlite config file.\n");
	printf("-L <log-queue>    Queue to send log details to.\n");
	printf("-s <stats-queue>  Filename prefix.\n");
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
// Handle the signal.  Any signal we receive can only mean that we need to exit.
static void sig_handler(const int sig) {

	if (sig == SIGINT) {
		printf("SIGINT handled.\n");
		_shutdown ++;
	  assert(_main_event_base != NULL);
  	event_base_loopbreak(_main_event_base);
	}
	else if (sig == SIGHUP) {
		printf("SIGHUP handled.\n");
		_reload ++;
	  assert(_main_event_base != NULL);
  	event_base_loopbreak(_main_event_base);
	}
}





static void timeout_handler(void *arg) {

	assert(0);

//	rq_settimeout(&ptr->rq, 1000, timeout_handler, ptr);
}



void process_args(settings_t *settings, int argc, char **argv)
{
	int c;
	
	while ((c = getopt(argc, argv,
			"hv"	/* Help... */
			"d:"	/* Daemonize */
			"u:"  /* User to re-own to */
			"P:"  /* PID file */
			"l:"  /* Interface to bind to */
			"L:"  /* Logger Queue */
			"s:"  /* Stats Queue */
			"z:"  /* gzip encoding Queue */
			"c:"	/* Blacklist Checking Queue */
			"a:"	/* Primary Controller IP */
			"A:"  /* Primary Controller Port */
			"b:"  /* Secondary Controller IP */
			"B:"  /* Secondary Controller Port */
		)) != -1) {
		switch (c) {
			case 'L':
				assert(0);	// dont yet support logging.
				settings->logqueue = optarg;
				break;
			case 's':
				assert(0);	// dont yet support stats.
				settings->statsqueue = optarg;
				break;
			case 'c':
				assert(0);	// dont yet support the blacklist.
				settings->blacklist = optarg;
				break;
			case 'z':
				assert(0);	// dont yet support the blacklist.
				settings->gzipqueue = optarg;
				break;

			case 'a':
				settings->primary = optarg;
				break;
			case 'A':
				settings->priport = atoi(optarg);
				break;

			case 'b':
				settings->secondary = optarg;
				break;
			case 'B':
				settings->secport = atoi(optarg);
				break;

			case 'h':
				usage();
				exit(EXIT_SUCCESS);
				break;
			case 'v':
				settings->verbose++;
				break;
			
			case 'd':
				settings->daemonize = true;
				break;
			case 'u':
				settings->username = optarg;
				break;
			case 'P':
				settings->pid_file = optarg;
				break;
			case 'l':
				settings->interface = strdup(optarg);
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
	server_t *server;
	settings_t *settings;
	rq_t *rq;

	// handle SIGINT 
	signal(SIGINT, sig_handler);
	signal(SIGHUP, sig_handler);
	
	// Create and init our controller object.   The  control object will have
	// everything in it that is needed to run this server.  It is in this object
	// because we need to pass a pointer to the handler that will be doing the
	// work.
	server = (server_t *) malloc(sizeof(server_t));
	server_init(server);

	
	// create and init settings
	settings = (settings_t *) malloc(sizeof(settings_t));
	settings_init(settings);
	server->settings = settings;

	// process arguments
	process_args(settings, argc, argv);

	// check that we have all our required settings.
	if (settings->primary == NULL) {
		fprintf(stderr, "Need a primary controller specified.\n");
		exit(EXIT_FAILURE);
	}

	// If we need to run as a daemon, then do so, dropping privs to a specific
	// username if it was specified, and creating a pid file, if that was
	// specified.
	if (settings->daemonize) {
		if (rq_daemon(settings->username, settings->pid_file, settings->verbose) != 0) {
			fprintf(stderr, "failed to daemon() in order to daemonize\n");
			exit(EXIT_FAILURE);
		}
	}

	if (settings->verbose) { printf("Creating RQ object\n"); }
	rq = (rq_t *) malloc(sizeof(rq_t));
	rq_init(rq);


	if (settings->verbose) printf("Initialising the event system.\n");
	assert(_main_event_base == NULL);
	_main_event_base = event_init();
	rq_setevbase(rq, _main_event_base);

	if (settings->verbose) printf("Adding controller: %s:%d\n", settings->primary, settings->priport);
	assert(settings->primary != NULL);
	assert(settings->priport > 0);
	rq_addcontroller(rq, settings->primary, settings->priport);

	if (settings->secondary != NULL) {
		if (settings->verbose) printf("Adding controller: %s:%d\n", settings->secondary, settings->secport);
		assert(settings->secport > 0);
		rq_addcontroller(rq, settings->secondary, settings->secport);
	}



	// connect to other controller.
	assert(settings->primary != NULL);
	if (settings->verbose) printf("Connecting to controller\n");
	if (rq_connect(rq) != 0) {
		fprintf(stderr, "Unable to connect to controller.\n");
		exit(EXIT_FAILURE);
	}


	// tell RQ that we want a timeout event after 1000 miliseconds (1 second);
	// this is a one-time timeout, so when handling it, will need to set another.
	if (settings->verbose) printf("Setting 1 second timeout\n");
	rq_settimeout(rq, 1000, timeout_handler, settings);
	
	

	// enter the processing loop.  This function will not return until there is
	// an interrupt and it is time for the service to shutdown.  Therefore
	// everything needs to be setup and running before this point.  Once inside
	// the rq_process function, everything is initiated by the RQ event system.
	if (settings->verbose) printf("Starting RQ Process\n");
	rq_process(rq);

	if (settings->verbose) printf("\nShutting down\n");


	assert(rq);
	rq_cleanup(rq);
	free(rq);
	rq = NULL;

	// remove the PID file if we're a daemon
	if (settings->daemonize && settings->pid_file != NULL) {
		assert(settings->pid_file[0] != 0);
		unlink(settings->pid_file);
	}



	// clean up the settings object and free it.
	assert(settings != NULL);
	settings_cleanup(settings);
	assert(settings != NULL);
	free(settings);
	settings = NULL;


	return 0;
}


