//-----------------------------------------------------------------------------
// rq-http
//	Service that accepts Http connections and passes control to particular
//	queues.
//-----------------------------------------------------------------------------


#include <rq-http.h>

#include "config.h"
#include "master-data.h"
#include "server.h"
#include "settings.h"
#include "signals.h"


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




//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-f <filename> Sqlite3 config file.\n");
	printf("\n");
	printf("-c <ip:port>  Controller to connect to.\n");
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
// Create and init our controller object.   The  control object will have
// everything in it that is needed to run this server.  It is in this object
// because we need to pass a pointer to the handler that will be doing the work.
static void init_control(control_t *control)
{
	assert(control != NULL);

	control->rq = NULL;
	control->settings = NULL;

	control->sigint_event = NULL;
	control->sighup_event = NULL;
	control->sigusr1_event = NULL;
	control->sigusr2_event = NULL;
}

static void cleanup_control(control_t *control)
{
	assert(control != NULL);

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

	// check that we have all our required settings.
	if (ll_count(control->settings->controllers) == 0) {
		fprintf(stderr, "Need at least one controller specified.\n");
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
	if (config_init(control->config, control->settings->configfile) < 0) {
		fprintf(stderr, "Unable to open config file: %s\n", control->settings->configfile);
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
		))) {
		switch (c) {

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


