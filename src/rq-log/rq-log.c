//-----------------------------------------------------------------------------
// RISP Server
// Example code that demonstrate how to develop a server that communicates thru 
// a RISP protocol.
//-----------------------------------------------------------------------------


#include "rq-log.h"

// includes
#include <assert.h>
#include <event.h>
#include <evlogging.h>
#include <expbuf.h>
#include <linklist.h>
#include <risp.h>
#include <rispbuf.h>
#include <rq.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define PACKAGE						"rq-log"
#define VERSION						"1.0"




typedef struct {
	// running params
	bool verbose;
	bool daemonize;
	char *username;
	char *pid_file;

	// controllers addresses.
	list_t *controllers;

	// unique settings.
	char *filename;
	char *queue;
	char *levelsqueue;
} settings_t;

#define LOG_DATA_MASK_LEVEL 1
#define LOG_DATA_MASK_TIME  2
#define LOG_DATA_MASK_TEXT  4


typedef struct {
	struct event_base *evbase;
	rq_t              *rq;
	settings_t        *settings;
	risp_t            *risp;
	logging_t         *logging;

	struct event *sigint_event;
	struct event *sigusr1_event;
	struct event *sigusr2_event;

	// data received
	risp_command_t op;
	int filter;

	unsigned int mask;
	int level;
	expbuf_t text;

	rq_message_t *req;
} control_t;







//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-f <filename> Filename to log to.\n");
	printf("-q <queue>    Queue to monitor.  Default: logger\n");
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







// We've received a command to indicate
void processSetLevel(control_t *ptr)
{
	assert(ptr != NULL);
	
	if (BIT_TEST(ptr->mask, LOG_DATA_MASK_LEVEL)) {
		ptr->filter = ptr->level;
	}
}


void processText(control_t *ptr)
{
	assert(ptr != NULL);
	assert(BIT_TEST(ptr->mask, LOG_DATA_MASK_TEXT));
	assert(ptr->text.length > 0);

	printf("LOG: %s\n", expbuf_string(&ptr->text));
	
	// if we dont have a file open, then we will need to open one.
// 	assert(0);
	
	// write the text entry to the file.

	// increment our stats.

	// increment the flag so that the changes can be flushed by the timer.
	
}




void cmdNop(control_t *ptr) 
{
	assert(ptr != NULL);
}

void cmdInvalid(control_t *ptr, void *data, risp_length_t len)
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

// This callback function is to be fired when the CMD_CLEAR command is 
// received.  It should clear off any data received and stored in variables 
// and flags.  In otherwords, after this is executed, the node structure 
// should be in a predictable state.
void cmdClear(control_t *ptr) 
{
 	assert(ptr);
 	
	ptr->op = LOG_CMD_NOP;
	ptr->mask = 0;
}


// This callback function is called when the CMD_EXECUTE command is received.  
// It should look at the data received so far, and figure out what operation 
// needs to be done on that data.  Since this is a simulation, and our 
// protocol doesn't really do anything useful, we will not really do much in 
// this example.   
void cmdExecute(control_t *ptr) 
{
 	assert(ptr);

	// here we check what the current operation is.
	switch(ptr->op) {
		case LOG_CMD_SETLEVEL:
 			processSetLevel(ptr);
			break;

		case LOG_CMD_TEXT:
 			processText(ptr);
			break;
			
		default:
			// we should not have any other op than what we know about.
			assert(0);
			break;
	}
}


void cmdSetLevel(control_t *ptr)
{
 	assert(ptr);
	ptr->op = LOG_CMD_SETLEVEL;
}

void cmdLevel(control_t *ptr, risp_int_t value)
{
 	assert(ptr);
 	assert(value >= 0 && value < 256);

	ptr->level = value;
	BIT_SET(ptr->mask, LOG_DATA_MASK_LEVEL);
}

void cmdText(control_t *ptr, risp_length_t length, risp_char_t *data)
{
	assert(ptr);
	assert(length > 0);
	assert(data != NULL);
	
	expbuf_set(&ptr->text, data, length);
	BIT_SET(ptr->mask, LOG_DATA_MASK_TEXT);
	ptr->op = LOG_CMD_TEXT;
}


//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(fd < 0);
	assert(arg);

	// need to initiate an RQ shutdown.
	assert(control->rq);
	rq_shutdown(control->rq);

	// put logging system in direct mode (so that it no longer uses any events)
	assert(control->logging);
	log_direct(control->logging);

	// delete the signal events.
	assert(control->sigint_event);
	event_free(control->sigint_event);
	control->sigint_event = NULL;

	assert(control->sigusr1_event);
	event_free(control->sigusr1_event);
	control->sigusr1_event = NULL;

	assert(control->sigusr2_event);
	event_free(control->sigusr2_event);
	control->sigusr2_event = NULL;
}



//-----------------------------------------------------------------------------
// increase the log level.
static void sigusr1_handler(evutil_socket_t fd, short what, void *arg)
{
// 	control_t *control = (control_t *) arg;

	assert(fd < 0);
	assert(arg);

	// increase the log level.
	assert(0);

	// write a log entry to indicate the new log level.
	assert(0);
}


//-----------------------------------------------------------------------------
// decrease the log level.
static void sigusr2_handler(evutil_socket_t fd, short what, void *arg)
{
// 	control_t *control = (control_t *) arg;

	assert(fd < 0);
	assert(arg);

	// decrease the log level.
	assert(0);

	// write a log entry to indicate the new log level.
	assert(0);
}


//-----------------------------------------------------------------------------
// Handle the message that was sent over the queue.  the message itself uses
// the RISP method, so we pass the data on to the risp processor.
static void message_handler(rq_message_t *msg, void *arg)
{
	int processed;
	control_t *control;

	assert(msg);
	
	control = (control_t *) arg;
	assert(control);
	
	assert(control->req == NULL);
	control->req = msg;

	assert(control->rq == msg->rq);

	assert(control->risp);
	assert(msg->data);
	processed = risp_process(control->risp, control, BUF_LENGTH(msg->data), (risp_char_t *) BUF_DATA(msg->data));
	assert(processed == BUF_LENGTH(msg->data));

	if (msg->noreply == 0) {
		// if we need to send a reply, we re-use the 'data' buffer.
		expbuf_clear(msg->data);
		rq_msg_addcmd(msg, RQ_CMD_NOP);
		rq_reply(msg);
	}

	// is this correct?   
	assert(0);

	control->req = NULL;
}



//-----------------------------------------------------------------------------
// Create and init our controller object.   The  control object will have
// everything in it that is needed to run this server.  It is in this object
// because we need to pass a pointer to the handler that will be doing the work.
static void init_control(control_t *control)
{
	assert(control != NULL);

	control->risp = NULL;

	control->mask = 0;
	expbuf_init(&control->text, 0);

	control->rq = NULL;
	control->settings = NULL;
	control->req = NULL;

	control->sigint_event = NULL;
	control->sigusr1_event = NULL;
	control->sigusr2_event = NULL;

	control->logging = NULL;
}

static void cleanup_control(control_t *control)
{
	assert(control != NULL);

	assert(control->logging == NULL);
	assert(control->settings == NULL);
	assert(control->rq == NULL);
	assert(control->req == NULL);
	assert(control->sigint_event == NULL);
	assert(control->sigusr1_event == NULL);
	assert(control->sigusr2_event == NULL);
	
	expbuf_free(&control->text);
}

static void init_settings(control_t *control)
{
	assert(control);

	assert(control->settings == NULL);

	control->settings->verbose = 0;
	control->settings->daemonize = false;
	control->settings->username = NULL;
	control->settings->pid_file = NULL;

	control->settings->controllers = (list_t *) malloc(sizeof(list_t));
	ll_init(control->settings->controllers);
	
	control->settings->filename = NULL;
	control->settings->queue = NULL;
	control->settings->levelsqueue = NULL;
}

static void cleanup_settings(control_t *control)
{
	assert(control);

	assert(control->settings);

	assert(control->settings->controllers);
	assert(ll_count(control->settings->controllers) == 0);
	ll_free(control->settings->controllers);
	free(control->settings->controllers);
	control->settings->controllers = NULL;

	if (control->settings->filename) {
		free(control->settings->filename);
		control->settings->filename = NULL;
	}

	if (control->settings->queue) {
		free(control->settings->queue);
		control->settings->queue = NULL;
	}

	if (control->settings->levelsqueue) {
		free(control->settings->levelsqueue);
		control->settings->levelsqueue = NULL;
	}

	if (control->settings->username) {
		free(control->settings->username);
		control->settings->username = NULL;
	}

	if (control->settings->pid_file) {
		free(control->settings->pid_file);
		control->settings->pid_file = NULL;
	}
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
	if (control->settings->filename == NULL) {
		fprintf(stderr, "Need to specify a path for the log.\n");
		exit(EXIT_FAILURE);
	}
	if (control->settings->queue == NULL) {
		control->settings->queue = strdup("logger");
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
	rq_setevbase(control->rq, control->evbase);
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
}

static void cleanup_rq(control_t *control)
{
	assert(control);
	assert(control->rq);

	rq_cleanup(control->rq);
	free(control->rq);
	control->rq = NULL;
}

// Initialise the risp system.
static void init_risp(control_t *control)
{
	assert(control);
	assert(control->risp == NULL);

	control->risp = risp_init();
	assert(control->risp != NULL);
	risp_add_invalid(control->risp, cmdInvalid);
	risp_add_command(control->risp, LOG_CMD_CLEAR, 	     &cmdClear);
	risp_add_command(control->risp, LOG_CMD_EXECUTE,     &cmdExecute);
	risp_add_command(control->risp, LOG_CMD_LEVEL,       &cmdLevel);
 	risp_add_command(control->risp, LOG_CMD_SETLEVEL,    &cmdSetLevel);
	risp_add_command(control->risp, LOG_CMD_TEXT,        &cmdText);
}

// cleanup risp library.
static void cleanup_risp(control_t *control)
{
	assert(control);
	assert(control->risp);

	risp_shutdown(control->risp);
	control->risp = NULL;
}

static void init_signals(control_t *control)
{
	assert(control);
	assert(control->evbase);
	
	assert(control->sigint_event == NULL);
	control->sigint_event = evsignal_new(control->evbase, SIGINT, sigint_handler, control);
	assert(control->sigint_event);
	event_add(control->sigint_event, NULL);

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
// connect to the specific queues that we want to connect to.  We wont use a
// specific handler for the queue, but will use a generic handler for both
// queues.  We can do this because both queues use compatible message
// structures.
static void init_queues(control_t *control)
{
	assert(control);
	assert(control->settings);
	assert(control->settings->queue);
	assert(control->rq);
	
	rq_consume(control->rq, control->settings->queue, 2, RQ_PRIORITY_NORMAL, 1, message_handler, control);
	if (control->settings->levelsqueue) {
		rq_consume(control->rq, control->settings->levelsqueue, 0, RQ_PRIORITY_NONE, 0, message_handler, control);
	}
}

static void cleanup_queues(control_t *control)
{
	assert(control);

	// not really anything we can do about the queues, they get cleaned up automatically by RQ.
}

static void init_logfile(control_t *control)
{
	assert(control);
	assert(control->settings);
	assert(control->settings->filename);
	
	assert(control->logging == NULL);
	control->logging = (logging_t *) malloc(sizeof(logging_t));
	log_init(control->logging, control->settings->filename, 1);

	assert(control->evbase);
	log_buffered(control->logging, control->evbase);
}

static void cleanup_logfile(control_t *control)
{
	assert(control);
	assert(control->logging);

	assert(control->evbase == NULL);

	log_free(control->logging);
	free(control->logging);
	control->logging = NULL;
}

/// %%%%%%%



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
			"l:"  /* logfile */
			"q:"  /* logger queue */
			"Q:"  /* levels queue */
		))) {
		switch (c) {

			case 'c':
				assert(settings->controllers);
				ll_push_tail(settings->controllers, strdup(optarg));
				break;

			case 'l':
				assert(settings->filename == NULL);
				settings->filename = strdup(optarg);
				assert(settings->filename);
				break;
			case 'q':
				assert(settings->queue == NULL);
				settings->queue = strdup(optarg);
				assert(settings->queue);
				break;
			case 'Q':
				assert(settings->levelsqueue == NULL);
				settings->levelsqueue = strdup(optarg);
				assert(settings->levelsqueue);
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
	init_logfile(control);
	init_rq(control);
	init_risp(control);
	init_signals(control);
	init_controllers(control);
	init_queues(control);

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
	cleanup_queues(control);
	cleanup_controllers(control);
	cleanup_signals(control);
	cleanup_risp(control);
	cleanup_rq(control);
	cleanup_logfile(control);
	cleanup_daemon(control);
	cleanup_settings(control);
	cleanup_control(control);

	free(control);

	return 0;
}


