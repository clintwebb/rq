//-----------------------------------------------------------------------------
// rq-log
//	Logging service for an RQ system.
//-----------------------------------------------------------------------------


#include <rq-log.h>

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


#if (RQ_LOG_VERSION != 0x00001000)
	#error "This version designed only for v0.10.00 of librq-log"
#endif




#define LOG_DATA_MASK_LEVEL 1
#define LOG_DATA_MASK_TEXT  2


typedef struct {
	struct event_base *evbase;
	rq_service_t      *rqsvc;
	risp_t            *risp;
	logging_t         *logging;

	struct event *sigint_event;
	struct event *sigusr1_event;
	struct event *sigusr2_event;

	// data received
	int filter;

	unsigned int mask;
	int level;
	expbuf_t text;

	rq_message_t *req;

	char *filename;
} control_t;


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
	processText(ptr);
}


void cmdSetLevel(control_t *ptr)
{
 	assert(ptr);
	processSetLevel(ptr);
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
}


//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(control);

	// need to initiate an RQ shutdown.
	assert(control->rqsvc);
	rq_svc_shutdown(control->rqsvc);

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

	control->rqsvc = NULL;
	control->req = NULL;

	control->sigint_event = NULL;
	control->sigusr1_event = NULL;
	control->sigusr2_event = NULL;

	control->logging = NULL;
	control->filename = NULL;
}

static void cleanup_control(control_t *control)
{
	assert(control != NULL);

	assert(control->risp == NULL);
	assert(control->logging == NULL);
	assert(control->rqsvc == NULL);
	assert(control->req == NULL);
	assert(control->sigint_event == NULL);
	assert(control->sigusr1_event == NULL);
	assert(control->sigusr2_event == NULL);
	
	expbuf_free(&control->text);
}






//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	rq_service_t *service;
	control_t *control  = NULL;
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
	rq_svc_setoption(service, 'l', "logfile",  "Filename to log to.");
	rq_svc_setoption(service, 'q', "queue",    "Queue to listen on for logging requests.");
	rq_svc_setoption(service, 'Q', "levelsqueue",  "Queue to listen on for level changes.");
	rq_svc_process_args(service, argc, argv);
	rq_svc_initdaemon(service);
	
	// initialize event system.
	assert(control->evbase == NULL);
	control->evbase = event_base_new();
	assert(control->evbase);
	rq_svc_setevbase(service, control->evbase);

	// initialise the risp system for processing what we receive on the queue.
	assert(control);
	assert(control->risp == NULL);
	control->risp = risp_init();
	assert(control->risp);
	risp_add_invalid(control->risp, cmdInvalid);
	risp_add_command(control->risp, LOG_CMD_CLEAR, 	     &cmdClear);
	risp_add_command(control->risp, LOG_CMD_EXECUTE,     &cmdExecute);
	risp_add_command(control->risp, LOG_CMD_LEVEL,       &cmdLevel);
 	risp_add_command(control->risp, LOG_CMD_SETLEVEL,    &cmdSetLevel);
	risp_add_command(control->risp, LOG_CMD_TEXT,        &cmdText);
 	
	// initialise signal handlers.
	assert(control);
	assert(control->evbase);
	assert(control->sigint_event == NULL);
	assert(control->sigusr1_event == NULL);
	assert(control->sigusr2_event == NULL);
	control->sigint_event = evsignal_new(control->evbase, SIGINT, sigint_handler, control);
	control->sigusr1_event = evsignal_new(control->evbase, SIGUSR1, sigusr1_handler, control);
	control->sigusr2_event = evsignal_new(control->evbase, SIGUSR2, sigusr2_handler, control);
	event_add(control->sigint_event, NULL);
	event_add(control->sigusr1_event, NULL);
	event_add(control->sigusr2_event, NULL);
	assert(control->sigint_event);
	assert(control->sigusr1_event);
	assert(control->sigusr2_event);

	// load the config file that we assume is supplied.
	assert(control->filename == NULL);
	control->filename = rq_svc_getoption(service, 'f');
	if (control->filename == NULL) {
		fprintf(stderr, "log filename is required\n");
		exit(EXIT_FAILURE);
	}
	else {
		assert(control->logging == NULL);
		control->logging = (logging_t *) malloc(sizeof(logging_t));
		log_init(control->logging, control->filename, 1);
	
		assert(control->evbase);
		log_buffered(control->logging, control->evbase);
	}

	// Tell the rq subsystem to connect to the rq servers.  It gets its info
	// from the common paramaters that it expects.
	rq_svc_connect(service, NULL, NULL, NULL);
	
	// initialise the queue that we are consuming, provide callback handler.
	queue = rq_svc_getoption(service, 'q');
	assert(queue);
	assert(service->rq);
	rq_consume(service->rq, queue, 2, RQ_PRIORITY_NORMAL, 1, message_handler, NULL, NULL, control);
	
	// initialise the setlevels queue... an optional queue to consume... we will
	// only be getting broadcast messages on this one though.
	queue = rq_svc_getoption(service, 'Q');
 	if (queue) {
 		rq_consume(service->rq, queue, 1, RQ_PRIORITY_NONE, 0, message_handler, NULL, NULL, control);
 	}

	// we also want to make sure that when we lose the connection to the
	// controller, we indicate that we lost connection to the queue, unless we
	// have already established another controller connection.

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

	// the rq service sub-system has no real way of knowing when the event-base
	// has been cleared, so we need to tell it.
	rq_svc_setevbase(service, NULL);

	// make sure signal handlers have been cleared.
	assert(control);
	assert(control->sigint_event == NULL);
	assert(control->sigusr1_event == NULL);
	assert(control->sigusr2_event == NULL);

	// finish up the logging, and free it.
	assert(control);
	assert(control->logging);
	assert(control->evbase == NULL);
	log_free(control->logging);
	free(control->logging);
	control->logging = NULL;

	// cleanup risp library.
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


