//-----------------------------------------------------------------------------
// RISP Server
// Example code that demonstrate how to develop a server that communicates thru 
// a RISP protocol.
//-----------------------------------------------------------------------------


#include <risp.h>
#include <expbuf.h>
#include <rispbuf.h>
#include <rq.h>
#include "rq-log.h"

// includes
#include <assert.h>
// #include <errno.h>
#include <event.h>
// #include <fcntl.h>
// #include <netdb.h>
// #include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// #include <sys/resource.h>
// #include <sys/socket.h>
// #include <sys/time.h>
// #include <sys/types.h>
#include <unistd.h>


#define PACKAGE						"rq-log"
#define VERSION						"1.0"





typedef struct {
	// running params
	bool verbose;
	bool daemonize;
	char *username;
	char *pid_file;
	char *interface;

	// connections to the controllers.
	char *primary;
	char *secondary;
	int priport;
	int secport;

	// unique settings.
	char *path;
	int threshold;
	char *filename;
	char *queue;
	char *levelsqueue;
} settings_t;


typedef struct {
	rq_t              *rq;
	settings_t        *settings;
	risp_t            *risp;
	int                logfile;
	int                modified;
	
	// data received
	risp_command_t  op;
	int filter;
	struct {
		bool     set;
		int      value;
	} level, time;
	struct {
		bool     set;
		expbuf_t value;
	} text;

	rq_message_t *req;
} control_t;




//-----------------------------------------------------------------------------
// Global variables.
struct event_base *main_event_base = NULL;



//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-p <path>     Path to store log files.\n");
	printf("-t <num>      Filesize threshold (in mb).\n");
	printf("-f <filename> Filename prefix.\n");
	printf("-q <queue>    Queue to monitor.  Default: logger\n");
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
    printf("SIGINT handled.\n");
    assert(main_event_base != NULL);
    event_base_loopbreak(main_event_base);
}



// We've received a command to indicate 
void processSetLevel(control_t *ptr)
{
	assert(ptr != NULL);
	if (ptr->level.set == true) {
		ptr->filter = ptr->level.value;
	}
}


void processText(control_t *ptr)
{
	assert(ptr != NULL);
	assert(ptr->text.set == true);
	assert(ptr->text.value.length > 0);
	
	// if we dont have a file open, then we will need to open one.
	assert(0);
	
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
 	assert(ptr != NULL);
 	
	ptr->op = LOG_CMD_NOP;
	ptr->level.set = false;
	ptr->time.set = false;
	ptr->text.set = false;
 	
 	if (ptr->settings->verbose > 1) printf("node: CLEAR\n");
}


// This callback function is called when the CMD_EXECUTE command is received.  
// It should look at the data received so far, and figure out what operation 
// needs to be done on that data.  Since this is a simulation, and our 
// protocol doesn't really do anything useful, we will not really do much in 
// this example.   
void cmdExecute(control_t *ptr) 
{
 	assert(ptr != NULL);
	if (ptr->settings->verbose > 1) printf("node: EXECUTE (%d)\n", ptr->op);

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
 	assert(ptr != NULL);
	ptr->op = LOG_CMD_SETLEVEL;
	printf("node: SETLEVEL\n");
}

void cmdLevel(control_t *ptr, risp_int_t value)
{
 	assert(ptr != NULL);
 	assert(value >= 0 && value < 256);

	ptr->level.value = value;
	ptr->level.set = true;
	
	printf("node: LEVEL %d\n", value);
}

void cmdTime(control_t *ptr, risp_int_t value)
{
 	assert(ptr != NULL);

	ptr->time.value = value;
	ptr->time.set = true;
	
	printf("node: TIME %d\n", value);
}

void cmdText(control_t *ptr, risp_length_t length, risp_char_t *data)
{
	assert(ptr != NULL);
	assert(length > 0);
	assert(data != NULL);
	
	expbuf_set(&ptr->text.value, data, length);
	ptr->text.set = true;
	ptr->op = LOG_CMD_TEXT;
	
	printf("node: TEXT <%d>\n", length);
}





void settings_init(settings_t *ptr)
{
	assert(ptr != NULL);

	ptr->verbose = false;
	ptr->daemonize = false;
	ptr->username = NULL;
	ptr->pid_file = NULL;

	ptr->primary = NULL;
	ptr->priport = RQ_DEFAULT_PORT;
	ptr->secondary = NULL;
	ptr->secport = RQ_DEFAULT_PORT;
	
	ptr->path = NULL;
	ptr->threshold = (25 * 1024 * 1024);
	ptr->filename = NULL;
	ptr->queue = NULL;
	ptr->levelsqueue = NULL;
}

void settings_cleanup(settings_t *ptr) 
{
	assert(ptr != NULL);
}


static void timeout_handler(void *arg) {

	control_t *ptr;
	ptr = (control_t *) arg;
	assert(ptr != NULL);

// 	printf("Timer timout\n");
	
	assert(ptr->modified >= 0);
	if (ptr->modified > 0) {
		if (ptr->logfile != INVALID_HANDLE) {
			fsync(ptr->logfile);
		}
		ptr->modified = 0;
	}

	assert(ptr->rq != NULL);
	rq_settimeout(ptr->rq, 1000, timeout_handler, ptr);
}



void control_init(control_t *control)
{
	assert(control != NULL);

	control->settings = NULL;
	control->risp = NULL;
	control->rq = NULL;
	control->logfile = INVALID_HANDLE;
	control->modified = 0;
	
	control->level.set = false;
	control->time.set = false;
	control->text.set = false;
	expbuf_init(&control->text.value, 0);
}

void control_cleanup(control_t *control)
{
	assert(control != NULL);
	if (control->logfile != INVALID_HANDLE) {
		close(control->logfile);
		control->logfile = INVALID_HANDLE;
	}
	expbuf_free(&control->text.value);
}


void process_args(settings_t *settings, int argc, char **argv)
{
	int c;
	
	while ((c = getopt(argc, argv, "p:k:c:hvd:u:P:l:s:a:A:b:B:")) != -1) {
		switch (c) {
			case 'p':
				settings->path = optarg;
				assert(settings->path != NULL);
				break;
			case 't':
				settings->threshold = atoi(optarg);
				assert(settings->threshold > 0);
				break;
			
			case 'q':
				settings->queue = optarg;
				assert(settings->queue != NULL);
				break;
			case 'Q':
				assert(settings->levelsqueue == NULL);
				settings->levelsqueue = optarg;
				assert(settings->levelsqueue != NULL);
				break;
				
			
			case 'a':
				assert(settings->primary == NULL);
				settings->primary = optarg;
				assert(settings->primary != NULL);
				assert(settings->primary[0] != '\0');
				break;
			case 'A':
				settings->priport = atoi(optarg);
				assert(settings->priport > 0);
				break;

			case 'b':
				assert(settings->secondary == NULL);
				settings->secondary = optarg;
				assert(settings->secondary != NULL);
				assert(settings->secondary[0] != '\0');
				break;
			case 'B':
				settings->secport = atoi(optarg);
				assert(settings->secport > 0);
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
				assert(settings->interface == NULL);
				settings->interface = strdup(optarg);
				assert(settings->interface != NULL);
				assert(settings->interface[0] != '\0');
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				exit(EXIT_FAILURE);
		}
	}
}

// Handle the message that was sent over the queue.  the message itself uses
// the RISP method, so we pass the data on to the risp processor.
void message_handler(rq_message_t *msg, void *arg)
{
	int processed;
	control_t *control;

	assert(msg != NULL);
	control = (control_t *) arg;

	assert((msg->type == RQ_TYPE_REQUEST && msg->arg == NULL) || (msg->type == RQ_TYPE_REPLY));

	assert(msg->request.length > 0);
	assert(msg->request.data != NULL);
	assert(control != NULL);

	// since we are merely logging info, then we wont have any intermediate states.
	assert(control->req == NULL);
	control->req = msg;
	
	assert(msg->reply.data == NULL);
	assert(msg->reply.length == 0);

	// we are only expecting broadcast types.  we are not setup to handle anything else.
	assert(msg->broadcast != 0);

	assert(control->risp != NULL);
	processed = risp_process(control->risp, control, msg->request.length, msg->request.data);
	assert(processed == msg->request.length);

	// since all log messages should be sent with NOREPLY, then we will not need to reply to it.
	assert(msg->reply.data == NULL);
	assert(msg->reply.length == 0);

	control->req = NULL;
}



//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	control_t      *control  = NULL;

	// handle SIGINT 
	signal(SIGINT, sig_handler);
	
	// Create and init our controller object.   The  control object will have
	// everything in it that is needed to run this server.  It is in this object
	// because we need to pass a pointer to the handler that will be doing the
	// work.
	control = (control_t *) malloc(sizeof(control_t));
	assert(control != NULL);
	control_init(control);

	
	// create and init settings
	control->settings = (settings_t *) malloc(sizeof(settings_t));
	assert(control->settings != NULL);
	settings_init(control->settings);

	// process arguments
	process_args(control->settings, argc, argv);

	// check that we have all our required settings.
	if (control->settings->primary == NULL) {
		fprintf(stderr, "Need a primary controller specified.\n");
		exit(EXIT_FAILURE);
	}
	if (control->settings->path == NULL) {
		fprintf(stderr, "Need to specify a path for the logs.\n");
		exit(EXIT_FAILURE);
	}
	if (control->settings->queue == NULL) {
		control->settings->queue = "logger";
	}

	// If we need to run as a daemon, then do so, dropping privs to a specific
	// username if it was specified, and creating a pid file, if that was
	// specified.
	if (control->settings->daemonize) {
		if (rq_daemon(control->settings->username, control->settings->pid_file, control->settings->verbose) != 0) {
			fprintf(stderr, "failed to daemon() in order to daemonize\n");
			exit(EXIT_FAILURE);
		}
	}


	

	// Create the RQ object.   This will be used to communicate with the rq controller. 
	control->rq = (rq_t *) malloc(sizeof(rq_t));
	assert(control->rq);
	rq_init(control->rq);

	control->req = NULL;

	if (control->settings->verbose) printf("Initialising the event system.\n");
	assert(main_event_base == NULL);
	main_event_base = event_init();
	rq_setevbase(control->rq, main_event_base);

	if (control->settings->verbose) printf("Adding controller: %s:%d\n", control->settings->primary, control->settings->priport);
	assert(control->settings->primary != NULL);
	assert(control->settings->priport > 0);
	rq_addcontroller(control->rq, control->settings->primary, control->settings->priport);

	if (control->settings->secondary != NULL) {
		if (control->settings->verbose) printf("Adding controller: %s:%d\n", control->settings->secondary, control->settings->secport);
		assert(control->settings->secport > 0);
		rq_addcontroller(control->rq, control->settings->secondary, control->settings->secport);
	}

	// Initialise the risp system.
	assert(control->risp == NULL);
	control->risp = risp_init();
	assert(control->risp != NULL);
	risp_add_invalid(control->risp, cmdInvalid);
	risp_add_command(control->risp, LOG_CMD_CLEAR, 	     &cmdClear);
	risp_add_command(control->risp, LOG_CMD_EXECUTE,     &cmdExecute);
	risp_add_command(control->risp, LOG_CMD_LEVEL,       &cmdLevel);
 	risp_add_command(control->risp, LOG_CMD_SETLEVEL,    &cmdSetLevel);
	risp_add_command(control->risp, LOG_CMD_TIME,        &cmdTime);
	risp_add_command(control->risp, LOG_CMD_TEXT,        &cmdText);


	// connect to other controller.
	assert(control->settings->primary != NULL);
	assert(control != NULL);
	assert(control->rq != NULL);
	if (control->settings->verbose) printf("Connecting to controller\n");
	if (rq_connect(control->rq) != 0) {
		fprintf(stderr, "Unable to connect to controller.\n");
		exit(EXIT_FAILURE);
	}


	// tell RQ that we want a timeout event after 1000 miliseconds (1 second);
	// this is a one-time timeout, so when handling it, will need to set another.
	assert(control != NULL);
	assert(control->rq != NULL);
	if (control->settings->verbose) printf("Setting 1 second timeout\n");
	rq_settimeout(control->rq, 1000, timeout_handler, control);
	
	
	// connect to the specific queues that we want to connect to.  We wont use a
	// specific handler for the queue, but will use a generic handler for both
	// queues.  We can do this because both queues use compatible message
	// structures.
	assert(control != NULL);
	assert(control->rq != NULL);
	assert(control->settings != NULL);
	assert(control->settings->queue != NULL);
	if (control->settings->verbose) printf("Consuming queue: %s\n", control->settings->queue);

	rq_consume(control->rq, control->settings->queue, 0, RQ_PRIORITY_NORMAL, 1, message_handler, control);
	if (control->settings->levelsqueue != NULL) {
		rq_consume(control->rq, control->settings->levelsqueue, 0, RQ_PRIORITY_NONE, 0, message_handler, control);
	}
	
	// enter the processing loop.  This function will not return until there is
	// an interrupt and it is time for the service to shutdown.  Therefore
	// everything needs to be setup and running before this point.  Once inside
	// the rq_process function, everything is initiated by the RQ event system.
	assert(control != NULL);
	assert(control->rq != NULL);
	if (control->settings->verbose) printf("Starting RQ Process\n");
	rq_process(control->rq);

	if (control->settings->verbose) printf("\nShutting down\n");

	// cleanup risp library.
	assert(control != NULL);
	assert(control->risp != NULL);
	risp_shutdown(control->risp);
	control->risp = NULL;

	// cleanup the RQ stuff.
	assert(control->rq != NULL);
	rq_cleanup(control->rq);
	free(control->rq);
	control->rq = NULL;


	// remove the PID file if we're a daemon
	if (control->settings->daemonize && control->settings->pid_file != NULL) {
		assert(control->settings->pid_file[0] != 0);
		unlink(control->settings->pid_file);
	}

	// clean up the settings object and free it.
	assert(control->settings != NULL);
	settings_cleanup(control->settings);
	assert(control->settings != NULL);
	free(control->settings);
	control->settings = NULL;

	assert(control->req == NULL);

  // clean up the 'control' structure, closing any handles that are still open.
	assert(control != NULL);
	control_cleanup(control);
	assert(control != NULL);
	free(control);
	control = NULL;


	return 0;
}


