//-----------------------------------------------------------------------------
// rq-blacklist
//	Service that interfaces a database that describes IP addresses that need to
//	denied access to our systems.  On startup (or when SIGHUP is received), it
//	will load the information in the database into memory.  Since the blacklist
//	is not expected to be a large amount of data, this should not be a problem.
//
//  We will maintain a sorted list of some sort that contains a pair of min/max
//  ip addresses.  Any ip address that falls in this range will be blocked.
//-----------------------------------------------------------------------------


#include <rq-blacklist.h>


// includes
#include <arpa/inet.h>
#include <assert.h>
#include <event.h>
#include <expbuf.h>
#include <libcsv/csv.h>
#include <linklist.h>
#include <netinet/in.h>
#include <risp.h>
#include <rq.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>





#define PACKAGE						"rq-blacklist"
#define VERSION						"1.0"


#if (RQ_BLACKLIST_VERSION != 0x00010000)
	#error "This version designed only for v1.00.00 of librq-blacklist"
#endif


#if (LIBLINKLIST_VERSION < 0x00006000)
	#error "liblinklist v0.6 or higher is required"
#endif



typedef struct {
	// running params
	short verbose;
	short daemonize;
	char *username;
	char *pid_file;

	// connections to the controllers.
	list_t *controllers;

	// unique settings.
	char *configfile;
	char *queue;
} settings_t;




typedef struct {
	in_addr_t start;
	in_addr_t end;
} entry_t;


typedef struct {
	struct event_base *evbase;
	rq_t              *rq;
	risp_t						*risp;
	settings_t        *settings;
	expbuf_pool_t     *bufpool;

	struct event *sigint_event;
	struct event *sighup_event;

	rq_message_t *req;
	expbuf_t *reply;

	list_t *entries;		/// entry_t

	unsigned int start;
	unsigned int end;
	in_addr_t ip;

} control_t;




//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
static void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-f <filename>       blacklist .csv file.\n");
	printf("-q <queue>          Queue to listen on for requests.\n");
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


static void config_unload(control_t *control) {
	entry_t *entry;

	assert(control);
	while ((entry = ll_pop_head(control->entries))) {
		free(entry);
	}
	ll_free(control->entries);
	free(control->entries);
	control->entries = NULL;
	control->start = 0;
	control->end = 0;
}




static void csv_data (void *s, size_t len, void *data)
{
	control_t *control = data;
	in_addr_t value;

	assert(s);
	assert(control);
	
	if (control->start == 0 || control->end == 0) {
		value = inet_addr(s);
		assert(value != 0);
		if (control->start == 0) control->start = value;
		else if (control->end == 0) control->end = value;
	}
}

static void csv_line (int c, void *data)
{
	control_t *control = data;
	entry_t *entry;
	entry_t *tmp;
	
	assert(data);

	if (control->start != 0 && control->end != 0) {
		if (control->end > control->start) {
		
			entry = (entry_t *)	malloc(sizeof(entry_t));
			assert(entry);
	
			entry->start = control->start;
			entry->end = control->end;

			printf("%u - %u\n", entry->start, entry->end);

			if (control->entries == NULL) {
				control->entries = (list_t *) malloc(sizeof(list_t));
				ll_init(control->entries);
				ll_push_tail(control->entries, entry);
			}
			else {
				tmp = ll_get_tail(control->entries);
				assert(entry->start > tmp->end);
				ll_push_tail(control->entries, entry);
			}
		}
		else {
			assert(0);
		}
	}
}


#define MAX_BUF 1024
static int config_load(control_t *control, const char *configfile)
{
	struct csv_parser p;
	FILE *fp;
	char buf[MAX_BUF];
	size_t bytes_read;
	
	assert(control);
	assert(configfile);
	assert(control->entries == NULL);
	assert(control->start == 0 && control->end == 0);
	
	// open the file.
	assert(control->settings->configfile);
	fp = fopen(control->settings->configfile, "rb");
	if (fp) {

		if (csv_init(&p, 0) == 0) {
			while ((bytes_read=fread(buf, 1, MAX_BUF, fp)) > 0) {
				if (csv_parse(&p, buf, bytes_read, csv_data, csv_line, control) != bytes_read) {
					fprintf(stderr, "Error while parsing file: %s\n",
					csv_strerror(csv_error(&p)) );
					exit(EXIT_FAILURE);
				}
			}

			csv_fini(&p, csv_data, csv_line, control);
			csv_free(&p);
		}
	
		fclose(fp);
		return 0;
	}
	else {
		return -1;
	}
}
#undef MAX_BUF

static void settings_init(settings_t *ptr)
{
	assert(ptr != NULL);

	ptr->verbose = false;
	ptr->daemonize = false;
	ptr->username = NULL;
	ptr->pid_file = NULL;

	ptr->configfile = NULL;
	ptr->queue = NULL;

	ptr->controllers = (list_t *) malloc(sizeof(list_t));
	ll_init(ptr->controllers);
}

static void settings_free(settings_t *ptr)
{
	assert(ptr != NULL);

	if (ptr->configfile) { free(ptr->configfile); ptr->configfile = NULL; }
	if (ptr->queue) { free(ptr->queue); ptr->queue = NULL; }

	assert(ptr->controllers);
	ll_free(ptr->controllers);
	free(ptr->controllers);
	ptr->controllers = NULL;
}




//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
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
static void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// clear out all cached objects.
	assert(0);

	// reload the config database file.
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

	// since we will only be processing one request at a time, and there are no
	// paths for blocking when processing the request, we will put the request
	// in the control structure.  If we were processing more than one, we would
	// create a list of pending requests which contain the control structure in
	// it.
	assert(control->req == NULL);
	control->req = msg;

	assert(control->rq == msg->rq);

	assert(control->risp);
	assert(msg->data);
	processed = risp_process(control->risp, control, BUF_LENGTH(msg->data), (risp_char_t *) BUF_DATA(msg->data));
	assert(processed == BUF_LENGTH(msg->data));

	// we need to get the reply and return it.  Has that been done?
	assert(0);

	// is this correct?   
	assert(0);

	control->req = NULL;
}


static void cmdNop(control_t *ptr) 
{
	assert(ptr != NULL);
}

static void cmdInvalid(control_t *ptr, void *data, risp_length_t len)
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
static void cmdClear(control_t *ptr) 
{
 	assert(ptr);

 	// clear the host and path info.
 	assert(0);
}



// This callback function is called when the CMD_EXECUTE command is received.  
// It should look at the data received so far, and figure out what operation 
// needs to be done on that data.
static void cmdExecute(control_t *ptr) 
{

 	assert(ptr);
 	assert(ptr->req);

	// lookup the data.
	assert(0);

}


static void cmdIP(control_t *ptr, int data)
{
	assert(ptr);

	ptr->ip = data;
}



//-----------------------------------------------------------------------------
// Create and init our controller object.   The  control object will have
// everything in it that is needed to run this server.  It is in this object
// because we need to pass a pointer to the handler that will be doing the work.
static void init_control(control_t *control)
{
	assert(control != NULL);

	control->rq = NULL;
	control->risp = NULL;
	control->settings = NULL;
	control->bufpool = NULL;

	control->sigint_event = NULL;
	control->sighup_event = NULL;

	control->req = NULL;
	control->ip = 0;
	
	control->reply = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(control->reply, 0);

	control->entries = NULL;
	control->start = 0;
	control->end = 0;
}

static void cleanup_control(control_t *control)
{
	assert(control != NULL);

	assert(control->reply);
	expbuf_clear(control->reply);
	expbuf_free(control->reply);
	free(control->reply);
	control->reply = NULL;

	if (control->entries) {
		config_unload(control);
	}

	assert(control->req == NULL);
	assert(control->bufpool == NULL);
	assert(control->settings == NULL);
	assert(control->rq == NULL);
	assert(control->risp == NULL);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
}

static void init_settings(control_t *control)
{
	assert(control);
	assert(control->settings == NULL);
	control->settings = (settings_t *) malloc(sizeof(settings_t));
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

	if (control->settings->queue == NULL) {
		fprintf(stderr, "Need to specify a queue.\n");
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

//-----------------------------------------------------------------------------
static void init_events(control_t *control)
{
	assert(control->evbase == NULL);
	control->evbase = event_base_new();
	assert(control->evbase);
}

//-----------------------------------------------------------------------------
static void cleanup_events(control_t *control)
{
	assert(control);
	assert(control->evbase);

	event_base_free(control->evbase);
	control->evbase = NULL;
}


//-----------------------------------------------------------------------------
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

//-----------------------------------------------------------------------------
static void cleanup_rq(control_t *control)
{
	assert(control);
	assert(control->rq);

	rq_cleanup(control->rq);
	free(control->rq);
	control->rq = NULL;
}


//-----------------------------------------------------------------------------
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
}

//-----------------------------------------------------------------------------
static void cleanup_signals(control_t *control)
{
	assert(control);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
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

//-----------------------------------------------------------------------------
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
	assert(control->entries == NULL);

	if (config_load(control, control->settings->configfile) < 0) {
		fprintf(stderr, "Errors loading config file: %s\n", control->settings->configfile);
		exit(EXIT_FAILURE);
	}
}

static void cleanup_config(control_t *control)
{
	assert(control);

	if (control->entries) {
		config_unload(control);
	}

	assert(control->entries == NULL);
}


//-----------------------------------------------------------------------------
static void init_bufpool(control_t *control)
{
	assert(control);
	assert(control->bufpool == NULL);

	control->bufpool = (expbuf_pool_t *) malloc(sizeof(expbuf_pool_t));
	expbuf_pool_init(control->bufpool, 0);
}

//-----------------------------------------------------------------------------
static void cleanup_bufpool(control_t *control)
{
	assert(control);
	assert(control->bufpool);

	expbuf_pool_free(control->bufpool);
	free(control->bufpool);
	control->bufpool = NULL;
}

//-----------------------------------------------------------------------------
// connect to the specific queues that we want to connect to.
static void init_queues(control_t *control)
{
	assert(control);
	assert(control->settings);
	assert(control->settings->queue);
	assert(control->rq);
	
	rq_consume(control->rq, control->settings->queue, 200, RQ_PRIORITY_NORMAL, 0, message_handler, control);
}

static void cleanup_queues(control_t *control)
{
	assert(control);

	// not really anything we can do about the queues, they get cleaned up automatically by RQ.
}

// Initialise the risp system.
static void init_risp(control_t *control)
{
	assert(control);
	assert(control->risp == NULL);

	control->risp = risp_init();
	assert(control->risp != NULL);
	risp_add_invalid(control->risp, cmdInvalid);
	risp_add_command(control->risp, BL_CMD_CLEAR, 	 &cmdClear);
	risp_add_command(control->risp, BL_CMD_EXECUTE,  &cmdExecute);
 	risp_add_command(control->risp, BL_CMD_IP,       &cmdIP);
}

// cleanup risp library.
static void cleanup_risp(control_t *control)
{
	assert(control);
	assert(control->risp);

	risp_shutdown(control->risp);
	control->risp = NULL;
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
			"q:"  /* queue to listen on */
		))) {
		switch (c) {

			case 'q':
				assert(settings->queue == NULL);
				settings->queue = strdup(optarg);
				assert(settings->queue);
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
	init_risp(control);
	init_signals(control);
	init_config(control);
	init_controllers(control);
	init_bufpool(control);
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
	cleanup_bufpool(control);
	cleanup_config(control);
	cleanup_controllers(control);
	cleanup_signals(control);
	cleanup_risp(control);
	cleanup_rq(control);

	cleanup_daemon(control);
	cleanup_settings(control);
	cleanup_control(control);

	free(control);

	return 0;
}


