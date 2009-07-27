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


#if (RQ_BLACKLIST_VERSION != 0x00010500)
	#error "This version designed only for v1.05.00 of librq-blacklist"
#endif


#if (LIBLINKLIST_VERSION < 0x00006000)
	#error "liblinklist v0.6 or higher is required"
#endif




typedef struct {
	in_addr_t start;
	in_addr_t end;
} entry_t;


typedef struct {
	struct event_base *evbase;
	rq_service_t *rqsvc;
	risp_t *risp;

	// unique settings.
	char *configfile;
	char *queue;

	struct event *sigint_event;
	struct event *sighup_event;

	rq_message_t *req;
	expbuf_t *reply;

	list_t *entries;		/// entry_t

	unsigned int start;
	unsigned int end;
	in_addr_t ip;

} control_t;







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
	char str[32];
	control_t *control = data;
	

	assert(s);
	assert(control);

	assert(len > 0);
	assert(len < 32);
	memcpy(str, s, len);
	str[len] = '\0';
	
	if (control->start == 0) {
		control->start = ntohl(inet_addr(str));
		assert(control->start != 0);
	}
	else if (control->end == 0) {
		control->end = ntohl(inet_addr(str));
		assert(control->end != 0);
	}
}

static void csv_line (int c, void *data)
{
	control_t *control = data;
	entry_t *entry;
	entry_t *tmp;
	
	assert(data);

	if (control->start != 0 && control->end != 0) {
		if (control->end >= control->start) {
		
			entry = (entry_t *)	malloc(sizeof(entry_t));
			assert(entry);
	
			entry->start = control->start;
			entry->end = control->end;

			control->start = 0;
			control->end = 0;

			if (control->entries == NULL) {
				control->entries = (list_t *) malloc(sizeof(list_t));
				ll_init(control->entries);
			}
			else {
				tmp = ll_get_tail(control->entries);
				assert(tmp);
				assert(tmp->end < entry->start);
			}
			ll_push_tail(control->entries, entry);
		}
		else {
			assert(0);
		}
	}
}


#define MAX_BUF 1024
static int config_load(control_t *control)
{
	struct csv_parser p;
	FILE *fp;
	char buf[MAX_BUF];
	size_t bytes_read;
	
	assert(control);
	assert(control->configfile);
	assert(control->entries == NULL);
	assert(control->start == 0 && control->end == 0);

	// open the file.
	fp = fopen(control->configfile, "rb");
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






//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// need to initiate an RQ shutdown.
	assert(control->rqsvc);
	rq_svc_shutdown(control->rqsvc);

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
//  	control_t *control = (control_t *) arg;

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

	assert(control->rqsvc);
	assert(control->rqsvc->rq);
	assert(msg->conn);
	assert(msg->conn->rq);
	assert(control->rqsvc->rq == msg->conn->rq);

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

 	// clear the ip.
 	assert(0);
}



// This callback function is called when the CMD_EXECUTE command is received.  
// It should look at the data received so far, and figure out what operation 
// needs to be done on that data.
static void cmdCheck(control_t *ptr)
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

	control->rqsvc = NULL;
	control->risp = NULL;

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
	assert(control->rqsvc == NULL);
	assert(control->risp == NULL);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
}




static void controller_connected_handler(rq_service_t *service, void *arg)
{
	control_t *control = arg;
	assert(service);
	assert(control);

	// what do we actually want to do when we finally do connect to the controller.
	assert(0);
}

static void controller_dropped_handler(rq_service_t *service, void *arg)
{
	control_t *control = arg;
	assert(service);
	assert(control);

	// what do we actually want to do when we lose a connection to a controller?
	assert(0);
}



//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	rq_service_t   *service;
	control_t      *control  = NULL;
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
	rq_svc_setoption(service, 'f', "filename", "blacklist .csv file.");
	rq_svc_setoption(service, 'q', "queue",    "Queue to listen on for requests.");
	rq_svc_process_args(service, argc, argv);
	rq_svc_initdaemon(service);
	
	assert(control->evbase == NULL);
	control->evbase = event_base_new();
	assert(control->evbase);
	rq_svc_setevbase(service, control->evbase);


	// initialise the risp system for processing what we receive on the queue.
	assert(control);
	assert(control->risp == NULL);
	control->risp = risp_init();
	assert(control->risp != NULL);
	risp_add_invalid(control->risp, cmdInvalid);
	risp_add_command(control->risp, BL_CMD_CLEAR, 	 &cmdClear);
	risp_add_command(control->risp, BL_CMD_CHECK,    &cmdCheck);
 	risp_add_command(control->risp, BL_CMD_IP,       &cmdIP);
 	
	// initialise signal handlers.
	assert(control);
	assert(control->evbase);
	assert(control->sigint_event == NULL);
	control->sigint_event = evsignal_new(control->evbase, SIGINT, sigint_handler, control);
	assert(control->sigint_event);
	event_add(control->sigint_event, NULL);
	assert(control->sighup_event == NULL);
	control->sighup_event = evsignal_new(control->evbase, SIGHUP, sighup_handler, control);
	assert(control->sighup_event);
	event_add(control->sighup_event, NULL);

	// load the config file that we assume is supplied.
	assert(control->configfile == NULL);
	control->configfile = rq_svc_getoption(service, 'f');
	if (control->configfile == NULL) {
		fprintf(stderr, "Configfile is required\n");
		exit(EXIT_FAILURE);
	}
	else {
		if (config_load(control) < 0) {
			fprintf(stderr, "Errors loading config file: %s\n", control->configfile);
			exit(EXIT_FAILURE);
		}
	}

	// Tell the rq subsystem to connect to the rq servers.  It gets its info
	// from the common paramaters that it expects.
	rq_svc_connect(service, NULL, NULL, NULL);
	
	// initialise the queue that we are consuming, provide callback handler.
	queue = rq_svc_getoption(service, 'q');
	assert(queue);
	assert(service->rq);
	rq_consume(service->rq, queue, 200, RQ_PRIORITY_NORMAL, 0, message_handler, NULL, NULL, control);

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
	control->rqsvc = NULL;


	// unload the config entries.
	assert(control);
	if (control->entries) {
		config_unload(control);
	}
	assert(control->entries == NULL);

	// make sure signal handlers have been cleared.
	assert(control);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);

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


