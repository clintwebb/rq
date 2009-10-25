//-----------------------------------------------------------------------------
// rq-http-config
//	Service that accepts Http connections and passes control to particular
//	queues.
//-----------------------------------------------------------------------------




// includes
#include <assert.h>
#include <event.h>
#include <expbuf.h>
#include <linklist.h>
#include <rq.h>
#include <rq-http.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define PACKAGE						"rq-http-static"
#define VERSION						"1.1"


#if (RQ_HTTP_VERSION < 0x00000300)
	#error "This version designed only for v0.03.00 of librq-http"
#endif


#if (LIBLINKLIST_VERSION < 0x00008100)
	#error "liblinklist v0.81 or higher is required"
#endif



typedef struct {
	// data we get from the controller.
	expbuf_t *host;
	expbuf_t *path;

	rq_message_t *req;
	expbuf_t *reply;

} request_t;


typedef struct {
	struct event_base *evbase;
	rq_service_t *rqsvc;
	rq_http_t *http;

	struct event *sigint_event;
	struct event *sighup_event;

	char *basedir;
	char *index;
	
	expbuf_t *workingdir;
	expbuf_t *databuf;
} control_t;




//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// delete the signal events.
	assert(control->sigint_event);
	event_free(control->sigint_event);
	control->sigint_event = NULL;

	assert(control->sighup_event);
	event_free(control->sighup_event);
	control->sighup_event = NULL;

	// need to initiate an RQ shutdown.
	assert(control->rqsvc);
	rq_svc_shutdown(control->rqsvc);
}


//-----------------------------------------------------------------------------
// SIGHUP can be used to re-initialise, but in this case, it would be used to
// simply clear any cached info.  First iteration of this service will not
// cache anything though.
static void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
// 	control_t *control = (control_t *) arg;

	assert(arg);

	// clear out all cached objects.
	assert(0);
}







//-----------------------------------------------------------------------------
// Create and init our controller object.   The  control object will have
// everything in it that is needed to run this server.  It is in this object
// because we need to pass a pointer to the handler that will be doing the work.
static void init_control(control_t *control)
{
	assert(control != NULL);

	control->rqsvc = NULL;

	control->sigint_event = NULL;
	control->sighup_event = NULL;

	control->basedir = NULL;
	control->index = NULL;

	control->workingdir = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(control->workingdir, 0);

	control->databuf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(control->databuf, 0);
}

static void cleanup_control(control_t *control)
{
	assert(control);

	assert(control->databuf);
	assert(BUF_LENGTH(control->databuf) == 0);
	expbuf_free(control->databuf);
	free(control->databuf);
	control->databuf = NULL;

	assert(control->workingdir);
	assert(BUF_LENGTH(control->workingdir) == 0);
	expbuf_free(control->workingdir);
	free(control->workingdir);
	control->workingdir = NULL;

	control->basedir = NULL;
	control->index = NULL;
	
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
}



//-----------------------------------------------------------------------------
// When a HTTP request comes in, this callback function is called.   It will need to add the path to our basepath, and then check to see if the file is there
void request_handler(rq_http_req_t *req)
{
	control_t *control;
	FILE *fp;
	int length;
	int count;
	char *ctype;
	
	assert(req);
	control = req->arg;
	assert(control);

	// get the path from the request, add it to the basedir.
	assert(control->workingdir);
	assert(BUF_LENGTH(control->workingdir) == 0);
	assert(control->basedir);
	expbuf_set(control->workingdir, control->basedir, strlen(control->basedir));
	expbuf_add(control->workingdir, "/", 1);
	assert(req->path);
	expbuf_add(control->workingdir, req->path, strlen(req->path));

	// if last char of the path is '/' then add the default index file to the end of it.
	assert(BUF_DATA(control->workingdir));
	assert(BUF_LENGTH(control->workingdir) > 0);
	if (control->index && BUF_DATA(control->workingdir)[BUF_LENGTH(control->workingdir)-1] == '/' ) {
		expbuf_add(control->workingdir, control->index, strlen(control->index));
	}

	fprintf(stderr, "Opening file: %s\n", expbuf_string(control->workingdir));

	// attempt to open the file.
	fp = fopen(expbuf_string(control->workingdir), "r");
	if (fp == NULL) {
		fprintf(stderr, "Unable to open file: %s\n", expbuf_string(control->workingdir));

		// if attempt failed, check to see if filepath is actually a directory.
		// If so, then return a redirect.
		assert(0);

		// if not a directory, need to return a message indicating file wasn't found.
		assert(0);
	}
	

	if (fp) {
		// if file is opened, get the length of the file.
		fseek(fp, 0, SEEK_END);
		length = ftell(fp);
		fseek(fp, 0, SEEK_SET);

		fprintf(stderr, "File opened:\n");

		if (length > 0) {

			assert(length < (1024*1024*1024));
		
			// TODO: We will need to support loading a chunk of the file into memory
			//       and sending that.  If the file is too big and the requestor asked
			//       for all of it, we need to reply saying it is too big and we can
			//       only supply it in chunks.  The client may also have requested the
			//       file only from a certain range also, since that is possible I
			//       think.  Need to be able to handle both situations.
	
			// make sure our expanding buffer is big enough for it.
			assert(control->databuf);
			assert(BUF_LENGTH(control->databuf) == 0);
			if (BUF_MAX(control->databuf) < length) {
				expbuf_shrink(control->databuf, length);
			}
			assert(BUF_DATA(control->databuf));
		
			// read in the entire contents of the file into our expanding buffer.
			assert(length <= BUF_MAX(control->databuf));
			count = fread(BUF_DATA(control->databuf), length, 1, fp);
			fprintf(stderr, "read file '%s'.  count=%d, length=%d\n",
				expbuf_string(control->workingdir),
				count, length);
			assert(count == 1);
			BUF_LENGTH(control->databuf) = length;
		}
		
		// close the file.
		fclose(fp);


		// return the request with the data content.
	

		ctype = rq_http_getmimetype(expbuf_string(control->workingdir));
		assert(ctype);
		fprintf(stderr, "Content type for '%s': %s\n", expbuf_string(control->workingdir), ctype);

		assert(control->databuf);
		rq_http_reply(req, ctype, control->databuf);
	}


	expbuf_clear(control->workingdir);
	expbuf_clear(control->databuf);
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

	control = (control_t *) malloc(sizeof(control_t));
	init_control(control);
	// create new service object.
	service = rq_svc_new();
	control->rqsvc = service;

	// add the command-line options that are specific to this service.
	rq_svc_setname(service, PACKAGE " " VERSION);
	rq_svc_setoption(service, 'q', "queue",      "Queue to listen on for requests.");
	rq_svc_setoption(service, 'b', "dir",        "Base directory to find files.");
	rq_svc_setoption(service, 'i', "index-file", "File to be used if directory is requested.");
	rq_svc_process_args(service, argc, argv);
	rq_svc_initdaemon(service);
	
	assert(control->evbase == NULL);
	control->evbase = event_base_new();
	assert(control->evbase);
	rq_svc_setevbase(service, control->evbase);


	// initialise signal handlers.
	assert(control);
	assert(control->evbase);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
	control->sigint_event = evsignal_new(control->evbase, SIGINT, sigint_handler, control);
	control->sighup_event = evsignal_new(control->evbase, SIGHUP, sighup_handler, control);
	assert(control->sigint_event);
	assert(control->sighup_event);
	event_add(control->sigint_event, NULL);
	event_add(control->sighup_event, NULL);

	// load the config file that we assume is supplied.
	assert(control->basedir == NULL);
	control->basedir = rq_svc_getoption(service, 'b');
	if (control->basedir == NULL) {
		fprintf(stderr, "Base directory (option -b) is required\n");
		exit(EXIT_FAILURE);
	}
	else {
		// check that the basedir exists.
// 		assert(0);
	}

	// if an index value was supplied, then we should store that.
	assert(control->index == NULL);
	control->index = rq_svc_getoption(service, 'i');

	// Tell the rq subsystem to connect to the rq servers.  It gets its info
	// from the common paramaters that it expects.
	rq_svc_connect(service, NULL, NULL, NULL);
	
	// initialise the queue that we are consuming, provide callback handler.
	queue = rq_svc_getoption(service, 'q');
	if (queue == NULL) {
		fprintf(stderr, "Need to specify a queue.\n");
		exit(EXIT_FAILURE);
	}
	assert(queue);
	assert(service->rq);

	control->http = rq_http_new(service->rq, queue, request_handler, control);
	assert(control->http);

	


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

	// close the http interface.
	assert(control->http);
	rq_http_free(control->http);

	// make sure signal handlers have been cleared.
	assert(control);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);

	// we are done, cleanup what is left in the control structure.
	cleanup_control(control);
	free(control);

	rq_svc_cleanup(service);

	return 0;
}


