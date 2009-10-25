//-----------------------------------------------------------------------------
// example-process
//	Main processing service for the 'example' site.
//
//	This site allows for simple authentication, and allows people to post text
//	information similar to pastebin.   It will utilise user authentication
//	models, and data access.
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


#define PACKAGE						"example-process"
#define VERSION						"1.0"

#if (RQ_HTTP_VERSION < 0x00000300)
	#error "This version requires at least v0.03.00 of librq-http"
#endif


#if (LIBLINKLIST_VERSION < 0x00008100)
	#error "liblinklist v0.81 or higher is required"
#endif



typedef struct {
	rq_http_req_t *hreq;
	expbuf_t *reply;
	struct __control_t *control;
	
	char *path;
	
} request_t;


typedef struct __control_t {
	struct event_base *evbase;
	rq_service_t *rqsvc;
	rq_http_t *http;
	struct event *sigint_event;

	list_t *pending;		/// request_t
} control_t;




//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// delete the signal event... the next time a SIGINT is received it will
	// kill the app without mercy, o we better shutdown quickly.
	assert(control->sigint_event);
	event_free(control->sigint_event);
	control->sigint_event = NULL;

	// need to initiate an RQ shutdown.
	assert(control->rqsvc);
	rq_svc_shutdown(control->rqsvc);
}


//-----------------------------------------------------------------------------
// create a new 'request' object.  This is used because we will potentially be
// handling more than one request at a time.  We will potentially be processing
// more than one at a time because other requests can be handled while queries
// are being sent to other services.
static request_t * req_new(rq_http_req_t *hreq, control_t *control)
{
	request_t *request;
	
	assert(hreq);
	assert(control);

	request = (request_t *) malloc(sizeof(request_t));
	request->hreq = hreq;
	request->control = control;

	// The reply buffer is used to handle the reply that will be sent for the query.
	request->reply = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(request->reply, 1024);

	// get the path for the request.  We will almost always need this, so 
	// might as well do it here.
	request->path = rq_http_getpath(request->hreq);
	assert(request->path);
	
	return(request);
}



// free the resources allocated with a request.
static void req_reply(request_t *request, char *ctype)
{
	assert(request);
	assert(ctype);

	// Send the reply... if there is one (a black reply is still valid)
	assert(request->hreq);
	assert(request->reply);
	rq_http_reply(request->hreq, ctype, request->reply);

	// Free the reply buffer, we dont need it anymore.
	assert(request->reply);
	expbuf_free(request->reply);
	free(request->reply);
	request->reply = NULL;

	// Remove the requet from our pending list.  
	assert(request->control);
	assert(request->control->pending);
	assert(ll_count(request->control->pending) > 0);
	ll_remove(request->control->pending, request);

	assert(request->path);
	
	free(request);
}

//-----------------------------------------------------------------------------
// This function is used to process a request.  It needs to look at the data we 
// have, and determine what data we need, and then gather the data.  When 
// everything has been gathered, put the html output together and return it.
static void main_proc(request_t *request)
{
	char *ctype = "text/html";

	assert(request);
	assert(request->reply);
	assert(BUF_LENGTH(request->reply) == 0);

	// get the path... so that we can determine what we need to do.

	assert(request->path);
	
	if (strcmp(request->path, "something.html") == 0) {
		expbuf_print(request->reply, "<html>\n");
		expbuf_print(request->reply, "<head>\n<title>test</title>\n</head>\n");
		expbuf_print(request->reply, "<body>\n");
		expbuf_print(request->reply, "test - %s<br>\n", request->path);
		expbuf_print(request->reply, "<a href=\"else.html\">click</a><br>\n");
		expbuf_print(request->reply, "</body>\n</html>\n");		
	}
	else if (strcmp(request->path, "else.html") == 0) {
		expbuf_print(request->reply, "<html>\n");
		expbuf_print(request->reply, "<head>\n<title>test</title>\n</head>\n");
		expbuf_print(request->reply, "<body>\n");
		expbuf_print(request->reply, "test - %s<br>\n", request->path);
		expbuf_print(request->reply, "<a href=\"something.html\">click</a><br>\n");
		expbuf_print(request->reply, "</body>\n</html>\n");		
	}
	else {
	  // no specific thing is being requested, therefore we must display the main introduction page.
	  
	  expbuf_print(request->reply, "<html>\n");
	  expbuf_print(request->reply, "<head>\n<title>rq-example</title>\n</head>\n");
	  expbuf_print(request->reply, "<body>\n");
	  expbuf_print(request->reply, "<h1>RQ Example</h1>\n");
	  expbuf_print(request->reply, "<p>This is an example site for RQ</p>\n");
	  expbuf_print(request->reply, "<span id=login>\n");
	  expbuf_print(request->reply, "</span>\n");
	  expbuf_print(request->reply, "<a href=\"something.html\">click</a><br>\n");
	  expbuf_print(request->reply, "</body>\n</html>\n");		
	}
	
	req_reply(request, ctype);
}



//-----------------------------------------------------------------------------
// When a HTTP request comes in, this callback function is called.   Create a 
// new request object to handle it, add it to the list, and then call the 
// mainproc to process it.
static void request_handler(rq_http_req_t *hreq)
{
	control_t *control;
	request_t *request;

	assert(hreq);
	assert(hreq->arg);
	control = hreq->arg;

	// create and initialise a request_t object.
	request = req_new(hreq, control);
	assert(request);

	// add the request to the 'pending' list.
	assert(control->pending);
	ll_push_tail(control->pending, request);

	// Then call the main_proc() function so that we can process what we've got.
	main_proc(request);
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

	// Create and init our controller object.   The  control object will have
	// everything in it that is needed to run this server.  It is in this object
	// because we need to pass a pointer to the handler that will be doing the 
	// work.
	control = (control_t *) malloc(sizeof(control_t));
	assert(control != NULL);
	control->rqsvc = NULL;
	control->sigint_event = NULL;
	control->pending = (list_t *) malloc(sizeof(list_t));
	ll_init(control->pending);
	
	// create new service object.
	service = rq_svc_new();
	control->rqsvc = service;

	// add the command-line options that are specific to this service.
	rq_svc_setname(service, PACKAGE " " VERSION);
	rq_svc_setoption(service, 'q', "queue",      "Queue to listen on for requests.");
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
	control->sigint_event = evsignal_new(control->evbase, SIGINT, sigint_handler, control);
	assert(control->sigint_event);
	event_add(control->sigint_event, NULL);

	
	// Tell the rq subsystem to connect to the rq servers.  It gets its info
	// from the common paramaters that it expects.  We dont really care if we 
	// lose a connection to the controllers, so we dont set any handlers for it.
	rq_svc_connect(service, NULL, NULL, NULL);
	
	// initialise the queue that we are consuming, provide callback handler.
	queue = rq_svc_getoption(service, 'q');
	if (queue == NULL) {
		fprintf(stderr, "Need to specify a queue.\n");
		exit(EXIT_FAILURE);
	}
	assert(queue); 
	assert(service->rq);

	// we will be processing rq-http requests, so we need to initialise that 
	// library and set request_handler as the callback routine.
	control->http = rq_http_new(service->rq, queue, request_handler, control);
	assert(control->http);

	


///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the processing loop.  This function will not return until there is
	// nothing more to do and the service has shutdown.  Therefore everything
	// needs to be setup and running before this point.  Once inside the
	// rq_process function, everything is initiated by the RQ event system (or 
	// libevent if you have established additional locket listeners or 
	// connections).
	assert(control != NULL);
	assert(control->evbase);
	event_base_loop(control->evbase, 0);

///============================================================================
/// Shutdown
///============================================================================

	// At this point the rq event loop has completed.  This means that there 
	// are no more events on the system.


	// since there are no more events, and we dont want to generate any more 
	// (because we are not going to ever process them), we want to free the 
	// event base.
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

	assert(control->pending);
	assert(ll_count(control->pending) == 0);

	// make sure signal handlers have been cleared.
	assert(control);
	assert(control->sigint_event == NULL);

	// we are done, cleanup what is left in the control structure.
	assert(control);
	assert(control->sigint_event == NULL);

	assert(control->pending);
	assert(ll_count(control->pending) == 0);
	ll_free(control->pending);
	free(control->pending);
	free(control);
	control = NULL;

	rq_svc_cleanup(service);

	return 0;
}


