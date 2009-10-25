//-----------------------------------------------------------------------------
// librq-http
// 
// Library to interact with the rq-http service.   It is used by the consumers
// that rq-http sends requests to.   The rq-http daemon also uses it to handle
// the results from the consumers.
//-----------------------------------------------------------------------------


#include "rq-http.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#if (RQ_HTTP_VERSION != 0x00000300)
#error "Compiling against incorrect version of rq-http.h"
#endif


typedef struct {
	char *key;
	char *value;
} param_t;


//-----------------------------------------------------------------------------
static rq_http_req_t * req_new(rq_http_t *http, void *arg)
{
	rq_http_req_t *req;

	assert(http);
	assert(arg);

	req = (rq_http_req_t *) malloc(sizeof(rq_http_req_t));
	req->method = 0;
	req->host = NULL;
	req->path = NULL;
	req->params = NULL;
	req->inprocess = 0;
	req->msg = NULL;

	// we dont actually process the params until a parameter is requested, so we
	// dont need to initialise the object yet, and is a good indicator to
	// determine if the params have been parsed or not.
	req->param_list = NULL;

	// PERF: get this buffer from the bufpool.
	req->reply = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(req->reply, 0);

	req->http = http;
	req->arg = arg;

	return(req);
}

//-----------------------------------------------------------------------------
static void req_free(rq_http_req_t *req)
{
	param_t *param;
	
	assert(req);

	assert(req->reply);
	assert(BUF_LENGTH(req->reply) == 0);
	expbuf_free(req->reply);
	free(req->reply);
	req->reply = NULL;

	// if the message is not NULL, then it means that we didn't send off the reply.  
	assert(req->msg == NULL);

	if (req->param_list) {
		assert(req->params);
		while((param = ll_pop_head(req->param_list))) {
			assert(param->key);
			free(param->key);
			assert(param->value);
			free(param->value);
			free(param);
		}
		ll_free(req->param_list);
		free(req->param_list);
		req->param_list = NULL;
	}

	if (req->host)   { free(req->host); }
	if (req->path)   { free(req->path); }
	if (req->params) { free(req->params); }

	free(req);
}


//-----------------------------------------------------------------------------
// this callback is called if we have an invalid command.  We shouldn't be
// receiving any invalid commands.  It should only really be used during
// primary development when the API is still fluctuating.  Once the API is
// stable, this function and mapping should be removed.
static void cmdInvalid(rq_http_req_t *ptr, void *data, risp_length_t len)
{
	unsigned char *cast;

	assert(ptr != NULL);
	assert(data != NULL);
	assert(len > 0);
	
	cast = (unsigned char *) data;
	printf("Received invalid (%d)): [%d, %d, %d]\n", len, cast[0], cast[1], cast[2]);
	assert(0);
}


//-----------------------------------------------------------------------------
// This callback function is to be fired when the CMD_CLEAR command is 
// received. In this case, it is not necessary, and should just check that
// everything is already in a cleared state.  It wouldn't make sense to receive
// a CLEAR in the middle of the payload, so we will treat it as a programmer
// error.
static void cmdClear(rq_http_req_t *req) 
{
 	assert(req);

	assert(req->http);
	assert(req->method == 0);
	assert(req->host == NULL);
	assert(req->path == NULL);
	assert(req->params == NULL);
	assert(req->param_list == NULL);
}


//-----------------------------------------------------------------------------
// This command means that all the information has been provided, and it needs
// to be actioned.  We dont return anything here.  The callback function will
// need to return somethng.
static void cmdExecute(rq_http_req_t *req)
{
	rq_http_t *http;
	
 	assert(req);

 	assert(req->http);
 	http = req->http;

 	assert(req->path);
	assert(http->handler);
	assert(http->arg);
	
	req->arg = http->arg;
	http->handler(req);
}


static void cmdMethodGet(rq_http_req_t *req)
{
 	assert(req);

 	assert(req->method == 0);
 	req->method = 'G';
}

static void cmdMethodPost(rq_http_req_t *req)
{
 	assert(req);

 	assert(req->method == 0);
 	req->method = 'P';
}

static void cmdMethodHead(rq_http_req_t *req)
{
 	assert(req);

 	assert(req->method == 0);
 	req->method = 'H';
}

static void cmdHost(rq_http_req_t *req, risp_length_t length, risp_char_t *data)
{
	assert(req);
	assert(length > 0);
	assert(data != NULL);

	assert(req->host == NULL);
	req->host = (char *) malloc(length+1);
	memcpy(req->host, data, length);
	req->host[length] = '\0';
}

static void cmdPath(rq_http_req_t *req, risp_length_t length, risp_char_t *data)
{
	assert(req);
	assert(length > 0);
	assert(data != NULL);

	assert(req->path == NULL);
	req->path = (char *) malloc(length+1);
	memcpy(req->path, data, length);
	req->path[length] = '\0';
}

static void cmdParams(rq_http_req_t *req, risp_length_t length, risp_char_t *data)
{
	assert(req);
	assert(length > 0);
	assert(data != NULL);

	assert(req->params == NULL);
	req->params = (char *) malloc(length+1);
	memcpy(req->params, data, length);
	req->params[length] = '\0';

	assert(req->param_list == NULL);
}

//-----------------------------------------------------------------------------
// This callback function is used when a complete message is received to
// consume.  We basically need to create a request to handle it, add it to the
// list.  If a reply is sent during the processing, then it will close out the
// request automatically, otherwise it will be up to something else to close it
// out.
static void message_handler(rq_message_t *msg, void *arg)
{
	int processed;
	rq_http_t *http;
	rq_http_req_t *req;

	assert(msg);
	assert(arg);
	
	http = (rq_http_t *) arg;
	assert(http);

	// We dont know what the use of this object will be, so we need to create it
	// and put it in a list (to keep track of it) until something gets rid of it.
	assert(http->arg);
	req = req_new(http, http->arg);
	req->msg = msg;

	assert(req->reply);
	assert(BUF_LENGTH(req->reply) == 0);

	assert(msg->data);
	assert(http->risp);
	processed = risp_process(http->risp, req, BUF_LENGTH(msg->data), (risp_char_t *) BUF_DATA(msg->data));
	assert(processed == BUF_LENGTH(msg->data));

	// if we still have the msg pointer as part of the request, then the message
	// hasn't been replied yet, so we need to add the request to the list and
	// let it finish elsewhere. 
	if (req->msg) {
		assert(req->inprocess == 0);
		req->inprocess++;

		// then we need to add this request to the list.
		assert(http->req_list);
		ll_push_head(http->req_list, req);
		req = NULL;
	}
	else {
		// We have already replied to the request, so we dont need it anymore.
		req_free(req);
		req = NULL;
	}
}



rq_http_t * rq_http_new
	(rq_t *rq, char *queue, void (*handler)(rq_http_req_t *req), void *arg)
{
	rq_http_t *http;

	assert(rq);
	assert(queue);
	assert(handler);
	assert((arg && handler) || (arg == NULL));

	// create http object.
	http = (rq_http_t *) malloc(sizeof(rq_http_t));
	http->rq = rq;
	http->queue = strdup(queue);
	assert(strlen(queue) < 256);
	http->handler = handler;
	http->arg = arg;

	http->req_list = (list_t *) malloc(sizeof(list_t));
	ll_init(http->req_list);

	// create RISP object
	http->risp = risp_init();
	assert(http->risp != NULL);
	risp_add_invalid(http->risp, cmdInvalid);
	risp_add_command(http->risp, HTTP_CMD_CLEAR, 	     &cmdClear);
	risp_add_command(http->risp, HTTP_CMD_EXECUTE,     &cmdExecute);
	risp_add_command(http->risp, HTTP_CMD_METHOD_GET,  &cmdMethodGet);
	risp_add_command(http->risp, HTTP_CMD_METHOD_POST, &cmdMethodPost);
	risp_add_command(http->risp, HTTP_CMD_METHOD_HEAD, &cmdMethodHead);
	risp_add_command(http->risp, HTTP_CMD_HOST,        &cmdHost);
	risp_add_command(http->risp, HTTP_CMD_PATH,        &cmdPath);
	risp_add_command(http->risp, HTTP_CMD_PARAMS,      &cmdParams);

// 	risp_add_command(http->risp, HTTP_CMD_SET_HEADER,  &cmdHeader);
// 	risp_add_command(http->risp, HTTP_CMD_LENGTH,      &cmdLength);
// 	risp_add_command(http->risp, HTTP_CMD_REMOTE_HOST, &cmdRemoteHost);
// 	risp_add_command(http->risp, HTTP_CMD_LANGUAGE,    &cmdLanguage);
// 	risp_add_command(http->risp, HTTP_CMD_FILE,        &cmdParams);
// 	risp_add_command(http->risp, HTTP_CMD_KEY,         &cmdKey);
// 	risp_add_command(http->risp, HTTP_CMD_VALUE,       &cmdValue);
// 	risp_add_command(http->risp, HTTP_CMD_FILENAME,    &cmdFilename);

	rq_consume(rq, http->queue, 200, RQ_PRIORITY_NORMAL, 0, message_handler, NULL, NULL, http);

	return(http);
}


void rq_http_free(rq_http_t *http)
{
	assert(http);

	assert(http->risp);
	risp_shutdown(http->risp);
	http->risp = NULL;

	assert(http->queue);
	free(http->queue);
	http->queue  = NULL;

	http->rq = NULL;
	http->handler = NULL;
	http->arg = NULL;

	assert(http->req_list);
	assert(ll_count(http->req_list) == 0);
	ll_free(http->req_list);
	free(http->req_list);
	http->req_list = NULL;

	free(http);
}


//-----------------------------------------------------------------------------
// given a filename, will return the mime-type for it, based on the extension.
//
// NOTE: variable names are shortened because we use them a lot in all the
//       comparisons.  Long variables just clutter things up.
//       To clarify, f=filename, l=length.
char * rq_http_getmimetype(char *f)
{
	int l;
	
	assert(f);
	l = strlen(f);
	assert(f[l] == '\0');

	// First check 4 letter extensions.
	if (l > 5) {
		if (f[l-5] == '.') {
			if (f[l-4] == 'h' && f[l-3] == 't' && f[l-2] == 'm' && f[l-1] == 'l') return "text/html";
			if (f[l-4] == 'j' && f[l-3] == 'p' && f[l-2] == 'e' && f[l-1] == 'g') return "image/jpeg";
		}
	}

	// Now check 3 letter extensions.
	if (l > 4) {
		if (f[l-4] == '.') {
			if (f[l-3] == 'j' && f[l-2] == 'p' && f[l-1] == 'g') return "image/jpeg";
			if (f[l-3] == 'h' && f[l-2] == 't' && f[l-1] == 'm') return "text/html";
		}
	}

	// if the file wasn't found elsewhere, then use the default.
	return "text/plain";
}



//-----------------------------------------------------------------------------
// External function that is used to reply to a http request.
void rq_http_reply(rq_http_req_t *req, char *ctype, expbuf_t *data)
{
	rq_http_t *http;
	
	assert(req);
	assert(ctype);
	assert(data);
	
	assert(req->reply);
	assert(BUF_LENGTH(req->reply) == 0);
	
	addCmd(req->reply, HTTP_CMD_CLEAR);
	addCmdShortStr(req->reply, HTTP_CMD_CONTENT_TYPE, strlen(ctype), ctype);
	addCmdLargeStr(req->reply, HTTP_CMD_FILE, BUF_LENGTH(data), BUF_DATA(data));
	addCmd(req->reply, HTTP_CMD_REPLY);

	// If we already have a reply, then we send it and then close off the request object.
	assert(req->msg);
	rq_reply(req->msg, BUF_LENGTH(req->reply), BUF_DATA(req->reply));
	expbuf_clear(req->reply);
	req->msg = NULL;

	if (req->inprocess > 0) {

		// need to remove the request from the list.
		assert(req->http);
		http = req->http;
		
		assert(http->req_list);
		ll_remove(http->req_list, req);

		req_free(req);
		req = NULL;
	}
}

// Return the path of the request.
char * rq_http_getpath(rq_http_req_t *req)
{
	assert(req);
	assert(req->path);
	return(req->path);
}



