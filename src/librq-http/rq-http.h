#ifndef __RQ_HTTP_H
#define __RQ_HTTP_H

#include <rq.h>
#include <expbuf.h>


#if (LIBRQ_VERSION < 0x00010700)
	#error "Requires librq at least 1.07 or higher"
#endif


#define RQ_HTTP_VERSION	0x00000300
#define RQ_HTTP_VERSION_NAME "0.03.00"


                                            // command paramaters (0 to 31)
#define HTTP_CMD_NOP              0
#define HTTP_CMD_CLEAR            1
#define HTTP_CMD_EXECUTE          2
#define HTTP_CMD_SET_HEADER       3
#define HTTP_CMD_REPLY            4
                                            // flag parameters (32 to 63)
#define HTTP_CMD_METHOD_GET       32
#define HTTP_CMD_METHOD_POST      33
#define HTTP_CMD_METHOD_HEAD      34
                                            // byte integer (64 to 95)
                                            // short integer (96 to 127)
                                            // large integer (128 to 159) 
#define HTTP_CMD_LENGTH           128
                                            // short string (160 to 192)
#define HTTP_CMD_REMOTE_HOST      161
#define HTTP_CMD_LANGUAGE         162
#define HTTP_CMD_CONTENT_TYPE     163
                                            // string (192 to 223)
#define HTTP_CMD_HOST             192
#define HTTP_CMD_PATH             193
#define HTTP_CMD_KEY              194
#define HTTP_CMD_VALUE            195
#define HTTP_CMD_FILENAME         196
#define HTTP_CMD_PARAMS           197
                                            // large string (224 to 255)
#define HTTP_CMD_FILE             226


typedef struct {
	char method;
	char *host;
	char *path;
	char *params;
	short int inprocess;
		
	list_t *param_list;

	expbuf_t *reply;
	void *http;
	void *arg;
	rq_message_t *msg;
} rq_http_req_t;


typedef struct {
  rq_t *rq;
  char *queue;
  void (*handler)(rq_http_req_t *req);
  void *arg;
  risp_t *risp;
  list_t *req_list;
} rq_http_t;


rq_http_t * rq_http_new(rq_t *rq, char *queue, void (*handler)(rq_http_req_t *req), void *arg);
void rq_http_free(rq_http_t *http);

char * rq_http_getmimetype(char *filename);

void rq_http_reply(rq_http_req_t *req, char *ctype, expbuf_t *data);

char * rq_http_getpath(rq_http_req_t *req);

#endif
