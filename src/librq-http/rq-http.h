#ifndef __RQ_HTTP_H
#define __RQ_HTTP_H

#include <rq.h>
#include <expbuf.h>



// null param (0 to 63)
#define HTTP_CMD_NOP              0
#define HTTP_CMD_CLEAR            1
#define HTTP_CMD_EXECUTE          2
#define HTTP_CMD_METHOD_GET       10
#define HTTP_CMD_METHOD_POST      11
#define HTTP_CMD_METHOD_HEAD      12
// byte integer (64 to 95)
// short integer (96 to 127)
// large integer (128 to 159) 
#define HTTP_CMD_LENGTH           128
// short string (160 to 192)
#define HTTP_CMD_HOST             160
#define HTTP_CMD_REMOTE_HOST      161
#define HTTP_CMD_LANGUAGE         162
#define HTTP_CMD_KEY              163
// string (192 to 223)
#define HTTP_CMD_PATH             192
#define HTTP_CMD_HEADER           193
#define HTTP_CMD_VALUE            194
#define HTTP_CMD_FILENAME         195
// large string (224 to 255)
#define HTTP_CMD_HEADERS          224
#define HTTP_CMD_PARAMS           225
#define HTTP_CMD_FILE             226


typedef struct {
//   rq_t *rq;
//   int handle;
//   char *host;
//   int port;
  
//   rq_message_t *msg;
} rq_http_t;



void rq_http_init(rq_http_t *http);
void rq_http_free(rq_http_t *http);



#endif
