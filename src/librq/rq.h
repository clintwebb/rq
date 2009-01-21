#ifndef __RQ_H
#define __RQ_H

#include <event.h>
#include <netdb.h>
#include <expbuf.h>
#include <risp.h>


/* Get a consistent bool type */
#if HAVE_STDBOOL_H
# include <stdbool.h>
#else
  typedef enum {false = 0, true = 1} bool;
#endif


// Since we will be using a number of bit masks to check for data status's and so on, we should include some macros to make it easier.
#define BIT_TEST(arg,val) (((arg) & (val)) == (val))
#define BIT_SET(arg,val) ((arg) |= (val))
#define BIT_CLEAR(arg,val) ((arg) &= ~(val))
#define BIT_TOGGLE(arg,val) ((arg) ^= (val))






#define INVALID_HANDLE -1

// global constants and other things go here.
#define RQ_DEFAULT_PORT      13700

// start out with an 1kb buffer.  Whenever it is full, we will double the
// buffer, so this is just a starting point.
#define RQ_DEFAULT_BUFFSIZE	1024

// The messages sent through the controller can be of 3 types.  These defines
// would really only be used by the external handles to determine what it is.
#define RQ_TYPE_REQUEST				1
#define RQ_TYPE_REPLY					2

// The priorities are used to determine which node to send a request to.  A
// priority of NONE indicates taht this node should only receive broadcast
// messages, and no actual requests.
#define RQ_PRIORITY_NONE		0
#define RQ_PRIORITY_LOW        10
#define RQ_PRIORITY_NORMAL     20
#define RQ_PRIORITY_HIGH       30






// null param (0 to 63)
#define RQ_CMD_NOP              0
#define RQ_CMD_CLEAR            1
#define RQ_CMD_EXECUTE          2

#define RQ_CMD_REQUEST          10
#define RQ_CMD_REPLY            11
#define RQ_CMD_RECEIVED         12
#define RQ_CMD_DELIVERED        13
#define RQ_CMD_BROADCAST        14
#define RQ_CMD_NOREPLY          15
#define RQ_CMD_UNDELIVERED      16

#define RQ_CMD_CONSUME          20
#define RQ_CMD_CANCEL_QUEUE     21
#define RQ_CMD_CLOSING          22
#define RQ_CMD_SERVER_FULL      23
#define RQ_CMD_CONTROLLER       24
// byte integer (64 to 95)
#define RQ_CMD_PRIORITY         64
// short integer (96 to 127)
#define RQ_CMD_ID               96
#define RQ_CMD_QUEUEID          97
#define RQ_CMD_TIMEOUT          98
#define RQ_CMD_MAX              99
// large integer (128 to 159 
// short string (160 to 192)
#define RQ_CMD_QUEUE            160
// string (192 to 223)
// large string (224 to 255)
#define RQ_CMD_PAYLOAD          224

typedef struct {
	char *hostname;
	int resolved[5];
	int port;
} rq_conn_t;

typedef struct {
	char *queue;
	char  type;
	int   id;
	char  broadcast;
	char  noreply;
	struct {
		void *data;
		int   length;
	} request, reply;
	void *arg;
} rq_message_t;


typedef struct {
	char *queue;
	int   qid;
	void (*handler)(rq_message_t *msg, void *arg);
	void *arg;
} rq_queue_t;

typedef struct {
  risp_command_t op;
	char broadcast;
	char noreply;
	unsigned short id;
	unsigned short qid;
	unsigned short timeout;
	unsigned short priority;
	expbuf_t payload;
	expbuf_t queue;
} rq_data_t;

typedef struct {
	int handle;		// socket handle to the connected controller.
	struct event event;
	struct event_base *evbase;
	risp_t *risp;

	expbuf_t in, out, readbuf, build;

	rq_conn_t *connlist;
	int conns;
	int connactive;

	rq_queue_t **queuelist;
	int queues;

// 	void *arg;
// 	void (*handler)(rq_message_t *msg, void *arg);

	rq_data_t data;
} rq_t;


void rq_set_maxconns(int maxconns);
int  rq_new_socket(struct addrinfo *ai);

int  rq_daemon(char *username, char *pidfile, int noclose);
void rq_init(rq_t *rq);
void rq_cleanup(rq_t *rq);
void rq_setevbase(rq_t *rq, struct event_base *base);
void rq_addcontroller(rq_t *rq, char *host, int port);
int  rq_connect(rq_t *rq);
void rq_settimeout(rq_t *rq, unsigned int msecs, void (*handler)(void *arg), void *arg);
void rq_consume(rq_t *rq, char *queue, int max, int priority, void (*handler)(rq_message_t *msg, void *arg), void *arg);
void rq_process(rq_t *rq);

void rq_message_init(rq_message_t *msg);
void rq_message_clear(rq_message_t *msg);
void rq_message_setqueue(rq_message_t *msg, char *queue);
void rq_message_setbroadcast(rq_message_t *msg);
void rq_message_setnoreply(rq_message_t *msg);
void rq_message_setdata(rq_message_t *msg, int length, char *data);

void rq_send(rq_t *rq, rq_message_t *msg);



#endif

