#ifndef __RQ_H
#define __RQ_H

#include <event.h>
#include <netdb.h>
#include <expbuf.h>
#include <linklist.h>
#include <mempool.h>
#include <risp.h>

// This version indicates the version of the library so that developers of
// services can ensure that the correct version is installed.
// This version number should be incremented with every change that would
// effect logic.
#define __LIBRQ_VERSION  0x01


#if (LIBEVENT_VERSION_NUMBER < 0x02000100)
	#error "Needs libEvent v2.00.01 or higher"
#endif

/* Get a consistent bool type */
#if HAVE_STDBOOL_H
# include <stdbool.h>
#else
  typedef enum {false = 0, true = 1} bool;
#endif


// Since we will be using a number of bit masks to check for data status's and
// so on, we should include some macros to make it easier.
#define BIT_TEST(arg,val) (((arg) & (val)) == (val))
#define BIT_SET(arg,val) ((arg) |= (val))
#define BIT_CLEAR(arg,val) ((arg) &= ~(val))
#define BIT_TOGGLE(arg,val) ((arg) ^= (val))




#define INVALID_HANDLE -1

// global constants and other things go here.
#define RQ_DEFAULT_PORT      13700

// start out with an 1kb buffer.  Whenever it is full, we will double the
// buffer, so this is just a minimum starting point.
#define RQ_DEFAULT_BUFFSIZE	1024

// The messages sent through the controller can be of 3 types.  These defines
// would really only be used by the external handles to determine what it is.
#define RQ_TYPE_REQUEST				1
#define RQ_TYPE_REPLY					2
#define RQ_TYPE_BROADCAST     3

// The priorities are used to determine which node to send a request to.  A
// priority of NONE indicates taht this node should only receive broadcast
// messages, and no actual requests.
#define RQ_PRIORITY_NONE		    0
#define RQ_PRIORITY_LOW        10
#define RQ_PRIORITY_NORMAL     20
#define RQ_PRIORITY_HIGH       30





// null param (0 to 63)
#define RQ_CMD_NOP              0
#define RQ_CMD_CLEAR            1
#define RQ_CMD_EXECUTE          2
#define RQ_CMD_PING             5
#define RQ_CMD_PONG             6

#define RQ_CMD_REQUEST          10
#define RQ_CMD_REPLY            11
#define RQ_CMD_RECEIVED         12
#define RQ_CMD_DELIVERED        13
#define RQ_CMD_BROADCAST        14
#define RQ_CMD_UNDELIVERED      15

#define RQ_CMD_CONSUME          20
#define RQ_CMD_CANCEL_QUEUE     21
#define RQ_CMD_CLOSING          22
#define RQ_CMD_SERVER_FULL      23
#define RQ_CMD_CONTROLLER       24

#define RQ_CMD_NOREPLY          30
#define RQ_CMD_EXCLUSIVE        31

// byte integer (64 to 95)
#define RQ_CMD_PRIORITY         64
#define RQ_CMD_RETRIES					65
// short integer (96 to 127)
#define RQ_CMD_QUEUEID          96
#define RQ_CMD_TIMEOUT          97
#define RQ_CMD_MAX              98
// large integer (128 to 159 
#define RQ_CMD_ID               128
// short string (160 to 192)
#define RQ_CMD_QUEUE            160
// string (192 to 223)
// large string (224 to 255)
#define RQ_CMD_PAYLOAD          224


typedef int queue_id_t;
typedef int msg_id_t;

typedef struct {
	msg_id_t  id;
	char      type;
	char      noreply;
	expbuf_t  data;
	void     *queue;
} rq_message_t;

typedef struct {
	char *queue;
	queue_id_t qid;
	char exclusive;
	short int max;
	unsigned char priority;
	
	void (*handler)(rq_message_t *msg, void *arg);
	void *arg;
} rq_queue_t;


#define RQ_DATA_FLAG_REQUEST      1
#define RQ_DATA_FLAG_REPLY        2
#define RQ_DATA_FLAG_RECEIVED     4
#define RQ_DATA_FLAG_DELIVERED    8
#define RQ_DATA_FLAG_BROADCAST    16
#define RQ_DATA_FLAG_UNDELIVERED  32
#define RQ_DATA_FLAG_CLOSING      64
#define RQ_DATA_FLAG_SERVER_FULL  128
#define RQ_DATA_FLAG_NOREPLY      256

#define RQ_DATA_MASK_PRIORITY     1
#define RQ_DATA_MASK_QUEUEID      2
#define RQ_DATA_MASK_TIMEOUT      4
#define RQ_DATA_MASK_ID           8
#define RQ_DATA_MASK_QUEUE        16
#define RQ_DATA_MASK_PAYLOAD      32


typedef struct {
 	unsigned int mask;
	unsigned int flags;
	
	msg_id_t id;
	queue_id_t qid;
	unsigned short timeout;
	unsigned short priority;
	expbuf_t payload;
	expbuf_t queue;
} rq_data_t;

typedef struct {
	risp_t *risp;
	struct event_base *evbase;


	// linked-list of our connections.  Only the one at the head is likely to be
	// active (although it might not be).  When a connection is dropped or is
	// timed out, it is put at the bottom of the list.
	list_t connlist;

	// Linked-list of queues that this node is consuming.
	list_t queues;

} rq_t;


typedef struct {
	evutil_socket_t handle;		// socket handle to the connected controller.
	enum {
		unknown,
		connecting,
		active,
		closing,
		inactive
	} status;
	struct event *read_event, *write_event;
	rq_t *rq;
	risp_t *risp;
	
	char *hostname;
	int resolved[5];
	int port;
	
	expbuf_t in, out, readbuf, build;
	rq_data_t data;
	
	// linked-list of the messages that are being processed (on the head), and
	// the empty messages that can be used (at the tail)
	list_t messages;

} rq_conn_t;


void rq_set_maxconns(int maxconns);
int  rq_new_socket(struct addrinfo *ai);

int  rq_daemon(char *username, char *pidfile, int noclose);
void rq_init(rq_t *rq);
void rq_cleanup(rq_t *rq);
void rq_setevbase(rq_t *rq, struct event_base *base);
void rq_addcontroller(rq_t *rq, char *host, int port);
void rq_settimeout(rq_t *rq, unsigned int msecs, void (*handler)(void *arg), void *arg);
void rq_consume(rq_t *rq, char *queue, int max, int priority, int exclusive, void (*handler)(rq_message_t *msg, void *arg), void *arg);


void rq_message_init(rq_message_t *msg);
void rq_message_clear(rq_message_t *msg);
void rq_message_setqueue(rq_message_t *msg, char *queue);
void rq_message_setbroadcast(rq_message_t *msg);
void rq_message_setnoreply(rq_message_t *msg);
void rq_message_setdata(rq_message_t *msg, int length, char *data);

void rq_send(rq_t *rq, rq_message_t *msg);



#endif

