#ifndef __RQ_H
#define __RQ_H

#include <event.h>
#include <netdb.h>
#include <expbuf.h>
#include <expbufpool.h>
#include <linklist.h>
// #include <mempool.h>
#include <risp.h>
#include <rispbuf.h>

// This version indicates the version of the library so that developers of
// services can ensure that the correct version is installed.
// This version number should be incremented with every change that would
// effect logic.
#define LIBRQ_VERSION  0x00010800
#define LIBRQ_VERSION_NAME "v1.08.00"


#if (LIBEVENT_VERSION_NUMBER < 0x02000200)
	#error "Needs libEvent v2.00.02 or higher"
#endif

// #if (MEMPOOL_VERSION < 0x00010200)
// 	#error "Needs libmempool v1.02.00 or higher"
// #endif

#if (EXPBUF_VERSION < 0x00010200)
	#error "Needs libexpbuf v1.2.1 or higher"
#endif

#if (LIBLINKLIST_VERSION < 0x00008000)
	#error "Needs liblinklist v0.80 or higher"
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

// The priorities are used to determine which node to send a request to.  A
// priority of NONE indicates taht this node should only receive broadcast
// messages, and no actual requests.
#define RQ_PRIORITY_NONE		    0
#define RQ_PRIORITY_LOW        10
#define RQ_PRIORITY_NORMAL     20
#define RQ_PRIORITY_HIGH       30





/// execute commands (0 to 31)
#define RQ_CMD_NOP              0
#define RQ_CMD_CLEAR            1
#define RQ_CMD_PING             5
#define RQ_CMD_PONG             6
#define RQ_CMD_REQUEST          10
#define RQ_CMD_REPLY            11
#define RQ_CMD_DELIVERED        13
#define RQ_CMD_BROADCAST        14
#define RQ_CMD_UNDELIVERED      16
#define RQ_CMD_CONSUME          20
#define RQ_CMD_CANCEL_QUEUE     21
#define RQ_CMD_CLOSING          22
#define RQ_CMD_SERVER_FULL      23
#define RQ_CMD_CONSUMING        24

/// flags (32 to 63)
#define RQ_CMD_EXCLUSIVE        32
#define RQ_CMD_NOREPLY          33

/// byte integer (64 to 95)
#define RQ_CMD_PRIORITY         64
#define RQ_CMD_RETRIES					65
/// short integer (96 to 127)
#define RQ_CMD_QUEUEID          96
#define RQ_CMD_TIMEOUT          97
#define RQ_CMD_MAX              98
/// large integer (128 to 159 
#define RQ_CMD_ID               128
/// short string (160 to 192)
#define RQ_CMD_QUEUE            160
/// string (192 to 223)
/// large string (224 to 255)
#define RQ_CMD_PAYLOAD          224


typedef int queue_id_t;
typedef int msg_id_t;


typedef struct {
	risp_t *risp;
	struct event_base *evbase;

	// linked-list of our connections.  Only the one at the head is likely to be
	// active (although it might not be).  When a connection is dropped or is
	// timed out, it is put at the bottom of the list.
	list_t connlist;		/// rq_conn_t

	// Linked-list of queues that this node is consuming.
	list_t queues;			/// rq_queue_t

	// mempool of messages.
	list_t *msg_pool;

	void **msg_list;
	int msg_max;
	int msg_used;
	int msg_next;

	// Buffer pool.
	expbuf_pool_t *bufpool;
} rq_t;





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
	expbuf_t *payload;
	expbuf_t *queue;
} rq_data_t;


typedef struct {
	evutil_socket_t handle;		// socket handle to the connected controller.
	char active;
	char closing;
	char shutdown;
	struct event *read_event;
	struct event *write_event;
	struct event *connect_event;
	rq_t *rq;
	risp_t *risp;
	char *hostname;
	
	expbuf_t *inbuf, *outbuf, *readbuf;
	rq_data_t *data;
	
} rq_conn_t;


typedef struct __rq_message_t {
	msg_id_t  id;
	msg_id_t  src_id;
	char      broadcast;
	char      noreply;
	expbuf_t *data;
	char     *queue;
	rq_t     *rq;
	rq_conn_t *conn;
	enum {
		rq_msgstate_new,
		rq_msgstate_delivering,
		rq_msgstate_delivered,
		rq_msgstate_replied
	} state;
	void (*reply_handler)(struct __rq_message_t *msg);
	void (*fail_handler)(struct __rq_message_t *msg);
	void *arg;
} rq_message_t;

typedef struct {
	char *queue;
	queue_id_t qid;
	char exclusive;
	short int max;
	unsigned char priority;
	
	void (*handler)(rq_message_t *msg, void *arg);
	void (*accepted)(char *queue, queue_id_t qid, void *arg);
	void (*dropped)(char *queue, queue_id_t qid, void *arg);
	
	void *arg;
} rq_queue_t;


typedef struct {
	char *param;
	char *details;
	char *value;
	int count;
} rq_svc_helpoption_t;

#define RQ_MAX_HELPOPTIONS 127

typedef struct {
	char *svcname;
	rq_t *rq;
	short verbose;
	rq_svc_helpoption_t *help_options[RQ_MAX_HELPOPTIONS];
} rq_service_t;



void rq_set_maxconns(int maxconns);
int  rq_new_socket(struct addrinfo *ai);
void rq_daemon(const char *username, const char *pidfile, const int noclose);

void rq_init(rq_t *rq);
void rq_shutdown(rq_t *rq);
void rq_cleanup(rq_t *rq);
void rq_setevbase(rq_t *rq, struct event_base *base);

// add a controller to the list, and it should attempt to connect to one of
// them.   Callback functions can be provided so that actions can be performed
// when there are no connections to the controllers.
void rq_addcontroller(
	rq_t *rq,
	char *host,
	void (*connect_handler)(rq_service_t *service, void *arg),
	void (*dropped_handler)(rq_service_t *service, void *arg),
	void *arg);

// start consuming a queue.
void rq_consume(
	rq_t *rq,
	char *queue,
	int max,
	int priority,
	int exclusive,
	void (*handler)(rq_message_t *msg, void *arg),
	void (*accepted)(char *queue, queue_id_t qid, void *arg),
	void (*dropped)(char *queue, queue_id_t qid, void *arg),
	void *arg);


rq_message_t * rq_msg_new(rq_t *rq, rq_conn_t *conn);
void rq_msg_clear(rq_message_t *msg);
void rq_msg_setqueue(rq_message_t *msg, char *queue);
void rq_msg_setbroadcast(rq_message_t *msg);
void rq_msg_setnoreply(rq_message_t *msg);


// macros to add RISP commands to the message buffer.   This is better than
// addng commands to a seperate buffer and then copying it to the message
// buffer.
#define rq_msg_addcmd(m,c)              (addCmd((m)->data,(c)))
#define rq_msg_addcmd_shortint(m,c,v)   (addCmdShortInt((m)->data,(c), (v)))
#define rq_msg_addcmd_int(m,c,v)        (addCmdInt((m)->data,(c),(v)))
#define rq_msg_addcmd_largeint(m,c,v)   (addCmdLargeInt((m)->data,(c), (v)))
#define rq_msg_addcmd_shortstr(m,c,l,s) (addCmdShortStr((m)->data,(c), (l), (s)))
#define rq_msg_addcmd_str(m,c,l,s)      (addCmdStr((m)->data,(c), (l), (s)))
#define rq_msg_addcmd_largestr(m,c,l,s) (addCmdLargeStr((m)->data,(c), (l), (s)))



// For situations where RISP messaging is not used, raw data can be supplied
// instead.  The contents will be copied into a buffer.
void rq_msg_setdata(rq_message_t *msg, int length, char *data);


void rq_send(
	rq_message_t *msg,
	void (*reply_handler)(rq_message_t *reply),
	void (*fail_handler)(rq_message_t *msg),
	void *arg);

void rq_resend(rq_message_t *msg);
void rq_reply(rq_message_t *msg, int length, char *data);


/*---------------------------------------------------------------------------*/
// Service control.  To make writing services for the RQ environment easier,
// we handle everything we can.



// Initialise a new rq_service_t object and return a pointer to it.  It should
// be cleaned up with rq_service_cleanup()
rq_service_t *rq_svc_new(void);

// Cleanup the service object and free its resources and itself.
void rq_svc_cleanup(rq_service_t *service);

void rq_svc_setname(rq_service_t *service, const char *name);
char * rq_svc_getoption(rq_service_t *service, char tag);
void   rq_svc_setoption(rq_service_t *service, char tag, const char *param, const char *details);

void rq_svc_process_args(rq_service_t *service, int argc, char **argv);

void rq_svc_shutdown(rq_service_t *service);

void rq_svc_initdaemon(rq_service_t *service);
void rq_svc_setevbase(rq_service_t *service, struct event_base *evbase);

int rq_svc_connect(
	rq_service_t *service,
	void (*connect_handler)(rq_service_t *service, void *arg),
	void (*dropped_handler)(rq_service_t *service, void *arg),
	void *arg);


// This value is the number of elements we pre-create for the message list.
// When the system is running, it should always assume that there is at least
// something in the list.   The list will grow as the need arises, so this
// number doesn't really matter much, except to maybe tune it a little better.
#define DEFAULT_MSG_ARRAY 10


#endif
