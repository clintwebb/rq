#ifndef __QUEUE_H
#define __QUEUE_H

//---------------------------------------------------------------------
#include "node.h"
#include "message.h"


#define QUEUE_LOW_PRIORITY	10

#define QUEUE_FLAG_EXCLUSIVE 0x0001

// structure to keep track of the node that is consuming the queue
typedef struct {
	node_t *node;
	short int max;				// maximum number of messages this node will process at a time.
	short int priority;
	short int waiting;
} node_queue_t;


typedef struct __queue_msg_t {
	message_t msg;
	struct __queue_msg_t *next, *prev;
} queue_msg_t;


typedef int queue_id_t;

typedef struct {
	char *name;
	queue_id_t qid;
	unsigned int flags;
	
	// need a list of messages that need to be delivered
	queue_msg_t *msghead, *msgtail;

	// need a list of nodes that have subscribed to this queue.
	node_queue_t **nodelist;
	int nodes;

	// when a queue is being consumed exclusively, this list contains the nodes
	// that are waiting.  When an exclusive consumer has disconnected, the next
	// entry in this list will 
	node_queue_t *waitinglist;
	int waiting;
} queue_t;


typedef struct {
	queue_t  **list;
	int        queues;
	queue_id_t next_qid;
} queue_list_t;


void queue_list_init(queue_list_t *list);
void queue_list_free(queue_list_t *list);

int queue_consume(queue_list_t *queuelist, node_t *node);
queue_t * queue_get(queue_list_t *ql, queue_id_t qid);
queue_t * queue_create(queue_list_t *ql, char *name);
void queue_cancel_node(queue_list_t *queuelist, node_t *node);

void queue_init(queue_t *queue);
void queue_free(queue_t *queue);
void queue_addmsg(queue_t *queue, message_t *msg);

void queue_msg_init(queue_msg_t *msg);
void queue_msg_free(queue_msg_t *msg);

int queue_id(queue_list_t *ql, char *name);
void queue_notify(queue_t *queue, void *server);

#endif

