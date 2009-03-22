#ifndef __QUEUE_H
#define __QUEUE_H

//---------------------------------------------------------------------
#include "node.h"
#include "message.h"


// #define FLAG_QUEUE_NOTIFY	0x01
// #define FLAG_QUEUE_DELETE	0x02
// #define FLAG_QUEUE_CLOSING 0x03


#define QUEUE_LOW_PRIORITY	10


// structure to keep track of the node that is consuming the queue
typedef struct {
	node_t *node;
	short int max;				// maximum number of messages this node will process at a time.
	short int priority;
	short int waiting;
} nodequeue_t;


typedef struct {
	message_t msg;
} queue_msg_t;


typedef struct {
	char *name;
	int qid;
// 	unsigned short int flags;

	// need a list of messages that need to be delivered
	queue_msg_t **msglist;
	int messages;

	// need a list of nodes that have subscribed to this queue.
	nodequeue_t **nodelist;
	int nodes;
	
} queue_t;


typedef struct {
	queue_t **list;
	int       queues;
	int       next_qid;
} queue_list_t;


void queue_list_init(queue_list_t *list);
void queue_list_free(queue_list_t *list);


void queue_init(queue_t *queue);
void queue_free(queue_t *queue);

int queue_consume(queue_list_t *queuelist, node_t *node);
queue_t * queue_get(queue_list_t *ql, char *name, int qid);
queue_t * queue_create(queue_list_t *ql, char *name);
void queue_addmsg(queue_t *queue, message_t *msg);
void queue_cancel_node(queue_list_t *queuelist, node_t *node);

void queue_msg_init(queue_msg_t *msg);
void queue_msg_free(queue_msg_t *msg);

int queue_id(queue_list_t *ql, char *name);

void queue_notify(queue_t *queue, void *server);
// int queue_cleanup_check(queue_t *queue);

#endif

