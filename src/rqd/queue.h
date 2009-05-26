#ifndef __QUEUE_H
#define __QUEUE_H


#include <linklist.h>

//---------------------------------------------------------------------
#include "node.h"
#include "message.h"
#include "system_data.h"




#define QUEUE_LOW_PRIORITY	10

#define QUEUE_FLAG_EXCLUSIVE 0x0001


typedef struct {
	char *name;
	queue_id_t qid;
	unsigned int flags;
	
	// a list of messages that need to be delivered.  New messages will be added
	// to the tail.   Messages to be delivered will be at the head.   When
	// messages are sent to a client, they are removed from the pending list, and
	// put in msgproc list;
	list_t msg_pending, msg_proc;

	// a list of nodes that have subscribed to this queue.  The busy list will
	// include all the nodes that have reached their MAX message allocations.
	// Nodes that can receive messages will be in ready.  When a message has
	// been replied, if the head node is processing messages, and if the current
	// node has more capacity, then it will be moved to the head.
	list_t nodes_busy, nodes_ready;

	// when a queue is being consumed exclusively, this list contains the nodes
	// that are waiting.  When an exclusive consumer has disconnected, the next
	// entry in this list will 
	list_t nodes_waiting;

	system_data_t *sysdata;
} queue_t;

void queue_set_id(list_t *queues, const char *name, queue_id_t id);

queue_t * queue_get_id(list_t *queues, queue_id_t qid);
queue_t * queue_get_name(list_t *queues, const char *qname);
void      queue_cancel_node(node_t *node);

queue_t * queue_create(system_data_t *sysdata, char *qname);
void      queue_init(queue_t *queue);
void      queue_free(queue_t *queue);
void      queue_addmsg(queue_t *queue, message_t *msg);
int       queue_add_node(queue_t *queue, node_t *node, int max, int priority, unsigned int flags);
void      queue_shutdown(queue_t *queue);


void      queue_deliver(queue_t *queue);
// void      queue_notify(queue_t *queue, void *server);
void      queue_msg_done(queue_t *queue, node_t *node);

void      queue_dump(queue_t *queue, expbuf_t *buf);

#endif

