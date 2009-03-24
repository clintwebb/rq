

#include "actions.h"
#include "queue.h"
#include "server.h"

#include <assert.h>
#include <evactions.h>
#include <stdlib.h>
#include <string.h>




void queue_list_init(queue_list_t *list)
{
	assert(list != NULL);
	list->list = NULL;
	list->queues = 0;
	list->next_qid = 1;
}


void queue_list_free(queue_list_t *list)
{
	assert(list != NULL);
	assert((list->list == NULL && list->queues == 0) || (list->list != NULL && list->queues >= 0));
	while(list->queues > 0) {
		list->queues --;

		// the queue should already have been removed from the list.
		assert(list->list[list->queues] == NULL);

		
// 		if (list->list[list->queues] != NULL) {
// 			queue_free(list->list[list->queues]);
// 			free(list->list[list->queues]);
// 			list->list[list->queues] = NULL;
// 		}
	}
	if (list->list != NULL) {
		free(list->list);
		list->list = NULL;
	}
	assert(list->next_qid > 0);
	assert(list->list == NULL);
	assert(list->queues == 0);
}

//-----------------------------------------------------------------------------
// Initialise a queue object.
void queue_init(queue_t *queue)
{
	assert(queue != NULL);
	queue->name = NULL;
	queue->qid = 0;
 	queue->flags = 0;

	queue->nodelist = NULL;
	queue->nodes = 0;

	queue->msghead = NULL;
	queue->msgtail = NULL;

	queue->waitinglist = NULL;
	queue->waiting = 0;
}


//-----------------------------------------------------------------------------
// free the resources in a queue object (but not the object itself.)
void queue_free(queue_t *queue)
{
	int i;
	assert(queue != NULL);

	if (queue->name != NULL) {
		free(queue->name);
		queue->name = NULL;
	}

	if (queue->nodelist != NULL) {
		assert(queue->nodes > 0);
		for (i=0; i<queue->nodes; i++) {
			assert(queue->nodelist[i] == NULL);
		}
		free(queue->nodelist);
		queue->nodelist = NULL;
		queue->nodes = 0;
	}
	assert(queue->nodes == 0);
	
	assert(queue->msghead == NULL);
	assert(queue->msgtail == NULL);
}


//-----------------------------------------------------------------------------
// When a node indicates that it wants to consume a queue, this function is
// called and the node is added to the queue list.   If this is the first time
// this queue is being consumed, then we need to mark it so that the timer can
// find it, and send the appropriate consume requests to the connected
// controller nodes.
int queue_consume(queue_list_t *queuelist, node_t *node, short exclusive)
{
	int i;
	queue_id_t qid, next;
	queue_t *q=NULL;
	node_queue_t *nq;
	queue_msg_t *qmsg;
	action_t *action;
	short exclusive;
	
	assert(queuelist);
	assert(node);

	assert(node->data.flags & DATA_FLAG_CONSUME);
	assert(node->data.mask & DATA_MASK_QUEUE);
	assert(node->data.queue.length > 0);

	// check to see if we already have that queue in our list.
	// if we dont, create the queue entry.
	// add this node to the queue consumer list.
	// add the queue pointer to the list in the node.
	qid = -1;
	next = -1;
	if (queuelist->queues > 0) {
		assert(q == NULL);
		assert(queuelist->list != NULL);
		for (i=0; i<queuelist->queues && qid < 0; i++) {
			q = queuelist->list[i];
			if (q != NULL) {
				assert(q->name != NULL);
				assert(q->qid == (i+1));
				if (strcmp(q->name, expbuf_string(&node->data.queue)) == 0) {
					printf("Queue '%s' found: %d\n", q->name, q->qid);
					qid = q->qid;
				}
			}
			else if (next < 0) {
				next = i;
			}
		}
		assert((qid < 0 && q == NULL) || (qid > 0 && q != NULL));
	}
	else {
		assert(q == NULL);
		assert(qid < 0);
		printf("queue list empty.\n");
	}
	
	if (qid < 0) {
		// we didn't find the queue...
		printf("Didn't find queue '%s', creating new entry. [next:%d]\n", expbuf_string(&node->data.queue), next);
		if (next < 0) {
			queuelist->list = (queue_t **) realloc(queuelist->list, sizeof(queue_t *)*(queuelist->queues+1));
			assert(queuelist->list != NULL);
			next = queuelist->queues;
			queuelist->queues ++;
		}

		assert(queuelist->queues > 0);
		assert(next >= 0);
		queuelist->list[next] = (queue_t *) malloc(sizeof(queue_t));
		q = queuelist->list[next];
		assert(q != NULL);
		queue_init(q);
		qid = next+1;

		// ok, we should now have a 'q' pointer.
		assert(q != NULL);
		assert(q->name == NULL);
		assert(node->data.queue.length > 0);
		q->name = (char *) malloc(node->data.queue.length + 1);
		assert(q->name != NULL);
		strncpy(q->name, expbuf_string(&node->data.queue), node->data.queue.length);
		assert(qid > 0);
		q->qid = qid;

		// set an action so that we can notify other nodes (controllers) that we are consuming a queue.
		assert(node->sysdata->actpool);
		action = action_pool_new(node->sysdata->actpool);
		assert(action);
		action_set(action, 0, ah_queue_notify, q);
		action = NULL;
	}

	assert(q != NULL);
	assert(q->qid == qid);

	exclusive = 0;
	if (BIT_TEST(node->data.flags, DATA_FLAG_EXCLUSIVE))
		exclusive = 1;



	
	// now we need to add this new node to the list for this queue, along with any other info we need to keep.
	assert(node);
	assert((q->nodelist == NULL && q->nodes == 0) || (q->nodelist != NULL));
	nq = NULL;
	for(i=0; i<q->nodes; i++) {
		if (q->nodelist[i] == NULL) {
			q->nodelist[i] = (node_queue_t *) malloc(sizeof(node_queue_t));
			assert(q->nodelist[i] != NULL);
			nq = q->nodelist[i];
			break;
		}
		else {
			assert(q->nodelist[i]->node != node);
		}
	}
	if (i >= q->nodes) {
		// we didn't break out of the list early, means we didn't find an empty slot in the list.
		q->nodelist = (node_queue_t **) realloc(q->nodelist, sizeof(node_queue_t *)*(q->nodes + 1));
		assert(q->nodelist != NULL);
			
		q->nodelist[q->nodes] = (node_queue_t *) malloc(sizeof(node_queue_t));
		assert(q->nodelist[q->nodes] != NULL);
		nq = q->nodelist[q->nodes];
		q->nodes++;
	}

	assert(nq != NULL);
	nq->node = node;

	if (node->data.mask & DATA_MASK_MAX) nq->max = node->data.max;
	else nq->max = 0;
	
	if (node->data.mask & DATA_MASK_PRIORITY) nq->priority = node->data.priority;
	else nq->priority = 0;

	nq->waiting = 0;

	// need to check the queue to see if there are messages pending.  If there are, then send some to this node.
	if (q->msghead != NULL) {

		// not done.
		assert(0);

	}


	if (node->sysdata->verbose)
		printf("Consuming queue: qid=%d\n", qid);

	return(qid);
}


queue_t * queue_get(queue_list_t *ql, queue_id_t qid)
{
	queue_t *q = NULL;
	queue_t *tmp;
	int i;

	assert(ql != NULL);
	assert(qid > 0);

	assert((ql->queues == 0 && ql->list == NULL) || (ql->queues > 0 && ql->list != NULL));
	for (i=0; i<ql->queues; i++) {
		tmp = ql->list[i];
		if (tmp != NULL) {
			assert(tmp->name != NULL);
			if (tmp->qid == qid) {
				q = tmp;
			}
		}
	}

	return(q);
}


//-----------------------------------------------------------------------------
// When creating a queue, this is done the first time a node has indicated that
// it is consuming the queue.  A search thru the queuelist didn't find it, so
// now we are creating it.
queue_t * queue_create(queue_list_t *ql, char *name)
{
	queue_t *q = NULL;
	queue_id_t qid;
	int slot, i;

	assert(ql != NULL);
	assert(name != NULL);
	assert(strlen(name) < 256);

	// get the next available qid;
	assert(ql->next_qid > 0);
	qid = ql->next_qid;
	ql->next_qid ++;

	// create the new queue object.
	q = (queue_t *) malloc(sizeof(queue_t));
	assert(q != NULL);

	// initialise the queue object.
	queue_init(q);
	q->qid = qid;

	// go thru the queue list and find and empty spot.
	slot = ql->queues;
	assert((ql->queues == 0 && ql->list == NULL) || (ql->queues > 0 && ql->list != NULL));
	for (i=0; i<ql->queues; i++) {
		if (ql->list[i] == NULL) {
			slot = i;
			break;
		}
	}

	// add the queue object to the list.
	if (slot == ql->queues) {
		ql->list = (queue_t **) realloc(ql->list, sizeof(queue_t *) * (ql->queues+1));
		ql->list[ql->queues] = q;
		ql->queues ++;
	}
	else {
		assert(slot < ql->queues);
		assert(ql->list[slot] == NULL);
		ql->list[slot] = q;
	}

	return(q);
}

//-----------------------------------------------------------------------------
// With this function, we have a message object, that we need to apply to the
// queue.  This is a core function that will perform as much as possible during
// this operation.  If the msg is a broadcast message, then it will need to
// attempt to output the message to all nodes that are members of the queue.
// If it is a request then we need to go through our node list and calculate
// the best node to deliver it to, and deliver it.
void queue_addmsg(queue_t *queue, message_t *msg)
{
	assert(queue != NULL);
	assert(msg != NULL);

	// check the message to see if it is broadcast.
	// and make sure that we have a timeout.

	// check to see if the message is a request.
	// if it is a broadcast request, then we need to create 

// ** Do we need to create a message object for every node that we send the message to?  That could be messy if we have a lot of consumers... or can the nodes share the structure?

	// for each node in the list, we need to examine to assign the message to it.

	// ** in trying to conserve memory by using the same structure from the originating node, the receiving nodes, and also in the queues, then we 


	assert(0);
}


// This function is used to initialise the message structure.
void queue_msg_init(queue_msg_t *msg)
{
	assert(msg != NULL);
	assert(0);
}

void queue_msg_free(queue_msg_t *msg)
{
	assert(msg);
	assert(0);
}


// this function will look at the flags for the queue, and if we need to 
void queue_notify(queue_t *queue, void *pserver)
{
	server_t *server = (server_t *) pserver;
	
	assert(queue);
	assert(server);
}


// returns 0 if the queue does not need to be cleaned up, will return a !0 if it does.
// int queue_cleanup_check(queue_t *queue)
// {
// 	assert(queue);
// 
// 	if (BIT_TEST(queue->flags, FLAG_QUEUE_DELETE)) {
// 		assert(queue->flags == FLAG_QUEUE_DELETE);
// 		return(!0);
// 	}
// 	else {
// 		return(0);
// 	}
// }


//-----------------------------------------------------------------------------
// go thru the list of queues and compare them against this name.  If there is
// a match, return the id.
int queue_id(queue_list_t *ql, char *name)
{
	int id = 0;
	int i;

	assert(ql);
	assert(name);
	assert(strlen(name) < 256);

	for (i=0; i < ql->queues && id == 0; i++) {
		if (ql->list[i]) {
			assert(ql->list[i]->qid > 0);
			assert(ql->list[i]->name != NULL);

			if (strcmp(name, ql->list[i]->name) == 0) {
				id = ql->list[i]->qid;
			}
		}
	}
	
	return(id);
}


//-----------------------------------------------------------------------------
// When a node needs to cancel all the queues that it is consuming, then we go
// thru the queue list, and remove the node from any of them.  At the point of
// invocation, the node does not know what queues it is consuming.
void queue_cancel_node(queue_list_t *ql, node_t *node)
{
	int i, j;
	
	assert(ql);
	assert(node);
	assert(node->sysdata);

	assert((ql->queues == 0 && ql->list == NULL) || (ql->queues > 0 && ql->list));
	for (i=0; i < ql->queues; i++) {
		if (ql->list[i]) {
			assert(ql->list[i]->qid > 0);
			assert(ql->list[i]->name != NULL);


			// we have a queue, now we need to go thru the list of nodes, and remove them when we find 'node'.
			for (j=0; j < ql->list[i]->nodes; j++) {
				assert(ql->list[i]->nodelist);
				if (ql->list[i]->nodelist[j]) {
					if (ql->list[i]->nodelist[j]->node == node) {

						if (node->sysdata->verbose)
							printf("queue %d:'%s' removing node:%d from pos:%d\n",
								ql->list[i]->qid,
								ql->list[i]->name,
								node->handle,
								j);
						
						free(ql->list[i]->nodelist[j]);
						ql->list[i]->nodelist[j] = NULL;
					}
				}
			}
		}
	}
}


