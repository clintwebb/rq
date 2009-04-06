

#include "actions.h"
#include "queue.h"
#include "send.h"
#include "server.h"

#include <assert.h>
#include <evactions.h>
#include <stdlib.h>
#include <string.h>



//-----------------------------------------------------------------------------
// Initialise a queue object.
void queue_init(queue_t *queue)
{
	assert(queue != NULL);
	queue->name = NULL;
	queue->qid = 0;
 	queue->flags = 0;

	queue->msghead = NULL;
	queue->msgtail = NULL;
	queue->msgproc = NULL;

	queue->busy = NULL;
	queue->ready_head = NULL;
	queue->ready_tail = NULL;
	
	queue->waitinglist = NULL;
	queue->sysdata = NULL;
}


//-----------------------------------------------------------------------------
// free the resources in a queue object (but not the object itself.)
void queue_free(queue_t *queue)
{
	assert(queue != NULL);

	if (queue->name != NULL) {
		free(queue->name);
		queue->name = NULL;
	}

	assert(queue->msghead == NULL);
	assert(queue->msgtail == NULL);
	assert(queue->msgproc == NULL);

	assert(queue->busy == NULL);
	assert(queue->ready_head == NULL);
	assert(queue->ready_tail == NULL);

	assert(queue->waitinglist == NULL);
}




queue_t * queue_get_id(queue_t *head, queue_id_t qid)
{
	queue_t *q = NULL;
	queue_t *tmp;

	assert(head);
	assert(qid > 0);

	tmp = head;
	while (tmp) {
		assert(tmp->name != NULL);
		assert(tmp->qid > 0);

		if (tmp->qid == qid) {
			q = tmp;
			tmp = NULL;
		}
		else {
			tmp = tmp->next;
		}
	}
	
	return(q);
}


queue_t * queue_get_name(queue_t *head, const char *qname)
{
	queue_t *q = NULL;
	queue_t *tmp;

	assert(head);
	assert(qname);

	tmp = head;
	while (tmp) {
		assert(tmp->name != NULL);
		assert(tmp->qid > 0);

		if (strcmp(tmp->name, qname) == 0) {
			q = tmp;
			tmp = NULL;
		}
		else {
			tmp = tmp->next;
		}
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


	// update the refcount for the message.
	msg->refcount ++;

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
// When a node needs to cancel all the queues that it is consuming, then we go
// thru the queue list, and remove the node from any of them.  At the point of
// invocation, the node does not know what queues it is consuming.
void queue_cancel_node(node_t *node)
{
	queue_t *queue;
	node_queue_t *nq;
	int found;
	
	assert(node);
	assert(node->sysdata);

	queue = node->sysdata->queues;
	while (queue) {
		assert(queue->qid > 0);
		assert(queue->name);

		found = 0;
		
		// need to check this node in the busy
		nq = queue->busy;
		while (nq && found == 0) {

			assert(nq->node);
			if (nq->node == node) {
				if (node->sysdata->verbose)
					printf("queue %d:'%s' removing node:%d from busy list\n", queue->qid, queue->name, node->handle);

				if (nq == queue->busy) queue->busy = nq->next;
				if (nq->next) nq->next->prev = nq->prev;
				if (nq->prev) nq->prev->next = nq->next;

				assert(nq->node->refcount > 0);
				nq->node->refcount --;

				free(nq);
				nq = NULL;
				found++;
			}
			else {
				nq = nq->next;
			}
		}
		
		// ready
		nq = queue->ready_head;
		while (nq && found == 0) {

			assert(nq->node);
			if (nq->node == node) {
				if (node->sysdata->verbose)
					printf("queue %d:'%s' removing node:%d from ready list\n", queue->qid, queue->name, node->handle);

				if (nq == queue->ready_head) queue->ready_head = nq->next;
				if (nq == queue->ready_tail) queue->ready_tail = nq->prev;
				if (nq->next) nq->next->prev = nq->prev;
				if (nq->prev) nq->prev->next = nq->next;
				
				assert(nq->node->refcount > 0);
				nq->node->refcount --;

				free(nq);
				nq = NULL;
				found++;
			}
			else {
				nq = nq->next;
			}
		}

		if (found != 0) {
			// The node was found already.  If the queue was in exclusive mode, need
			// to update the lists and activate the one that is waiting.

			if (BIT_TEST(queue->flags, QUEUE_FLAG_EXCLUSIVE) || (queue->ready_head == NULL && queue->busy == NULL && queue->waitinglist != NULL)) {
				// queue was already in exclusive mode, and we have removed a node, so that means our main lists should be empty.
				assert(queue->ready_head == NULL);
				assert(queue->ready_tail == NULL);
				assert(queue->busy == NULL);

				if (queue->waitinglist) {

					nq = queue->waitinglist;
					queue->ready_head = nq;
					queue->ready_tail = nq;

					assert(nq->prev == NULL);
					if (nq->next) nq->next->prev = NULL;
					queue->waitinglist = nq->next;
					nq->next = NULL;

					assert(nq->node);
					assert(queue->name);
					assert(queue->qid > 0);
					sendConsumeReply(nq->node, queue->name, queue->qid);

					if (queue->msghead) {
						// there are messages that need to be delivered.  Not sure yet, whether to assign an action to do this, or what...
						assert(0);
					}

					if (node->sysdata->verbose)
						printf("Promoting waiting node:%d to EXCLUSIVE queue '%s'.\n", nq->node->handle, queue->name);
				}
			}
			
		}
		
		// and waiting list.
		nq = queue->waitinglist;
		while (nq && found == 0) {

			assert(nq->node);
			if (nq->node == node) {
				if (node->sysdata->verbose)
					printf("queue %d:'%s' removing node:%d from waitinglist\n", queue->qid, queue->name, node->handle);

				if (nq == queue->waitinglist) queue->waitinglist = nq->next;
				if (nq->next) nq->next->prev = nq->prev;
				if (nq->prev) nq->prev->next = nq->next;

				assert(nq->node->refcount > 0);
				nq->node->refcount --;

				free(nq);
				nq = NULL;
				found++;
			}
			else {
				nq = nq->next;
			}
		}

		queue = queue->next;
	}
}



queue_t * queue_create(system_data_t *sysdata, char *qname)
{ 
	queue_t *q;
	action_t *action;

	assert(sysdata);
	assert(qname);
	
	// create an initialise a new queue structure.
	q = (queue_t *) malloc(sizeof(queue_t));
	queue_init(q);

	// determine the next queue id.
	if (sysdata->queues)
		q->qid = ((queue_t *)sysdata->queues)->qid + 1;
	else
		q->qid = 1;
	assert(q->qid > 0);

	// ok, we should now have a 'q' pointer, so we should assign some data to it.
	assert(q != NULL);
	assert(q->name == NULL);
	assert(strlen(qname) > 0);
	assert(strlen(qname) < 256);
	q->name =strdup(qname);

	// add the queue to the queue list.
	assert(q->prev == NULL);
	q->next = sysdata->queues;
	if (q->next) {
		assert(q->next->prev == NULL);
		q->next->prev = q;
	}
	sysdata->queues = q;

	q->sysdata = sysdata;

	// set an action so that we can notify other nodes (controllers) that we are consuming a queue.
	assert(sysdata->actpool);
	action = action_pool_new(sysdata->actpool);
	action_set(action, 0, ah_queue_notify, q);
	action = NULL;

	return (q);
}

