

#include "actions.h"
#include "queue.h"
#include "send.h"
#include "server.h"

#include <assert.h>
#include <evactions.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>



// structure to keep track of the node that is consuming the queue
typedef struct {
	node_t *node;
	short int max;				// maximum number of messages this node will process at a time.
	short int priority;
	short int waiting;
} node_queue_t;



//-----------------------------------------------------------------------------
// Initialise a queue object.
void queue_init(queue_t *queue)
{
	assert(queue != NULL);
	queue->name = NULL;
	queue->qid = 0;
 	queue->flags = 0;

	ll_init(&queue->msg_pending);
	ll_init(&queue->msg_proc);
	ll_init(&queue->nodes_busy);
	ll_init(&queue->nodes_ready);
	ll_init(&queue->nodes_waiting);
	
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

	ll_free(&queue->msg_pending);
	ll_free(&queue->msg_proc);
	ll_free(&queue->nodes_busy);
	ll_free(&queue->nodes_ready);
	ll_free(&queue->nodes_waiting);
}




queue_t * queue_get_id(list_t *queues, queue_id_t qid)
{
	queue_t *q = NULL;
	queue_t *tmp;
	void *next;

	assert(queues);
	assert(qid > 0);

	next = ll_start(queues);
	tmp = ll_next(queues, &next);
	while (tmp) {
		assert(tmp->name != NULL);
		assert(tmp->qid > 0);

		if (tmp->qid == qid) {
			q = tmp;
			tmp = NULL;
		}
		else {
			tmp = ll_next(queues, &next);
		}
	}
	
	return(q);
}


queue_t * queue_get_name(list_t *queues, const char *qname)
{
	queue_t *q = NULL;
	queue_t *tmp;
	void *next;

	assert(queues);
	assert(qname);

	next = ll_start(queues);
	tmp = ll_next(queues, &next);
	while (tmp) {
		assert(tmp->name != NULL);
		assert(tmp->qid > 0);

		if (strcmp(tmp->name, qname) == 0) {
			q = tmp;
			tmp = NULL;
		}
		else {
			tmp = ll_next(queues, &next);
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
	node_t *node;
	void *next;
	
	assert(queue != NULL);
	assert(msg != NULL);

	// check the message to see if it is broadcast.
	if (BIT_TEST(msg->flags, FLAG_MSG_BROADCAST)) {
		// and make sure that we have a timeout.
		// for each node in the list, we need to examine to assign the message to it.

		// send the message to all the nodes.
 		assert(0);

		// make sure that message doesn't include pointer to source node.
 		assert(0);

		// make sure message does not have target pointer.
 		assert(0);

		// make sure message does not have queue pointer.
 		assert(0);

		// create action to delete the message structure.
 		assert(0);
		
		
	}
	else {
		// not broadcast.

		if (BIT_TEST(msg->flags, FLAG_MSG_NOREPLY)) {

			//	make sure that the message doesn't include pointer to source node.
	 		assert(0);

			// get details of ready node.
 			assert(0);

		}
		
		
	}
	// and make sure that we have a timeout.

	// check to see if the message is a request.
	// if it is a broadcast request, then we need to create 



	// ** in trying to conserve memory by using the same structure from the originating node, the receiving nodes, and also in the queues, then we 



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
	void *next_queue, *next_node;
	
	assert(node);
	assert(node->sysdata);
	assert(node->sysdata->queues);

	next_queue = ll_start(node->sysdata->queues);
	queue = ll_next(node->sysdata->queues, &next_queue);
	while (queue) {
		assert(queue->qid > 0);
		assert(queue->name);

		found = 0;
		
		// need to check this node in the busy
		next_node = ll_start(&queue->nodes_busy);
		nq = ll_next(&queue->nodes_busy, &next_node);
		while (nq && found == 0) {

			assert(nq->node);
			if (nq->node == node) {
				if (node->sysdata->verbose)
					printf("queue %d:'%s' removing node:%d from busy list\n", queue->qid, queue->name, node->handle);

				ll_remove(&queue->nodes_busy, nq, next_node);

				nq->node->refcount --;
				assert(nq->node->refcount >= 0);

				free(nq);
				nq = NULL;
				found++;
			}
			else {
				nq = ll_next(&queue->nodes_busy, &next_node);
			}
		}
		
		// ready
		next_node = ll_start(&queue->nodes_ready);
		nq = ll_next(&queue->nodes_ready, &next_node);
		while (nq && found == 0) {

			assert(nq->node);
			if (nq->node == node) {
				if (node->sysdata->verbose)
					printf("queue %d:'%s' removing node:%d from ready list\n", queue->qid, queue->name, node->handle);

				ll_remove(&queue->nodes_ready, nq, next_node);
				
				nq->node->refcount --;
				assert(nq->node->refcount >= 0);

				free(nq);
				nq = NULL;
				found++;
			}
			else {
				nq = ll_next(&queue->nodes_ready, &next_node);
			}
		}

		if (found != 0) {
			// The node was found already.  If the queue was in exclusive mode, need
			// to update the lists and activate the one that is waiting.

			if (BIT_TEST(queue->flags, QUEUE_FLAG_EXCLUSIVE) ||
				(ll_count(&queue->nodes_ready) == 0 && ll_count(&queue->nodes_busy) == 0 && ll_count(&queue->nodes_waiting) > 0)) {
				// queue was already in exclusive mode, and we have removed a node, so that means our main lists should be empty.
				assert(ll_count(&queue->nodes_ready) == 0);
				assert(ll_count(&queue->nodes_busy) == 0);

				// if we have any nodes waiting, we will use it.
				nq = ll_pop_tail(&queue->nodes_waiting);
				if (nq) {
					assert(nq->node);
					assert(queue->name);
					assert(queue->qid > 0);

					// add it to the ready list.
					ll_push_head(&queue->nodes_ready, nq);

					// tell the node that we are consuming the queue now.
					sendConsumeReply(nq->node, queue->name, queue->qid);

					if (ll_count(&queue->msg_pending) > 0) {
						// there are messages that need to be delivered.  Not sure yet, whether to assign an action to do this, or what...
						assert(0);
					}

					if (node->sysdata->verbose)
						printf("Promoting waiting node:%d to EXCLUSIVE queue '%s'.\n", nq->node->handle, queue->name);
				}
			}
		}
		else {
		
			// and waiting list.
			next_node = ll_start(&queue->nodes_waiting);
			nq = ll_next(&queue->nodes_waiting, &next_node);
			while (nq && found == 0) {
	
				assert(nq->node);
				if (nq->node == node) {
					if (node->sysdata->verbose)
						printf("queue %d:'%s' removing node:%d from waitinglist\n", queue->qid, queue->name, node->handle);
	
					ll_remove(&queue->nodes_waiting, nq, next_node);
					
					assert(nq->node->refcount > 0);
					nq->node->refcount --;
	
					free(nq);
					nq = NULL;
					found++;
				}
				else {
					nq = ll_next(&queue->nodes_waiting, &next_node);
				}
			}
		}
		
		queue = ll_next(node->sysdata->queues, &next_queue);
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

	q->sysdata = sysdata;

	// add the queue to the queue list.
	ll_push_head(sysdata->queues, q);

	// set an action so that we can notify other nodes (controllers) that we are consuming a queue.
	assert(sysdata->actpool);
	action = action_pool_new(sysdata->actpool);
	action_set(action, 0, ah_queue_notify, q);
	action = NULL;

	return (q);
}


//-----------------------------------------------------------------------------
// Add the node to the queue as a consumer.  If the node is requesting an
// exclusive consume, then we will accept it if we aren't already processing
// any others.  Otherwise it will go on the waiting list.
int queue_add_node(queue_t *queue, node_t *node, int max, int priority, unsigned int flags)
{
	node_queue_t *nq;
	
	assert(queue);
	assert(node);
	assert(max >= 0);
	assert(priority >= 0);
		
	// We need to create a node_queue_t object to hold the node details in it.
	nq = (node_queue_t *) malloc(sizeof(node_queue_t));
	nq->node = node;
	nq->max = max;
	nq->priority = priority;
	nq->waiting = 0;


	// check to see if the current queue settings are for it to be
	// exclusive.   If so, then we will need to add this node to the waiting
	// list.
	if (BIT_TEST(queue->flags, QUEUE_FLAG_EXCLUSIVE) || ll_count(&queue->nodes_ready) > 0 || ll_count(&queue->nodes_busy) > 0) {
		// The queue is already in exclusive mode (or already has members that
		// are not exlusive).  So this node would need to be added to the
		// waiting list.

		ll_push_head(&queue->nodes_waiting, nq);

		if (node->sysdata->verbose > 1)
			printf("processConsume - Defered, queue already consumed exclusively.\n");

		return (0);
	}
	else {
			
		// if the consume request was for an EXCLUSIVE queue (and since we got
		// this far it means that no other nodes are consuming this queue yet),
		// then we mark it as exclusive.
		if (BIT_TEST(flags, QUEUE_FLAG_EXCLUSIVE)) {
			assert(ll_count(&queue->nodes_ready) == 0);
			assert(ll_count(&queue->nodes_busy) == 0);
			BIT_SET(queue->flags, QUEUE_FLAG_EXCLUSIVE);
			if (node->sysdata->verbose)
				printf("Consuming Queue '%s' in EXCLUSIVE mode.\n", expbuf_string(&node->data.queue));
		}

		// add the node to the appropriate list.
		ll_push_head(&queue->nodes_ready, nq);

		if (node->sysdata->verbose)
			printf("Consuming queue: qid=%d\n", queue->qid);

		return(1);	
	}
}

