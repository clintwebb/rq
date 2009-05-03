

#include "actions.h"
#include "queue.h"
#include "send.h"
#include "server.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>



// structure to keep track of the node that is consuming the queue
typedef struct {
	node_t *node;
	short int priority;
	int max;				// maximum number of messages this node will process at a time.
	int waiting;
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


// This could be improved a little... when a queue is found, it is removed from the spot in the list, and placed at the top of the list.  That means that queues with higher activity are found first.
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
	assert(queue);
	assert(msg);
	assert(queue->sysdata);

	assert(msg->queue == NULL);
	msg->queue = queue;

	// add the message to the queue
	ll_push_tail(&queue->msg_pending, msg);

	// if the only message in the list is the one we just added, then we will create an action to process it.
	if (ll_count(&queue->msg_pending) == 1) {
		queue_deliver(queue);
	}
}



// this function will look at the flags for the queue, and if we need to 
void queue_notify(queue_t *queue, void *pserver)
{
	server_t *server = (server_t *) pserver;
	
	node_t *node;
	void *next;
	
	assert(queue->qid > 0);
	assert(queue->name != NULL);

	// now that we have our server object, we can go thru the list of nodes.  If
	// any of them are controllers, then we need to send a consume request.
	next = ll_start(&server->nodelist);
	node = ll_next(&server->nodelist, &next);
	while (node) {
		
		if (BIT_TEST(node->flags, FLAG_NODE_CONTROLLER)) {
			assert(0);

			sendConsume(node, queue->name, 1, QUEUE_LOW_PRIORITY);
		}
		
		node = ll_next(&server->nodelist, &next);
	}
}


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
	while ((queue = ll_next(node->sysdata->queues, &next_queue))) {
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
		
		// the node has being removed from the queue, if there are no more nodes, and there are no messages in the queue, then the queue needs to be deleted.
		if (ll_count(&queue->nodes_busy) <= 0 && ll_count(&queue->nodes_ready) == 0) {
			if (ll_count(&queue->nodes_waiting) > 0) {
				// we dont have any active nodes anymore, but we have a waiting node... we need to activate the waiting node.
				assert(0);
			}
			else {
				// this queue has no nodes at all, not even any waiting to start up exclusively.
				if (ll_count(&queue->msg_pending) <= 0 && ll_count(&queue->msg_proc) <= 0) {
					action_t *action;
					assert(node->sysdata->actpool);
					action = action_pool_new(node->sysdata->actpool);
					action_set(action, 0, ah_queue_shutdown, queue);
				}
				else {
					// but it does have messages waiting to be deliverd... so we wont delete the queue just yet.
				}
			}
		}
	}
}



queue_t * queue_create(system_data_t *sysdata, char *qname)
{ 
	queue_t *q;

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

	// notify other nodes (controllers) that we are consuming a queue.
	assert(sysdata->server);
	queue_notify(q, sysdata->server);

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


//-----------------------------------------------------------------------------
// This function should only be called from an action.  It will look at the
// messages pending in the queue and will process the first one in the list.
// If it is unable to process the message (because there are no available
// consumers), then it will not send the message.  When consumers become
// available, this action would be fired again anyway.  if there are any more
// messages in the queue, it will fire the action again.
void queue_deliver(queue_t *queue)
{
	message_t *msg, *tmp;
	system_data_t *sysdata;
	node_queue_t *nq;
	void *next;

	assert(queue);
	assert(queue->sysdata);

	sysdata = queue->sysdata;
	
	// when this function is fired, there should be at least one message in the
	// queue to process.
	assert(ll_count(&queue->msg_pending) > 0);

// 	printf("queue_deliver. q:%d, pending:%d\n", queue->qid, ll_count(&queue->msg_pending));

	msg = ll_pop_head(&queue->msg_pending);
	if (msg) {
	
// 		printf("queue_deliver. q:%d, pending:%d, msgid:%d\n", queue->qid, ll_count(&queue->msg_pending), msg->id);

		// check the message to see if it is broadcast.
		if (BIT_TEST(msg->flags, FLAG_MSG_BROADCAST)) {
	
			if (sysdata->verbose > 1) printf("queue_deliver: delivering broadcast message\n");
	
			// This is a broadcast message.
			assert(BIT_TEST(msg->flags, FLAG_MSG_NOREPLY));
			assert(msg->source_node == NULL);
			assert(msg->target_node == NULL);
			assert(msg->queue != NULL);
	
			// if we have at least one node that is not busy, then we will send the message.
			// even if a node is busy, we will send the broadcast to it.
			if (ll_count(&queue->nodes_ready) > 0) {
				next = ll_start(&queue->nodes_ready);
				nq = ll_next(&queue->nodes_ready, &next);
				while (nq) {
					assert(nq->node);
					if (sysdata->verbose > 1) printf("queue_deliver: sending broadcast msg to node:%d\n", nq->node->handle);
					assert(msg->id == 0);
					assert(msg->target_node == NULL);
					sendMessage(nq->node, msg);
					nq = ll_next(&queue->nodes_ready, &next);
				}
				
				// since it is broadcast, we are not expecting a reply, so we can delete
				// the message (it should already be removed from the node).
				message_delete(msg);
			}
			else {
				// we dont have any available nodes so we wont send the broadcast yet.
				// We use this for throttling so we dont give our nodes a deluge if they
				// are busy.   Of course, if there are a number of broadcast messages
				// next in the queue, it will deliver them all pretty quickly until some
				// normal requests come in.
			}
		}
		else {
			// This is a request.  Even requests with NOREPLY work the same at this point, because we keep the message in memory until DELIVERED is received.
	
			// if we have a node that is ready, use it.
			nq = ll_pop_head(&queue->nodes_ready);
			if (nq) {
				assert(nq->max == 0 || (nq->waiting <= nq->max));
				
				// add the node pointer to the message.
				msg->target_node = nq->node;

				// add the message to the target node's list.
				tmp = ll_get_head(&nq->node->out_msg);
				if (tmp) { msg->id = tmp->id + 1; }
				else { msg->id = 1; }
				ll_push_head(&nq->node->out_msg, msg);

				// send the message to the node.
				if (sysdata->verbose > 1) printf("queue_deliver: sending msg to node:%d\n", nq->node->handle);
				sendMessage(nq->node, msg);
				// increment the 'waiting' count for the nq.
				nq->waiting ++;
				assert(nq->waiting > 0 && (nq->max == 0 || nq->waiting <= nq->max));
		
				// if the node has reached the max number of consumed messages, then it
				// will be put in the busy list.  Otherwise it will be put in the tail
				// of the ready list where it can receive more.
				if (nq->max > 0 && nq->waiting >= nq->max) {
					ll_push_tail(&queue->nodes_busy, nq);
				}
				else {
					ll_push_tail(&queue->nodes_ready, nq);
				}
					
				// add the message to the msgproc list.
				ll_push_head(&queue->msg_proc, msg);
			}
			else {
				if (sysdata->verbose > 1)
					printf("queue_deliver. q:%d, no nodes ready to consume.\n", queue->qid);

				// we couldn't process the message, so put it back.
				ll_push_head(&queue->msg_pending, msg);
			}
		}
	}
	else {
		if (sysdata->verbose > 1)
			printf("queue_deliver: queue:%d, no messages waiting.\n", queue->qid);
	}
}


// this function is called when a message has been delivered (in NOREPLY
// mode), or a reply sent.  It is used to remove a node from the busy list if
// it is marked as busy.
void queue_msg_done(queue_t *queue, node_t *node)
{
	node_queue_t *nq;
	void *next;
	
	assert(queue);
	assert(node);


	next = ll_start(&queue->nodes_busy);
	nq = ll_next(&queue->nodes_busy, &next);
	while (nq) {
		assert(nq->node);
		if (nq->node == node) {
			// found it... in the busy list... so remove it from the busy list and add it back to the ready list.

			assert(nq->waiting > 0);
			nq->waiting --;
			assert(nq->waiting >= 0);
			
			ll_remove(&queue->nodes_busy, nq, next);
			ll_push_tail(&queue->nodes_ready, nq);
			nq = NULL;
		}
		else {
			nq = ll_next(&queue->nodes_busy, &next);
		}
	}
}


