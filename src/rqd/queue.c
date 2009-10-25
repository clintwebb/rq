

#include "queue.h"
#include "send.h"
#include "server.h"

#include <assert.h>
#include <evlogging.h>
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
	ll_init(&queue->nodes_consuming);
	
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

	assert(ll_count(&queue->msg_pending) == 0);
	ll_free(&queue->msg_pending);

	assert(ll_count(&queue->msg_proc) == 0);
	ll_free(&queue->msg_proc);

	assert(ll_count(&queue->nodes_busy) == 0);
	ll_free(&queue->nodes_busy);

	assert(ll_count(&queue->nodes_ready) == 0);
	ll_free(&queue->nodes_ready);

	assert(ll_count(&queue->nodes_waiting) == 0);
	ll_free(&queue->nodes_waiting);

	assert(ll_count(&queue->nodes_consuming) == 0);
	ll_free(&queue->nodes_consuming);
}




queue_t * queue_get_id(list_t *queues, queue_id_t qid)
{
	queue_t *q = NULL;
	queue_t *tmp;

	assert(queues);
	assert(qid > 0);

	ll_start(queues);
	tmp = ll_next(queues);
	while (tmp) {
		assert(tmp->name != NULL);
		assert(tmp->qid > 0);

		if (tmp->qid == qid) {
			q = tmp;
			tmp = NULL;
		}
		else {
			tmp = ll_next(queues);
		}
	}
	ll_finish(queues);
	
	return(q);
}


// This could be improved a little... when a queue is found, it is removed from the spot in the list, and placed at the top of the list.  That means that queues with higher activity are found first.
queue_t * queue_get_name(list_t *queues, const char *qname)
{
	queue_t *q = NULL;
	queue_t *tmp;

	assert(queues);
	assert(qname);

	ll_start(queues);
	tmp = ll_next(queues);
	while (tmp) {
		assert(tmp->name);
		assert(tmp->qid > 0);

		if (strcmp(tmp->name, qname) == 0) {
			q = tmp;
			tmp = NULL;
		}
		else {
			tmp = ll_next(queues);
		}
	}
	ll_finish(queues);
	
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

	// if the only message in the list is the one we just added, then we will
	// deliver it now.
	if (ll_count(&queue->msg_pending) == 1) {
		queue_deliver(queue);
	}
}

//-----------------------------------------------------------------------------
// Check to see if a particular node is consuming the queue.  The node itself
// does not know what queues it is consuming, so we must go thru the list that
// the queue has.
// Returns 0 if it is not in any of the lists for the queue.
// Otherwise returns 1 if it is either ready or busy, and -1 if it is in the
// waiting list.
int queue_check_node(queue_t *queue, node_t *node)
{
	int found = 0;
	node_queue_t *nq;
	node_t *tmp;

	assert(queue);
	assert(node);

	ll_start(&queue->nodes_ready);
	nq = ll_next(&queue->nodes_ready);
	while (nq) {
		assert(nq->node);
		if (nq->node == node) {
			found = 1;
			nq = NULL;
		}
		else {
			nq = ll_next(&queue->nodes_ready);
		}
	}
	ll_finish(&queue->nodes_ready);
	assert(found >= 0);

	if (found == 0) {
		ll_start(&queue->nodes_busy);
		nq = ll_next(&queue->nodes_busy);
		while (nq) {
			assert(nq->node);
			if (nq->node == node) {
				found = 1;
				nq = NULL;
			}
			else {
				nq = ll_next(&queue->nodes_busy);
			}
		}
		ll_finish(&queue->nodes_busy);
		assert(found >= 0);

		if (found == 0) {
			ll_start(&queue->nodes_waiting);
			nq = ll_next(&queue->nodes_waiting);
			while (nq) {
				assert(nq->node);
				if (nq->node == node) {
					found = -1;
					nq = NULL;
				}
				else {
					nq = ll_next(&queue->nodes_waiting);
				}
			}
			ll_finish(&queue->nodes_waiting);

			if (found == 0) {
				ll_start(&queue->nodes_consuming);
				tmp = ll_next(&queue->nodes_consuming);
				while (tmp) {
					if (tmp == node) {
						found = -2;
						tmp = NULL;
					}
					else {
						tmp = ll_next(&queue->nodes_consuming);
					}
				}
				ll_finish(&queue->nodes_consuming);
				assert(found <= 0);
			}
		}
		assert(found <= 0);
	}

	return(found);
}


//-----------------------------------------------------------------------------
// This function will send consume requests for this queue to all the connected
// controllers.  However, before sending the request we need to check to see if
// the controller is already consuming the queue...
static void queue_notify(queue_t *queue)
{
	node_t *node;
	short int exclusive;
	
	assert(queue->qid > 0);
	assert(queue->name != NULL);
	assert(queue->sysdata);
	assert(queue->sysdata->nodelist);

	// now that we have our server object, we can go thru the list of nodes.  If
	// any of them are controllers, then we need to send a consume request.
	ll_start(queue->sysdata->nodelist);
	while ((node = ll_next(queue->sysdata->nodelist))) {
		
		if (BIT_TEST(node->flags, FLAG_NODE_CONTROLLER)) {

			assert(node->controller);
	
			exclusive = 0;
			if (BIT_TEST(queue->flags, QUEUE_FLAG_EXCLUSIVE)) {
				exclusive = 1;
			}

			if (queue_check_node(queue, node) == 0) {
				sendConsume(node, queue->name, 1, QUEUE_LOW_PRIORITY, exclusive);
				ll_push_head(&queue->nodes_consuming, node);
				logger(node->sysdata->logging, 2, "Sending consume of '%s' to controller node %d", queue->name, node->handle);
			}
		}
		else {
			assert(node->controller == NULL);
		}
	}
	ll_finish(queue->sysdata->nodelist);
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
	
	assert(node);
	assert(node->sysdata);
	assert(node->sysdata->queues);

	ll_start(node->sysdata->queues);
	while ((queue = ll_next(node->sysdata->queues))) {
		assert(queue->qid > 0);
		assert(queue->name);

		found = 0;
		
		// need to check this node in the busy list.
		ll_start(&queue->nodes_busy);
		nq = ll_next(&queue->nodes_busy);
		while (nq && found == 0) {

			assert(nq->node);
			if (nq->node == node) {
				logger(node->sysdata->logging, 2, "queue %d:'%s' removing node:%d from busy list", queue->qid, queue->name, node->handle);

				ll_remove(&queue->nodes_busy, nq);

				free(nq);
				nq = NULL;
				found++;
			}
			else {
				nq = ll_next(&queue->nodes_busy);
			}
		}
		ll_finish(&queue->nodes_busy);
		
		// ready
		if (found == 0) {
			ll_start(&queue->nodes_ready);
			nq = ll_next(&queue->nodes_ready);
			while (nq) {
	
				assert(nq->node);
				if (nq->node == node) {
					logger(node->sysdata->logging, 2,
						"queue %d:'%s' removing node:%d from ready list",
						queue->qid, queue->name, node->handle);
	
					ll_remove(&queue->nodes_ready, nq);
					
					free(nq);
					nq = NULL;
					found++;
				}
				else {
					nq = ll_next(&queue->nodes_ready);
				}
			}
			ll_finish(&queue->nodes_ready);
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

					logger(node->sysdata->logging, 2,
						"Promoting waiting node:%d to EXCLUSIVE queue '%s'.",
						nq->node->handle, queue->name);
				}
			}
		}
		else {
			assert(found == 0);
		
			// and waiting list.
			ll_start(&queue->nodes_waiting);
			nq = ll_next(&queue->nodes_waiting);
			while (nq) {
	
				assert(nq->node);
				if (nq->node == node) {
					logger(node->sysdata->logging, 2,
						"queue %d:'%s' removing node:%d from waitinglist",
						queue->qid, queue->name, node->handle);
	
					ll_remove(&queue->nodes_waiting, nq);
						
					free(nq);
					nq = NULL;
					found++;
				}
				else {
					nq = ll_next(&queue->nodes_waiting);
				}
			}
			ll_finish(&queue->nodes_waiting);
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
					queue_shutdown(queue);
				}
				else {
					// but it does have messages waiting to be deliverd... so we wont delete the queue just yet.
				}
			}
		}

		// and the nodes_consuming list
		ll_start(&queue->nodes_consuming);
		nq = ll_next(&queue->nodes_consuming);
		while (nq) {
			assert(nq->node);
			if (nq->node == node) {
				logger(node->sysdata->logging, 2,
					"queue %d:'%s' removing node:%d from consuminglist",
					queue->qid, queue->name, node->handle);
	
				ll_remove(&queue->nodes_consuming, nq);
				free(nq);
				nq = NULL;
			}
			else {
				nq = ll_next(&queue->nodes_consuming);
			}
		}
		ll_finish(&queue->nodes_consuming);
		
	}
	ll_finish(node->sysdata->queues);

	
}



queue_t * queue_create(system_data_t *sysdata, char *qname)
{ 
	queue_t *q, *tmp;

	assert(sysdata);
	assert(qname);
	
	// create an initialise a new queue structure.
	q = (queue_t *) malloc(sizeof(queue_t));
	queue_init(q);

	// look at the qid for the queue that is currently at the top of the list and set ours +1.
	tmp = ll_get_head(sysdata->queues);
	if (tmp) {
		assert(tmp->qid > 0);
		q->qid = tmp->qid + 1;
	}
	else {
		q->qid = 1;
	}
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
	if (BIT_TEST(queue->flags, QUEUE_FLAG_EXCLUSIVE) && (ll_count(&queue->nodes_busy) > 0 || ll_count(&queue->nodes_ready) > 0)) {
		// The queue is already in exclusive mode, and we have nodes processing
		// it, so this node would need to be added to the waiting list.

		ll_push_head(&queue->nodes_waiting, nq);
		logger(node->sysdata->logging, 2, "processConsume - Defered, queue already consumed exclusively.");
		return (0);
	}
	else {
		assert(ll_count(&queue->nodes_waiting) == 0);

		// if the consume request was for an EXCLUSIVE queue (and since we got
		// this far it means that no other nodes are consuming this queue yet),
		// then we mark it as exclusive.
		if (BIT_TEST(flags, QUEUE_FLAG_EXCLUSIVE)) {
			assert(ll_count(&queue->nodes_ready) == 0);
			assert(ll_count(&queue->nodes_busy) == 0);
			BIT_SET(queue->flags, QUEUE_FLAG_EXCLUSIVE);
			logger(node->sysdata->logging, 2, 
				"Consuming Queue '%s' in EXCLUSIVE mode.",
				expbuf_string(&node->data.queue));
		}

		// add the node to the appropriate list.
		ll_push_head(&queue->nodes_ready, nq);

		// notify other nodes (controllers) that we are consuming a queue.
		assert(queue->sysdata);
		queue_notify(queue);

		logger(node->sysdata->logging, 2, "Consuming queue: qid=%d", queue->qid);

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
	message_t *msg;
	system_data_t *sysdata;
	node_queue_t *nq;

	assert(queue);
	assert(queue->sysdata);

	sysdata = queue->sysdata;

	// when this function is fired, there should be at least one message in the
	// queue to process.
	assert(ll_count(&queue->msg_pending) > 0);
	msg = ll_pop_head(&queue->msg_pending);
	if (msg) {

		assert(BIT_TEST(msg->flags, FLAG_MSG_ACTIVE));
	
		// check the message to see if it is broadcast.
		if (BIT_TEST(msg->flags, FLAG_MSG_BROADCAST)) {
	
			logger(sysdata->logging, 2, "queue_deliver: delivering broadcast message");
	
			// This is a broadcast message.
			assert(BIT_TEST(msg->flags, FLAG_MSG_NOREPLY));
			assert(msg->source_node == NULL);
			assert(msg->target_node == NULL);
			assert(msg->queue != NULL);
	
			// if we have at least one node that is not busy, then we will send the message.
			// even if a node is busy, we will send the broadcast to it.
			if (ll_count(&queue->nodes_ready) > 0) {
				ll_start(&queue->nodes_ready);
				while ((nq = ll_next(&queue->nodes_ready))) {
					assert(nq->node);
					logger(sysdata->logging, 2, "queue_deliver: sending broadcast msg to node:%d", nq->node->handle);
					assert(msg->target_node == NULL);
					sendMessage(nq->node, msg);
				}
				ll_finish(&queue->nodes_ready);
				
				// since it is broadcast, we are not expecting a reply, so we can delete
				// the message (it should already be removed from the node).
				assert(BIT_TEST(msg->flags, FLAG_MSG_ACTIVE));
				message_clear(msg);

				// at this point, we know about the message, but we dont really know
				// it's id, so we wont update the 'next' value.  This is only a minor
				// performance thing and we might spend more time trying to determine
				// the id of this message than we would save.  This really only happens
				// for broadcast messages, if we were using a REQUEST message, then we
				// would actually know the ID when we got the REPLY back, so we could
				// update the 'next' value then.  
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
			// This is a request.  Even requests with NOREPLY work the same at this
			// point, because we keep the message in memory until DELIVERED is
			// received.
	
			// if we have a node that is ready, use it.
			nq = ll_pop_head(&queue->nodes_ready);
			if (nq) {
				assert(nq->max == 0 || (nq->waiting <= nq->max));
				
				// add the node pointer to the message.
				msg->target_node = nq->node;

				// send the message to the node.
				logger(sysdata->logging, 2, "queue_deliver: sending msg to node:%d", nq->node->handle);
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
				logger(sysdata->logging, 2,
					"queue_deliver. q:%d, no nodes ready to consume.", queue->qid);

				// we couldn't process the message, so put it back.
				ll_push_head(&queue->msg_pending, msg);
			}
		}
	}
	else {
		logger(sysdata->logging, 2,
			"queue_deliver: queue:%d, no messages waiting.", queue->qid);
	}
}


// this function is called when a message has been delivered (in NOREPLY
// mode), or a reply sent.  It is used to remove a node from the busy list if
// it is marked as busy.
void queue_msg_done(queue_t *queue, node_t *node)
{
	node_queue_t *nq;
	
	assert(queue);
	assert(node);


	ll_start(&queue->nodes_busy);
	nq = ll_next(&queue->nodes_busy);
	while (nq) {
		assert(nq->node);
		if (nq->node == node) {
			// found it... in the busy list... so remove it from the busy list and add it back to the ready list.

			assert(nq->waiting > 0);
			nq->waiting --;
			assert(nq->waiting >= 0);
			
			ll_remove(&queue->nodes_busy, nq);
			ll_push_tail(&queue->nodes_ready, nq);
			nq = NULL;
		}
		else {
			nq = ll_next(&queue->nodes_busy);
		}
	}
	ll_finish(&queue->nodes_busy);
}



//-----------------------------------------------------------------------------
// shutdown the queue.  If there are messages in a queue waiting to be
// delivered or processed, we will need to wait for them.  Once all nodes have
// been stopped, then we can exit.
void queue_shutdown(queue_t *queue)
{
	system_data_t *sysdata;
	
	assert(queue);
	assert(queue->sysdata);
	sysdata = queue->sysdata;
	
	// if there is pending messages to deliver, then reply to the source, indicating unable to deliver.
	if (ll_count(&queue->msg_pending) > 0) {
		// We have a message that needs to be returned.

		assert(0);
	}
	
	// if we have messages being processed, might need to reply to source.
	if (ll_count(&queue->msg_proc) > 0) {
		// We have a message that needs to be returned.

		assert(0);
	}


	// delete the list of nodes that are consuming this queue.
	if (ll_count(&queue->nodes_busy) > 0) {
		assert(0);
	}

	if (ll_count(&queue->nodes_ready) > 0) {
		assert(0);
	}

	if (ll_count(&queue->nodes_waiting) > 0) {
		assert(0);
	}
}


void queue_set_id(list_t *queues, const char *name, queue_id_t id)
{
	queue_t *q = NULL;
	queue_t *tmp;

	assert(queues);
	assert(name);
	assert(id > 0);

	ll_start(queues);
	tmp = ll_next(queues);
	while (tmp) {
		assert(tmp->name);
		assert(tmp->qid > 0);

		if (strcmp(tmp->name, name) == 0) {
			q = tmp;
			q->qid = id;
			tmp = NULL;
		}
		else {
			tmp = ll_next(queues);
		}
	}
	ll_finish(queues);
}



// This function is used to dump detailed information about the queue to a expanding buffer.
void queue_dump(queue_t *q, expbuf_t *buf)
{
	assert(q && buf);

	assert(q->name);

	expbuf_print(buf, "\tName: %s\n", q->name);
	expbuf_print(buf, "\tID: %d\n", q->qid);

	expbuf_print(buf, "\tFlags: ");
	if (BIT_TEST(q->flags, QUEUE_FLAG_EXCLUSIVE))
		expbuf_print(buf, "EXCLUSIVE ");
	expbuf_print(buf, "\n");
	
	expbuf_print(buf, "\tMessages Pending: %d\n", ll_count(&q->msg_pending));
	// TODO: show info about the pending messages.
	expbuf_print(buf, "\tMessages Processing: %d\n", ll_count(&q->msg_proc));
	// TODO: show info about the processing messages.
	expbuf_print(buf, "\tNodes Ready: %d\n", ll_count(&q->nodes_ready));
	// todo: show info about the ready nodes.
	expbuf_print(buf, "\tNodes Busy: %d\n", ll_count(&q->nodes_busy));
	// todo: show info about the busy nodes.
	expbuf_print(buf, "\tNodes Waiting: %d\n", ll_count(&q->nodes_waiting));
	// todo: show info about the waiting nodes.
}

