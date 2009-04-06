///// action.c

#include "actions.h"
#include "send.h"
#include "server.h"

#include <assert.h>
#include <evactions.h>
#include <stdlib.h>
#include <unistd.h>




//-----------------------------------------------------------------------------
// This action is created when we need to shutdown the server.  
void ah_server_shutdown(action_t *action)
{
	server_t *server;
	action_t *fire;
	stats_t *stats;
	system_data_t *sysdata;
	queue_t *q;
	node_t *node;
	int i;
	unsigned char verbose = 0;
	
	assert(action != NULL);
	assert(action->shared != NULL);
	sysdata = action->shared;
	
	assert(sysdata->server != NULL);
	server = sysdata->server;

	assert(sysdata->verbose >= 0);
	verbose = sysdata->verbose;

	if (server->shutdown == 0) {

		if (verbose) printf("Shutting down server object.\n");

		// close the listening handle and remove the listening event, we dont want
		// to accept any more connections.
		if (verbose) printf("Closing listening sockets.\n");
		for (i=0; i<MAX_SERVERS; i++) {
			if (server->servers[i].handle != INVALID_HANDLE) {
				if (verbose) printf("Closing socket %d.\n", server->servers[i].handle);

			  event_del(&server->servers[i].event);
				close(server->servers[i].handle);
				server->servers[i].handle = INVALID_HANDLE;
			}
		}

		// All active nodes should be informed that we are shutting down.
		assert(sysdata->actpool != NULL);
		if (verbose) printf("Firing event to shutdown nodes.\n");
		node = server->nodelist;
		while (node) {
			
			assert(node->handle > 0);

			// fire action_node_shutdown events for each node.
			fire = action_pool_new(sysdata->actpool);
			action_set(fire, 0, ah_node_shutdown, node);
			node->refcount ++;
			fire = NULL;

			if (verbose)
				printf("Initiating shutdown of node %d.\n", node->handle);
		
			node = node->next;
		}
		
		// fire an action for each queue.
		assert(sysdata->actpool != NULL);
		if (verbose) printf("Initiating shutdown of queues.\n");
		q = sysdata->queues;
		while (q) {
			assert(q->name != NULL);
			assert(q->qid > 0);
	
			fire = action_pool_new(sysdata->actpool);
			action_set(fire, 250, ah_queue_shutdown, q);
			fire = NULL;
	
			if (verbose)
				printf("Initiating shutdown of queue %d ('%s').\n",
					q->qid,
					q->name);

			q = q->next;
		}

		// fire this action again, because we are not finished.
		server->shutdown = 1;
		action_timeout(action, 1000);
		action_reset(action);
	}
	else if (server->shutdown == 1) {
		// if already shutting down

		// check that all nodes have shutdown.
		if (server->nodelist && verbose)
			printf("Waiting for %d nodes to shutdown.\n", server->active);
		else if (sysdata->queues && verbose)
			printf("Waiting for queues to shutdown.\n");
		
		if (server->nodelist == NULL && sysdata->queues == NULL) {
			// we are good to quit.
			// stop stats timer event.
			assert(sysdata != NULL);
			assert(sysdata->stats != NULL);
			stats = sysdata->stats;
			assert(stats->shutdown == 0);
			stats->shutdown ++;
			
			action_pool_return(action);
		}
		else {
// 			if (verbose)
// 				printf("Waiting for server to shutdown.\n");
			action_reset(action);
		}
	}
	else {
		// we are done, so we dont need to do anything more... the system should start shutting down now.
		assert(0);
	}
}


//-----------------------------------------------------------------------------
// shutdown the node.  If the node still has pending requests, then we need to
// wait until the requests have been returned or have timed out.
void ah_node_shutdown(action_t *action)
{
	system_data_t *sysdata;
	server_t *server;
	node_t *node;

	assert(action != NULL);
	assert(action->shared != NULL);
	assert(action->data != NULL);
	
	sysdata = action->shared;
	node = action->data;

	// if the node is consuming any queues, we need to cancel them (internally)
	if (BIT_TEST(node->flags, FLAG_NODE_NOQUEUES) == 0) {
		queue_cancel_node(node);
		BIT_SET(node->flags, FLAG_NODE_NOQUEUES);
	}

	// if the node is still connected, then we will attempt to first tell it that
	// we are closing the node.  If the node closed the connection first, then we
	// wont have a chance to do this.
	if (node->handle != INVALID_HANDLE && BIT_TEST(node->flags, FLAG_NODE_CLOSING) == 0) {
		sendClosing(node);
		BIT_SET(node->flags, FLAG_NODE_CLOSING);
	}

	// We are still connected to the node, so we need to put a timeout on the
	// messages the node is servicing.
	if (node->handle != INVALID_HANDLE && node->msglist) {

		// need to set a timeout on the messages.
		assert(0);
		
		// we still have stuff to do for this node, so we will reset the action to try again.
		action_timeout(action, 100);
		action_reset(action);
	}
	else {

		// we are not connected to the node anymore, so we cannot service the
		// messages that we were waiting for, so we need to return replies on
		// undelivery to the source nodes.
		if (node->handle == INVALID_HANDLE && node->msglist) {
			
			// send undelivered notices to the owners of all the pending messages
			// or return the message to the queue.
			assert(0);
		}

		// if we have done all we need to do, but still have a valid handle, then we should close it, and delete the event.
		if (node->handle != INVALID_HANDLE && node->out->length == 0) {
			assert(node->out->length == 0);
			assert(node->msglist == NULL);
			assert(node->handle > 0);
			close(node->handle);
			node->handle = INVALID_HANDLE;

			assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));
			assert(node->event.ev_base != NULL);
			event_del(&node->event);
			BIT_SET(node->flags, FLAG_NODE_ACTIVE);

			node->refcount --;

			// remove the node from the server object.
			server = sysdata->server;
			assert(server);
			assert(server->nodelist);
			if (server->nodelist == node) server->nodelist = node->next;
			if (node->next) node->next->prev = node->prev;
			if (node->prev) node->prev->next = node->next;
			node->prev = NULL;
			node->next = NULL;
			
			// free the resources used by the node.
			node_free(node);
			free(node);
	
			// we are done, so we dont need the action any more.
			action_pool_return(action);
		}
		else {
			// we have outgoing data that we are waiting to deliver... so continue to wait.
			action_timeout(action, 100);
			action_reset(action);
		}
	}
}



//-----------------------------------------------------------------------------
// shutdown the queue.  If there are messages in a queue waiting to be
// delivered or processed, we will need to wait for them.  Once all nodes have
// been stopped, then we can exit.
void ah_queue_shutdown(action_t *action)
{
	queue_t *queue, *tmp;
	system_data_t *sysdata;
	
	assert(action != NULL);

	assert(action->shared != NULL);
	sysdata = action->shared;
	
	assert(action->data != NULL);
	queue = action->data;

	// if there is pending messages to deliver, then reply to the source, indicating unable to deliver.
	if (queue->msghead != NULL) {
		// We have a message that needs to be returned.

		assert(0);
	}
	
	// delete the list of nodes that are consuming this queue.
	assert(0);   // code needs to be changed.
	
	// Delete the queue from the queue list.
	tmp = sysdata->queues;
	assert(tmp);
	while (tmp) {
		if (tmp == queue) {
			if (sysdata->queues == tmp) sysdata->queues = tmp->next;
			if (tmp->prev) tmp->prev->next = tmp->next;
			if (tmp->next) tmp->next->prev = tmp->prev;
			queue_free(tmp);
			free(tmp);
			tmp = NULL;
		}
		else {
			tmp = tmp->next;
		}
	}

	action_pool_return(action);
}



//-----------------------------------------------------------------------------
// This action handler handles the stats output.  Even if we are not doing any
// output this needs to run so that it can keep the counters under control.
void ah_stats(action_t *action)
{
	int clients;
	int queues;
	system_data_t *sysdata;
	stats_t *stats;
	queue_t *q;
	server_t *server;
	
	assert(action != NULL);
	assert(action->shared != NULL);
	assert(action->data != NULL);

	sysdata = action->shared;
	stats = action->data;

	assert(sysdata->server != NULL);
	server = sysdata->server;

	assert(server->active >= 0);
	clients = server->active;

	queues = 0;
	q = sysdata->queues;
	while (q) {
		assert(q->name != NULL);
		assert(q->qid > 0);
		queues ++;
		q = q->next;
	}

	assert(stats != NULL);
	if (stats->in_bytes || stats->out_bytes || stats->requests || stats->replies || stats->broadcasts) {

		assert(sysdata->actpool);

		if (sysdata->verbose)
			printf("Bytes [%u/%u], Clients [%u], Requests [%u], Replies [%u], Broadcasts [%u], Queues[%u], ActPool[%u/%u]\n",
				stats->in_bytes,
				stats->out_bytes,
				clients,
				stats->requests,
				stats->replies,
				stats->broadcasts,
				queues,
				action_pool_active(sysdata->actpool),
				action_pool_inactive(sysdata->actpool));
		
		stats->in_bytes = 0;
		stats->out_bytes = 0;
		stats->requests = 0;
		stats->replies = 0;
		stats->broadcasts = 0;
	}

	assert(stats != NULL);
	if (stats->shutdown == 0) {
		// since we are not shutting down, schedule the action again.
		action_reset(action);
	}
	else {
		// we are not firing the action again, so we should return it to the pool it came from.
		action_pool_return(action);
	}
}


//-----------------------------------------------------------------------------
// 
void ah_message(action_t *action)
{
	assert(action);
	assert(0);
}


//-----------------------------------------------------------------------------
// When a node consumes a queue for the first time, we need to send a consume
// request to other controllers.
void ah_queue_notify(action_t *action)
{
	system_data_t *sysdata;
	queue_t *queue;
	server_t *server;
	node_t *node;
	
	assert(action);
	
	assert(action->shared);
	sysdata = action->shared;

	assert(action->data);
	queue = action->data;
	assert(queue->qid > 0);
	assert(queue->name != NULL);

	assert(sysdata->server);
	server = sysdata->server;

	// now that we have our server object, we can go thru the list of nodes.  If
	// any of them are controllers, then we need to send a consume request.
	node = server->nodelist;
	while (node) {
		if (BIT_TEST(node->flags, FLAG_NODE_CONTROLLER)) {
			sendConsume(node, queue->name, 1, QUEUE_LOW_PRIORITY);
		}
		
		node = node->next;
	}

	// return the action to the action pool.
	action_pool_return(action);
}

// When a message is added to the queue, 
void ah_queue_deliver(action_t *action)
{
	system_data_t *sysdata;
	queue_t *q;
	node_queue_t *nq;
	message_t *msg;
	queue_msg_t *qm;
	action_t *delact;

	assert(action);
	q = action->data;
	sysdata = action->shared;
	assert(q && sysdata);

	assert(q->msghead && q->msgtail);
	assert(q->msghead->msg);
	assert(q->msghead->prev == NULL);

	msg = q->msghead->msg;
	assert(msg);

	// check the message to see if it is broadcast.
	if (BIT_TEST(msg->flags, FLAG_MSG_BROADCAST)) {

		if (sysdata->verbose > 1) printf("ah_queue_deliver: delivering broadcast message\n");

		// This is a broadcast message.
		assert(BIT_TEST(msg->flags, FLAG_MSG_NOREPLY));
		assert(msg->next == NULL);
		assert(msg->prev == NULL);

		// if we have at least one node that is not busy, then we will send the message.
		// even if a node is busy, we will send the broadcast to it.
		if (q->ready_head) {
			nq = q->ready_head;
			while (nq) {
				assert(nq->node);
				if (sysdata->verbose > 1) printf("ah_queue_deliver: sending broadcast msg to node:%d\n", nq->node->handle);
				sendMessage(nq->node, msg);
			}

			// we have delivered the broadcast msg to all the consuming nodes, so we
			// will remove the message and set an action to free it.
			assert(q->msgtail);
			qm = q->msghead;
			assert(qm->prev == NULL);
			if (qm->next) qm->next->prev = NULL;
			if (q->msghead == q->msgtail) q->msgtail = NULL;
			q->msghead = qm->next;
			mempool_return(q->sysdata->qmpool, qm);
			qm = NULL;

			// since it is broadcast, we are not expecting a reply, so we can delete
			// the message (it should already be removed from the node).
			// create action to delete the message.
			assert(q->sysdata->actpool);
			delact = action_pool_new(q->sysdata->actpool);
			assert(msg);
			action_set(delact, 0, ah_msg_delete, msg);
			delact = NULL;
		}
	}
	else {
		// This is a request.

		// if we have a node that is ready, use it.
		if (q->ready_head) {

			nq = q->ready_head;

			assert(nq->waiting <= nq->max);
			
			// send the message to the node.
			if (sysdata->verbose > 1) printf("ah_queue_deliver: sending msg to node:%d\n", nq->node->handle);
			sendMessage(nq->node, msg);
	
			// increment the 'waiting' count for the nq.
			nq->waiting ++;
			assert(nq->waiting > 0 && nq->waiting <= nq->max);
	
			// if the node has reached the max number of consumed messages, then it
			// will be put in the busy list.
			if (nq->waiting >= nq->max) {
				if (q->ready_tail == q->ready_head) q->ready_tail = NULL;
				if (nq->next) nq->next->prev = NULL;
				q->ready_head = nq->next;
				assert(nq->prev == NULL);
				nq->next = q->busy;
				q->busy = nq;
			}
			else {
				// if the 'available' is less than the next node in the list, then
				// move this node to the end of the ready list.

				assert(q->ready_head);
				assert(q->ready_tail);
				if (q->ready_head->next) {
					if ((q->ready_head->next->max - q->ready_head->next->waiting) > (nq->max - nq->waiting)) {
						assert(nq->prev == NULL);
						nq->prev = q->ready_tail;
						assert(q->ready_tail->next == NULL);
						q->ready_tail->next = nq;

						q->ready_head = nq->next;
						assert(nq->next->prev == nq);
						nq->next->prev = NULL;
						nq->next = NULL;
						
						q->ready_tail = nq;
					}
				}
			}
				
			// remove the msg from the msglist, and add it to the msgproc list.
			qm = q->msghead;
			if (qm->next) qm->next->prev = NULL;
			q->msghead = qm->next;
			qm->next = q->msgproc;
			assert(q->msgproc->prev == NULL);
			q->msgproc->prev = qm;
			q->msgproc = qm;
		}
	}

	// return the action to the action pool.
	action_pool_return(action);	
}


//-----------------------------------------------------------------------------
// This action is created when a message is provided that has a timeout.  This
// event will fire every second, decrementing the counter of the message.  If
// the countdown gets to 0, then it will initiate a 'return' of the failed
// message.
void ah_msg_countdown(action_t *action)
{
	assert(action);

	assert(0);
}


//-----------------------------------------------------------------------------
// This action is used to delete a message.  It will only delete the message if
// it is safe to do so.  If it cannot delete the message, it will simply exit,
// returning the action.  When other activities occur that might allow for the
// message to be deleted, it will raise the action again.
void ah_msg_delete(action_t *action)
{
	assert(action);

	assert(0);

	// return the action to the action pool.
	action_pool_return(action);	
	
}


