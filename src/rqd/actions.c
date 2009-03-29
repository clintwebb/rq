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
			action_set(fire, 250, ah_node_shutdown, node);
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
	node_t *node;
	int pending = 0;
	int i;

	assert(action != NULL);
	assert(action->shared != NULL);
	assert(action->data != NULL);
	
	sysdata = action->shared;
	node = action->data;


	if (BIT_TEST(node->flags, FLAG_NODE_CLOSING) == 0) {
		
		// if this is the first time for the node, we need to look at all the messages it is servicing and we need to put a timeout on it.
		assert(node);
		for (i=0; i < node->messages; i++) {
			if (node->msglist[i] != NULL) {

				

				assert(0);
			}
		}

		// if the node is consuming any queues, we need to cancel them (internally)
		assert(sysdata->queues);
		queue_cancel_node(sysdata->queues, node);

		// send out a message to the node telling them that the server is going offline.
		sendClosing(node);
		BIT_SET(node->flags, FLAG_NODE_CLOSING);
	}

	// we need to make sure that the node doesnt have any pending requests.
	for (i=0; i < node->messages && pending == 0; i++) {
		if (node->msglist[i] != NULL) {
			pending ++;
		}
	}

	if (pending == 0) { action_pool_return(action); }
	else { action_reset(action); }
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


