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
	queue_list_t *ql;
	int i;
	short int status;
	unsigned char verbose = 0;
	
	assert(action != NULL);
	assert(action->shared != NULL);
	sysdata = action->shared;
	
	assert(sysdata->server != NULL);
	server = sysdata->server;

	assert(sysdata->queuelist != NULL);
	ql = sysdata->queuelist;

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
		for (i=0; i<server->maxconns; i++) {
			if (server->nodes[i] != NULL) {
				if (server->nodes[i]->handle != INVALID_HANDLE) {
					// fire action_node_shutdown events for each node.
					fire = action_pool_new(sysdata->actpool);
					action_set(fire, 250, ah_node_shutdown, server->nodes[i]);
					fire = NULL;

					if (verbose)
						printf("Initiating shutdown of node %d.\n", server->nodes[i]->handle);
				}
			}
		} 
		

		// fire an action for each queue.
		assert(sysdata->actpool != NULL);
		if (verbose) printf("Initiating shutdown of queues.\n");
		for (i=0; i < ql->queues; i++) {
			if (ql->list[i] != NULL) {
				assert(ql->list[i]->name != NULL);
				assert(ql->list[i]->qid > 0);
			
				fire = action_pool_new(sysdata->actpool);
				action_set(fire, 250, ah_queue_shutdown, ql->list[i]);
				fire = NULL;

				if (verbose)
					printf("Initiating shutdown of queue %d ('%s').\n",
						ql->list[i]->qid,
						ql->list[i]->name);
			}
		}

		// fire this action again, because we are not finished.
		server->shutdown = 1;
		action_timeout(action, 1000);
		action_reset(action);
	}
	else if (server->shutdown == 1) {
		// if already shutting down

		// this status counter will start at zero, and will increment if we find
		// anything that hasn't shutdown yet.
		status = 0;
		

		// check that all nodes have shutdown.
		for (i=0; status == 0 && i < server->maxconns; i++) {
			if (server->nodes[i] != NULL) {
				if (server->nodes[i]->handle != INVALID_HANDLE) {
					status ++;
				}
			}
		}
		if (status > 0 && verbose)
			printf("Waiting for nodes to shutdown.\n");
		
				
		// check that all queues have been stopped.
		// TODO: We dont really need to do this... only to show if we are waiting on queues... because the events will stick in the event loop until they are done anyway.
		if (status == 0) {
			for (i=0; status == 0 && i < ql->queues; i++) {
				if (ql->list[i] != NULL) {
					status ++;
				}
			}
			if (status > 0 && verbose)
				printf("Waiting for queues to shutdown.\n");
		}
		
		if (status == 0) {
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
		assert(sysdata->queuelist);
		queue_cancel_node(sysdata->queuelist, node);

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
	system_data_t *sysdata;
	queue_list_t *ql;
	queue_t *queue;
	int i;
	
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
	if (queue->nodelist != NULL) {
		free(queue->nodelist);
		queue->nodelist = NULL;
		queue->nodes = 0;
	}
	assert(queue->nodelist == NULL && queue->nodes == 0);

	// Delete the queue from the queue list.
	ql = sysdata->queuelist;
	assert(ql);
	assert((ql->queues == 0 && ql->list == NULL) || (ql->queues > 0 && ql->list != NULL));
	assert(queue != NULL);
	for (i=0; i < ql->queues; i++) {
		if (ql->list[i] == queue) {
			assert(ql->list[i]);
			queue_free(ql->list[i]);
			free(ql->list[i]);
			ql->list[i] = NULL;
			break;
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
	int i;
	system_data_t *sysdata;
	stats_t *stats;
	queue_list_t *ql;
	queue_t *q;
	server_t *server;
	
	assert(action != NULL);
	assert(action->shared != NULL);
	assert(action->data != NULL);

	sysdata = action->shared;
	stats = action->data;

	assert(sysdata->queuelist != NULL);
	ql = sysdata->queuelist;
	
	assert(sysdata->server != NULL);
	server = sysdata->server;

	assert(server->active >= 0);
	clients = server->active;

	assert((ql->queues == 0 && ql->list == NULL) || (ql->queues > 0 && ql->list != NULL));
	queues = 0;
	for (i=0; i < ql->queues; i++) {
		q = ql->list[i];
		if (q != NULL) {
			assert(q->name != NULL);
			assert(q->qid > 0);
			queues ++;
		}
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
	int i;
	
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
	for (i=0; i<server->maxconns; i++) {
		assert(server->nodes);
		if (server->nodes[i] != NULL) {
			if (BIT_TEST(server->nodes[i]->flags, FLAG_NODE_CONTROLLER)) {
				sendConsume(server->nodes[i], queue->name, 1, QUEUE_LOW_PRIORITY);
			}
		}
	}

	// return the action to the action pool.
	action_pool_return(action);
}


