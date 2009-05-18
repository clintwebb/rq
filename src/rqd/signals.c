// signals.c

#include "queue.h"
#include "server.h"
#include "signals.h"
#include "stats.h"
#include "system_data.h"


#include <assert.h>
#include <event.h>
#include <stdio.h>
#include <unistd.h>

//-----------------------------------------------------------------------------
// When the SIGINT signal is received, we need to start shutting down the
// service.  This means that it needs to:
//   1.  Close the listening socket.
//   2.  Send a CLOSING message to every connected node.
//   3.  if there are any undelivered messages in the queues, we need to return them.
//   4.  Shutdown the nodes if we can.
//   5.  Shutdown the queues if we can.
//   6.  Tell the stats system to close its event as soon as there are no more nodes connected.
void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
	system_data_t *sysdata = (system_data_t *) arg;
	server_t *server;
	unsigned char verbose = 0;
	stats_t *stats;
	queue_t *q;
	node_t *node;
	int i;
	void *next;
	controller_t *ct;
	
	printf("SIGINT\n");

	assert(sysdata);
	assert(sysdata->server);
	assert(sysdata->verbose >= 0);

	server = sysdata->server;
	verbose = sysdata->verbose;

	// delete the sigint event, we dont need it anymore.
	assert(sysdata->sigint_event);
	event_free(sysdata->sigint_event);
	sysdata->sigint_event = NULL;

	// delete the sighup event, we dont need that either.
	assert(sysdata->sighup_event);
	event_free(sysdata->sighup_event);
	sysdata->sighup_event = NULL;

	if (verbose) printf("Shutting down server object.\n");

	// close the listening handle and remove the listening event, we dont want
	// to accept any more connections.
	if (verbose) printf("Closing listening sockets.\n");
	for (i=0; i<MAX_SERVERS; i++) {
		if (server->servers[i].handle != INVALID_HANDLE) {
			if (verbose) printf("Closing socket %d.\n", server->servers[i].handle);

			assert(server->servers[i].event);
			event_free(server->servers[i].event);
			server->servers[i].event = NULL;
			close(server->servers[i].handle);
			server->servers[i].handle = INVALID_HANDLE;
		}
	}

	// All active nodes should be informed that we are shutting down.
	if (verbose) printf("Firing event to shutdown nodes.\n");
	assert(sysdata->nodelist);
	next = ll_start(sysdata->nodelist);
	while ((node = ll_next(sysdata->nodelist, &next))) {
		assert(node->handle > 0);
		if (verbose) printf("Initiating shutdown of node %d.\n", node->handle);
		node_shutdown(node);
	}

	// Now attempt to shutdown all the queues.  Basically, this just means that the queue will close down all the 'waiting' consumers.
	if (verbose) printf("Initiating shutdown of queues.\n");
	next = ll_start(sysdata->queues);
	while ((q = ll_next(sysdata->queues, &next))) {
		assert(q->name != NULL);
		assert(q->qid > 0);
		if (verbose) printf("Initiating shutdown of queue %d ('%s').\n", q->qid, q->name);
		queue_shutdown(q);
	}

	// if we have controllers that are attempting to connect, we need to change their status so that they dont.
	if (verbose) printf("Stopping controllers that are connecting.\n");
	next = ll_start(sysdata->controllers);
	while ((ct = ll_next(sysdata->controllers, &next))) {
		BIT_SET(ct->flags, FLAG_CONTROLLER_FAILED);
		if (ct->connect_event) {
			event_free(ct->connect_event);
			ct->connect_event = NULL;
		}
	}

	// Put stats event on notice that we are shutting down, so that as soon as there are no more nodes, it needs to stop its event.
	assert(sysdata != NULL);
	assert(sysdata->stats != NULL);
	stats = sysdata->stats;
	assert(stats->shutdown == 0);
	stats->shutdown ++;
}



void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
	printf("SIGHUP\n");
}

