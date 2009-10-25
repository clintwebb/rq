// signals.c

#include "queue.h"
#include "server.h"
#include "signals.h"
#include "stats.h"
#include "system_data.h"


#include <assert.h>
#include <event.h>
#include <evlogging.h>
#include <stdio.h>
#include <stdlib.h>
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
	stats_t *stats;
	queue_t *q;
	node_t *node;
	controller_t *ct;
	
	logger(sysdata->logging, 3, "SIGINT");

	assert(sysdata);
	assert(sysdata->servers);

	// delete the sigint event, we dont need it anymore.
	assert(sysdata->sigint_event);
	event_free(sysdata->sigint_event);
	sysdata->sigint_event = NULL;

	// delete the sighup event, we dont need that either.
	assert(sysdata->sighup_event);
	event_free(sysdata->sighup_event);
	sysdata->sighup_event = NULL;

	// delete the sigusr1 event, we dont need that either.
	assert(sysdata->sigusr1_event);
	event_free(sysdata->sigusr1_event);
	sysdata->sigusr1_event = NULL;

	// delete the sigusr2 event, we dont need that either.
	assert(sysdata->sigusr2_event);
	event_free(sysdata->sigusr2_event);
	sysdata->sigusr2_event = NULL;

	logger(sysdata->logging, 2, "Shutting down servers.");
	while ((server = ll_pop_head(sysdata->servers))) {
		server_shutdown(server);
		server_free(server);
		free(server);
	}

	// All active nodes should be informed that we are shutting down.
	logger(sysdata->logging, 2, "Shutting down nodes.");
	assert(sysdata->nodelist);
	ll_start(sysdata->nodelist);
	while ((node = ll_next(sysdata->nodelist))) {
		assert(node->handle > 0);
		logger(sysdata->logging, 2, "Initiating shutdown of node %d.", node->handle);
		node_shutdown(node);
	}
	ll_finish(sysdata->nodelist);

	// Now attempt to shutdown all the queues.  Basically, this just means that the queue will close down all the 'waiting' consumers.
	logger(sysdata->logging, 2, "Initiating shutdown of queues.");
	ll_start(sysdata->queues);
	while ((q = ll_next(sysdata->queues))) {
		assert(q->name != NULL);
		assert(q->qid > 0);
		logger(sysdata->logging, 2, "Initiating shutdown of queue %d ('%s').", q->qid, q->name);
		queue_shutdown(q);
	}
	ll_finish(sysdata->queues);

	// if we have controllers that are attempting to connect, we need to change their status so that they dont.
	logger(sysdata->logging, 2, "Stopping controllers that are connecting.");
	ll_start(sysdata->controllers);
	while ((ct = ll_next(sysdata->controllers))) {
		BIT_SET(ct->flags, FLAG_CONTROLLER_FAILED);
		if (ct->connect_event) {
			event_free(ct->connect_event);
			ct->connect_event = NULL;
		}
	}
	ll_finish(sysdata->controllers);

	// Put stats event on notice that we are shutting down, so that as soon as there are no more nodes, it needs to stop its event.
	assert(sysdata != NULL);
	assert(sysdata->stats != NULL);
	stats = sysdata->stats;
	assert(stats->shutdown == 0);
	stats->shutdown ++;

	// Tell the logging system not to use events anymore.
	log_direct(sysdata->logging);
}



void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
	system_data_t *sysdata = (system_data_t *) arg;
	expbuf_t *buf;
	queue_t *q;
	
	assert(sysdata);
	assert(sysdata->bufpool);
	buf = expbuf_pool_new(sysdata->bufpool, 1024);

	expbuf_print(buf, "Complete data dump\n");

	assert(sysdata->msg_list);
	expbuf_print(buf, "Messages:\n\tMax=%d\n\tActive=%d\n\n", sysdata->msg_max, sysdata->msg_used);

	assert(sysdata->in_buf);
	assert(sysdata->build_buf);
	expbuf_print(buf, "In Buffer size: %d\n", BUF_MAX(sysdata->in_buf));
	expbuf_print(buf, "Build Buffer size: %d\n", BUF_MAX(sysdata->build_buf));

	expbuf_print(buf, "\nEvents:\n");
	expbuf_print(buf, "\tsigint: %s\n",  sysdata->sigint_event  ? "yes" : "no");
	expbuf_print(buf, "\tsighup: %s\n",  sysdata->sighup_event  ? "yes" : "no");
	expbuf_print(buf, "\tsigusr1: %s\n", sysdata->sigusr1_event ? "yes" : "no");
	expbuf_print(buf, "\tsigusr2: %s\n", sysdata->sigusr2_event ? "yes" : "no");

// 	list_t *servers;

// 	list_t *controllers;

// 	list_t *nodelist;

	expbuf_print(buf, "\nQueues:\n");
	assert(sysdata->queues);
	ll_start(sysdata->queues);
	while ((q = ll_next(sysdata->queues))) {
		queue_dump(q, buf);
	}
	ll_finish(sysdata->queues);


	assert(sysdata->logging);
	expbuf_print(buf, "\nLogging:\n");
	expbuf_print(buf, "\tLog Level: %d\n", log_getlevel(sysdata->logging));
  expbuf_print(buf, "\tDump string length: ");
	expbuf_print(buf, "%d\n", BUF_LENGTH(buf));

	logger(sysdata->logging, 1, "%s", expbuf_string(buf));
	expbuf_clear(buf);

	// return the buffer to the pool.
	expbuf_pool_return(sysdata->bufpool, buf);
}

void sigusr1_handler(evutil_socket_t fd, short what, void *arg)
{
	system_data_t *sysdata = (system_data_t *) arg;
	log_inclevel(sysdata->logging);
	logger(sysdata->logging, 0, "Loglevel increased to %d", log_getlevel(sysdata->logging));
}

void sigusr2_handler(evutil_socket_t fd, short what, void *arg)
{
	system_data_t *sysdata = (system_data_t *) arg;
	log_declevel(sysdata->logging);
	logger(sysdata->logging, 0, "Loglevel decreased to %d", log_getlevel(sysdata->logging));
}
