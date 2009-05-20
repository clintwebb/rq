// stats.c

#include "queue.h"
#include "server.h"
#include "stats.h"


#include <assert.h>
#include <stdio.h>


void stats_init(stats_t *stats)
{
	assert(stats != NULL);
	
	stats->out_bytes = 0;
	stats->in_bytes = 0;
	stats->requests = 0;
	stats->replies = 0;
	stats->broadcasts = 0;
	stats->re = 0;
	stats->we = 0;
	stats->te = 0;

	stats->logfile = NULL;
	stats->shutdown = 0;

	stats->sysdata = NULL;
}






void stats_handler(int fd, short int flags, void *arg)
{
	stats_t *stats;
	int clients;
	int queues;
	int msg_pending, msg_proc;
	server_t *server;
	queue_t *q;
	void *n;

	assert(fd < 0);
	assert((flags & EV_TIMEOUT) == EV_TIMEOUT);
	assert(arg);

	stats = arg;

	// clear the stats event.
	assert(stats->stats_event);
	event_free(stats->stats_event);
	stats->stats_event = NULL;
	
	assert(stats->sysdata);
	assert(stats->sysdata->server != NULL);
	
	server = stats->sysdata->server;

	clients = ll_count(stats->sysdata->nodelist);

	queues = 0;
	msg_pending = 0;
	msg_proc = 0;
	n = ll_start(stats->sysdata->queues);
	q = ll_next(stats->sysdata->queues, &n);
	while (q) {
		queues ++;
		msg_pending += ll_count(&q->msg_pending);
		msg_proc += ll_count(&q->msg_proc);
		
		q = ll_next(stats->sysdata->queues, &n);
	}

	assert(stats != NULL);
	if (stats->in_bytes || stats->out_bytes || stats->requests || stats->replies || stats->broadcasts || stats->re || stats->we) {

		if (stats->sysdata->verbose)
			printf("Bytes[%u/%u], Clients[%u], Requests[%u], Replies[%u], Broadcasts[%u], Queues[%u], Msgs[%d/%d], MsgPool[%u/%u], Events[%u/%u/%u]\n",
				stats->in_bytes,
				stats->out_bytes,
				clients,
				stats->requests,
				stats->replies,
				stats->broadcasts,
				queues,
				msg_pending, msg_proc,
				mempool_active_count(stats->sysdata->msgpool), mempool_inactive_count(stats->sysdata->msgpool),
				stats->re, stats->we, stats->te);
		
		stats->in_bytes = 0;
		stats->out_bytes = 0;
		stats->requests = 0;
		stats->replies = 0;
		stats->broadcasts = 0;
		stats->re = 0;
		stats->we = 0;
		stats->te = 0;
	}

	// if we are not shutting down, then schedule the stats event again.
	if (stats->shutdown == 0) {
		stats_start(stats);
	}
	
}



void stats_start(stats_t *stats)
{
	struct timeval t = {.tv_sec = 1, .tv_usec = 0};
	
	assert(stats);
	assert(stats->stats_event == NULL);
	assert(stats->sysdata);
	assert(stats->sysdata->evbase);
		
	stats->stats_event = evtimer_new(stats->sysdata->evbase, stats_handler, (void *) stats);
	evtimer_add(stats->stats_event, &t);
}

