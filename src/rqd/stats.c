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

	stats->logfile = NULL;
	stats->shutdown = 0;
}



void stats_display(stats_t *stats, system_data_t *sysdata)
{
	int clients;
	int queues;
	int msg_pending, msg_proc;
	server_t *server;
	queue_t *q;
	void *n;
	
	assert(stats);
	assert(sysdata);
	assert(sysdata->server != NULL);
	
	server = sysdata->server;

	assert(server->active >= 0);
	clients = server->active;

// 	queues = ll_count(sysdata->queues);

	queues = 0;
	msg_pending = 0;
	msg_proc = 0;
	n = ll_start(sysdata->queues);
	q = ll_next(sysdata->queues, &n);
	while (q) {
		queues ++;
		msg_pending += ll_count(&q->msg_pending);
		msg_proc += ll_count(&q->msg_proc);
		
		q = ll_next(sysdata->queues, &n);
	}

	assert(stats != NULL);
	if (stats->in_bytes || stats->out_bytes || stats->requests || stats->replies || stats->broadcasts) {

		assert(sysdata->actpool);

		if (sysdata->verbose)
			printf("Bytes [%u/%u], Clients [%u], Requests [%u], Replies [%u], Broadcasts [%u], Queues[%u], Msgs[%d/%d] MsgPool[%u/%u]\n",
				stats->in_bytes,
				stats->out_bytes,
				clients,
				stats->requests,
				stats->replies,
				stats->broadcasts,
				queues,
				msg_pending, msg_proc,
				mempool_active_count(sysdata->msgpool), mempool_inactive_count(sysdata->msgpool));
		
		stats->in_bytes = 0;
		stats->out_bytes = 0;
		stats->requests = 0;
		stats->replies = 0;
		stats->broadcasts = 0;
	}
}
