#ifndef __SYSTEM_DATA_H
#define __SYSTEM_DATA_H

#include <evactions.h>
#include <event.h>
#include <expbufpool.h>
#include <linklist.h>
#include <mempool.h>
#include <risp.h>

typedef struct {
	struct event_base *evbase;
	risp_t *risp;
	expbuf_pool_t *bufpool;
	action_pool_t *actpool;
	mempool_t *msgpool;

	struct event *sighup_event;
	struct event *sigint_event;

	expbuf_t *in_buf, *build_buf;
	
	int verbose;
	void *settings;
	void *server;
	void *stats;
	list_t *queues;
	list_t *nodelist;
} system_data_t;


#endif
