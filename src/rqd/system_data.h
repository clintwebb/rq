#ifndef __SYSTEM_DATA_H
#define __SYSTEM_DATA_H

#include <evactions.h>
#include <event.h>
#include <expbufpool.h>
#include <mempool.h>
#include <risp.h>

typedef struct {
	struct event_base *evbase;
	risp_t *risp;
	expbuf_pool_t *bufpool;
	action_pool_t *actpool;
	mempool_t *msgpool;
	mempool_t *qmpool;
	
	int verbose;
	void *settings;
	void *server;
	void *stats;
	void *queues;
} system_data_t;


#endif
