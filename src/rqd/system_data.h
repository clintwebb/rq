#ifndef __SYSTEM_DATA_H
#define __SYSTEM_DATA_H

#include <evactions.h>
#include <event.h>
#include <expbufpool.h>
#include <mempool.h>
#include <risp.h>

typedef struct {
	struct event_base *evbase;
	expbuf_pool_t *bufpool;
	risp_t *risp;
	action_pool_t *actpool;
	mempool_t *msgpool;
	
	int verbose;
	void *settings;
	void *server;
	void *stats;
	void *queuelist;
} system_data_t;


#endif
