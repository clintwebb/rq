#ifndef __SYSTEM_DATA_H
#define __SYSTEM_DATA_H

#include "settings.h"
#include "stats.h"

#include <event.h>
#include <evlogging.h>
#include <expbufpool.h>
#include <linklist.h>
#include <mempool.h>
#include <risp.h>


typedef struct {
	struct event_base *evbase;
	risp_t *risp;
	
	mempool_t *msgpool;
	
	expbuf_pool_t *bufpool;
	expbuf_t *in_buf, *build_buf;

	struct event *sigint_event;
	struct event *sighup_event;
	struct event *sigusr1_event;
	struct event *sigusr2_event;

	settings_t *settings;
	stats_t *stats;
	list_t *queues;
	list_t *nodelist;
	list_t *controllers;
	list_t *servers;

	logging_t *logging;
} system_data_t;


#endif
