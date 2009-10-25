#ifndef __SYSTEM_DATA_H
#define __SYSTEM_DATA_H

#include "message.h"
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
	
	// message list.  We keep an array of message_t structures.   This array is
	// used for all the requests received by the system.  The index in the array
	// is used as the message ID that is passed to the nodes for processing.
	// When replies are returned, the message ID is again specified so that the
	// reply payload can be returned to the node that delivered it.
	message_t **msg_list;
	int msg_max;
	int msg_next;
	int msg_used;

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
