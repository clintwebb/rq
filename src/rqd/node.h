#ifndef __NODE_H
#define __NODE_H

#include <event.h>
#include <risp.h>
#include <expbuf.h>
#include <expbufpool.h>
#include <rq.h>

#include "data.h"
#include "system_data.h"


#define DEFAULT_BUFFSIZE 1024

#define FLAG_NODE_ACTIVE			1
#define FLAG_NODE_CLOSING 		2
#define FLAG_NODE_CONTROLLER	4
#define FLAG_NODE_NOQUEUES    8
#define FLAG_NODE_BUSY        16

typedef struct {
	int handle;
	unsigned short flags;
	struct event *read_event, *write_event;
	expbuf_t *waiting, *out;
	data_t data;
	system_data_t *sysdata;
	list_t in_msg, out_msg;
	int idle;
// 	int refcount;
} node_t ;


void node_init(node_t *node, system_data_t *sysdata);
void node_free(node_t *node);
void node_shutdown(node_t *node);

void node_write_now(node_t *node, int length, char *data);
void node_read_handler(int hid, short flags, void *data);
void node_write_handler(int hid, short flags, void *data);


#endif


