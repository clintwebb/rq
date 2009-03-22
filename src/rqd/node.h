#ifndef __NODE_H
#define __NODE_H

#include <event.h>
#include <risp.h>
#include <expbuf.h>
#include <expbufpool.h>
#include <rq.h>

#include "data.h"
#include "message.h"
#include "system_data.h"


#define DEFAULT_BUFFSIZE 1024

#define FLAG_NODE_ACTIVE			0x01
#define FLAG_NODE_CLOSING 		0x02
#define FLAG_NODE_CONTROLLER	0x04

typedef struct {
	int handle;
	unsigned short flags;
	struct event event;
	expbuf_t *in, *waiting, *out, *build;
	data_t data;
	system_data_t *sysdata;

	message_t **msglist;
	int messages;
} node_t ;


void node_init(node_t *node, system_data_t *sysdata);
void node_clear(node_t *node);
void node_free(node_t *node);

void node_write_now(node_t *node, int length, char *data);
void node_event_handler(int hid, short flags, void *data);


#endif


