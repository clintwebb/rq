#ifndef __NODE_H
#define __NODE_H

#include <event.h>
#include <risp.h>
#include <expbuf.h>
#include <expbufpool.h>
#include <rq.h>

#include "controllers.h"
#include "data.h"
#include "message.h"
#include "system_data.h"


#define DEFAULT_BUFFSIZE 1024

#define FLAG_NODE_ACTIVE			1
#define FLAG_NODE_CLOSING 		2
#define FLAG_NODE_CONTROLLER	4
#define FLAG_NODE_BUSY        8

typedef struct {
	int handle;
	unsigned short flags;
	struct event *read_event,
	             *write_event;
	expbuf_t *waiting,
	         *out;
	data_t data;
	system_data_t *sysdata;
	list_t in_msg,
	       out_msg;
	int idle;
	controller_t *controller;
} node_t ;


void node_init(node_t *node, system_data_t *sysdata);
void node_free(node_t *node);
void node_shutdown(node_t *node);
node_t * node_create(system_data_t *sysdata, int handle);

void node_write_now(node_t *node, int length, char *data);
void node_read_handler(int hid, short flags, void *data);
void node_write_handler(int hid, short flags, void *data);

message_t * node_findoutmsg(node_t *node, msg_id_t msgid);


#endif


