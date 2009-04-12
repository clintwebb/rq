#ifndef __MESSAGE_H
#define __MESSAGE_H

//---------------------------------------------------------------------

#include "system_data.h"

#include <expbuf.h>
#include <rq.h>


#define FLAG_MSG_BROADCAST	0x01
#define FLAG_MSG_NOREPLY		0x02
#define FLAG_MSG_TIMEOUT    0x08		// set if there is a timeout specified.

typedef int message_id_t;

typedef struct __message_t {
	message_id_t   id;
	unsigned int   flags;
	int            timeout;
	expbuf_t      *data;
	message_id_t   source_id;
	void          *source_node;
	void          *target_node;
	void          *queue;
	system_data_t *sysdata;
} message_t;



void message_init(message_t *msg, system_data_t *sysdata);
void message_free(message_t *msg);

void message_set_orignode(message_t *msg, void *node);
void message_set_origid(message_t *msg, message_id_t id);
void message_set_broadcast(message_t *msg);
void message_set_noreply(message_t *msg);
void message_set_queue(message_t *msg, void *queue);
void message_set_timeout(message_t *msg, int seconds);

#endif

