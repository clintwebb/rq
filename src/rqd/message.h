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

typedef struct {
	message_id_t   id;						// ID given to the delivery node.
	unsigned int   flags;					// flags that indicate various modes and settings.
	int            timeout;				// timeout value to be counted down.
	expbuf_t      *data;
	message_id_t   source_id;			// ID received from the source.
	void          *source_node;
	void          *target_node;
	void          *queue;
	system_data_t *sysdata;
} message_t;



void message_init(message_t *msg, system_data_t *sysdata);
void message_free(message_t *msg);
void message_delete(message_t *msg);

void message_set_orignode(message_t *msg, void *node);
void message_set_origid(message_t *msg, message_id_t id);
void message_set_broadcast(message_t *msg);
void message_set_noreply(message_t *msg);
void message_set_queue(message_t *msg, void *queue);
void message_set_timeout(message_t *msg, int seconds);

#endif

