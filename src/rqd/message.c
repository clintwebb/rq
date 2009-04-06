
#include "message.h"
#include "queue.h"

#include <assert.h>
#include <rq.h>
#include <stdlib.h>


//-----------------------------------------------------------------------------
// Initialise a message object.
void message_init(message_t *msg, system_data_t *sysdata)
{
	assert(msg != NULL);
	assert(sysdata != NULL);
	
	msg->id = 0;
	msg->flags = 0;
	msg->timeout = 0;
	msg->data = NULL;
	msg->source_id = 0;
	msg->source_node = NULL;
	msg->target_node = NULL;
	msg->queue = NULL;
	msg->next = NULL;
	msg->prev = NULL;
	msg->sysdata = sysdata;
	msg->refcount = 0;
}


//-----------------------------------------------------------------------------
// free the resources in a message object (but not the object itself.)
void message_free(message_t *msg)
{
	assert(msg != NULL);
	assert(msg->sysdata != NULL);

	// If we are freeing the message, there shouldn't be anything referencing it.
	assert(msg->refcount == 0);

	if (msg->data != NULL) {
		expbuf_pool_return(msg->sysdata->bufpool, msg->data);
		msg->data = NULL;
	}

	assert(msg->next == NULL && msg->prev == NULL);
		
	msg->source_node = NULL;
	msg->target_node = NULL;
	msg->queue = NULL;
}

//-----------------------------------------------------------------------------
// Provides the pointer to the original node this message originated from.
// When replies are received, they will be returned to this node.
void message_set_orignode(message_t *msg, void *node)
{
	assert(msg != NULL);
	assert(node != NULL);
	assert(msg->source_node == NULL);
	msg->source_node = node;
}


//-----------------------------------------------------------------------------
// Set the original msg ID for the message, that we will need when we return
// anuy data to the node.
void message_set_origid(message_t *msg, message_id_t id)
{
	assert(msg != NULL);
	assert(id > 0);
	assert(msg->source_id == 0);
	msg->source_id = id;
}


//-----------------------------------------------------------------------------
// Set the flag that indicates that the message is a broadcast message (to be
// delivered to multiple nodes).
void message_set_broadcast(message_t *msg)
{
	assert(msg != NULL);
	BIT_SET(msg->flags, FLAG_MSG_BROADCAST);
}

//-----------------------------------------------------------------------------
// Set the flag that indicates that we dont expect a reply to this message.
void message_set_noreply(message_t *msg)
{
	assert(msg != NULL);
	BIT_SET(msg->flags, FLAG_MSG_NOREPLY);
}


//-----------------z------------------------------------------------------------
// Assign the queue object that this message belongs to.
void message_set_queue(message_t *msg, void *queue)
{
	assert(msg != NULL);
	assert(queue != NULL);
	assert(msg->queue == NULL);
	msg->queue = queue;
}

//-----------------------------------------------------------------------------
// If the message has a timeout, we need to know.
void message_set_timeout(message_t *msg, int seconds)
{
	assert(msg);
	assert(seconds >= 0);

	assert(0);
}

