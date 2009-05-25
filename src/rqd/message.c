
#include "message.h"
#include "queue.h"

#include <assert.h>
#include <evlogging.h>
#include <rq.h>
#include <stdio.h>
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
	msg->sysdata = sysdata;
}


//-----------------------------------------------------------------------------
// free the resources in a message object (but not the object itself.)
void message_free(message_t *msg)
{
	assert(msg != NULL);
	assert(msg->sysdata != NULL);

	// If we are freeing the message, there shouldn't be anything referencing it.
	assert(msg->source_node == NULL);
	assert(msg->target_node == NULL);
	assert(msg->queue == NULL);

	if (msg->data != NULL) {
		expbuf_clear(msg->data);
		expbuf_pool_return(msg->sysdata->bufpool, msg->data);
		msg->data = NULL;
	}
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

	// set the timeout value into the message structure.
	assert(msg->timeout == 0);
	assert(BIT_TEST(msg->flags, FLAG_MSG_TIMEOUT) == 0);
	msg->timeout = seconds;
	BIT_SET(msg->flags, FLAG_MSG_TIMEOUT);

	// set some better values, because the timeout is not very accurate.  Need to look at hte current time, and specify an expiry.  Keep in mind that the socket will only break on Timeout every 5 seconds, so any timeout smaller than that might not get actioned unless there are other messages arriving on that queue that pushes it through.
	assert(0);
}


//-----------------------------------------------------------------------------
// This action is used to delete a message.  It will only delete the message if
// it is safe to do so.  If it cannot delete the message, it will simply exit,
// returning the action.  When other activities occur that might allow for the
// message to be deleted, it will raise the action again.
void message_delete(message_t *msg)
{
	assert(msg);
	assert(msg->sysdata);
	assert(msg->sysdata->msgpool);

	// if the flags indicate that the message has been decoupled,
	if (msg->source_node == NULL && msg->target_node == NULL && msg->queue == NULL) {
		// clear it, and add it back to the mempool we got it from.
		message_free(msg);
		mempool_return(msg->sysdata->msgpool, msg);
	}
	else {
		logger(msg->sysdata->logging, 2,
			"Unable to delete message(%X) since it is still referenced somewhere",
			(unsigned int)msg);
	}
}

