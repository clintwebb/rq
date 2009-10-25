
#include "message.h"
#include "queue.h"

#include <assert.h>
#include <evlogging.h>
#include <rq.h>
#include <stdio.h>
#include <stdlib.h>


//-----------------------------------------------------------------------------
// reset a message that was in use.  Even though some data should have been
// cleared as it was being processed, we will want to make sure that is so.
void message_clear(message_t *msg)
{
	assert(msg);

	assert(BIT_TEST(msg->flags, FLAG_MSG_ACTIVE));
	assert(msg->id >= 0);
	
	msg->flags = 0;
	msg->timeout = 0;
	msg->source_id = 0;
	
	assert(msg->source_node == NULL);
	assert(msg->target_node == NULL);
	assert(msg->queue == NULL);
	assert(msg->data == NULL);
}


//-----------------------------------------------------------------------------
// Initialise a message object.
void message_init(message_t *msg, message_id_t id)
{
	assert(msg != NULL);
	assert(id >= 0);

	msg->id = id;
	msg->flags = 0;
	msg->timeout = 0;
	msg->source_id = 0;
	
	msg->data = NULL;
	msg->source_node = NULL;
	msg->target_node = NULL;
	msg->queue = NULL;
}


//-----------------------------------------------------------------------------
// free the resources in a message object (but not the object itself.)
void message_free(message_t *msg)
{
	assert(msg);

	assert(BIT_TEST(msg->flags, FLAG_MSG_ACTIVE) == 0);
	
	// there are no other resources to release that shouldn't have been cleaned up with message_clean().
}




//-----------------------------------------------------------------------------
// Set the original msg ID for the message, that we will need when we return
// anuy data to the node.
void message_set_origid(message_t *msg, message_id_t id)
{
	assert(msg != NULL);
	assert(id >= 0);
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


//-----------------------------------------------------------------------------
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




