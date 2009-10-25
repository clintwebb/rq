// commands.c


#include "commands.h"
#include "node.h"
#include "queue.h"
#include "send.h"

#include <assert.h>
#include <evlogging.h>
#include <risp.h>
#include <rq.h>
#include <stdio.h>
#include <stdlib.h>


#ifndef BIT_TEST
#error BIT_TEST is a macro that should be included as part of RQ.
#endif

void cmdNop(node_t *ptr)
{
	assert(ptr != NULL);
}


//-----------------------------------------------------------------------------
// this callback is called if we have an invalid command.  We shouldn't be
// receiving any invalid commands.
void cmdInvalid(void *base, void *data, risp_length_t len)
{
	node_t *node;
	unsigned char *cast;

	assert(base != NULL);
	assert(data != NULL);
	assert(len > 0);

	node = (node_t *) base;
	cast = (unsigned char *) data;

	assert(node != NULL);
	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 1,
		"Received invalid (%d)): [%d, %d, %d]", len, cast[0], cast[1], cast[2]);
		
	assert(0);
}

// This callback function is to be fired when the CMD_CLEAR command is 
// received.  It should clear off any data received and stored in variables 
// and flags.  In otherwords, after this is executed, the node structure 
// should be in a predictable state.
void cmdClear(void *base) 
{
 	node_t *node = (node_t *) base;

 	assert(node != NULL);
 	assert(node->handle >= 0);
 	
	data_clear(&node->data);
 	
 	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3, "node:%d CLEAR", node->handle);
}

//-----------------------------------------------------------------------------
// PING and PONG are handled differently to all other flags.  It will be
// actioned straight away, and does not require an EXECUTE.
void cmdPing(void *base)
{
 	node_t *node = (node_t *) base;

 	assert(node != NULL);
 	assert(node->handle >= 0);
 	
	sendPong(node);
}


//-----------------------------------------------------------------------------
// When a PONG is received, it is assumed that we sent a ping.  It can also be
// used as a keep-alive.
void cmdPong(void *base)
{
 	node_t *node = (node_t *) base;

 	assert(node != NULL);
 	assert(node->handle >= 0);

 	assert(node->idle >= 0);
 	node->idle = 0;

 	if (BIT_TEST(node->flags, FLAG_NODE_BUSY)) {
		BIT_CLEAR(node->flags, FLAG_NODE_BUSY);

		// since the node is no longer marked as busy, then we need to alert the
		// queues so that they can begin sending messages to this node again.
		assert(0);
	}
}

//-----------------------------------------------------------------------------
// This function is used to find the next available message object that is not
// in use.  We will first check the 'next' value.  If it is available, then we
// will use that, and then increment the 'next' value.  If we dont find what we
// are looking for with 'next', then we will go trhough the list and try to
// find one.  If we still dont find one, then we need to increase the number of
// elements in the list, and create a new one.
static message_t * next_message(node_t *node)
{
	message_t *msg = NULL;
	system_data_t *sysdata;
	int i;

	assert(node);
	
	sysdata = node->sysdata;
	assert(sysdata);

	assert(sysdata->msg_list);
	assert(sysdata->msg_max > 0);
	assert(sysdata->msg_next < sysdata->msg_max);
	assert(sysdata->msg_used <= sysdata->msg_max);

	if (sysdata->msg_used < sysdata->msg_max) {
		// We know that there must be at least one availble message in the list.

		// first check the 'next' entry and see if it contains an empty slot.
		if (sysdata->msg_next != -1) {
			assert(sysdata->msg_next >= 0 && sysdata->msg_next < sysdata->msg_max);
			assert(sysdata->msg_list[sysdata->msg_next]);
			assert(sysdata->msg_list[sysdata->msg_next]->id == sysdata->msg_next);
			assert(sysdata->msg_list[sysdata->msg_next]->flags == 0);
			
			msg = sysdata->msg_list[sysdata->msg_next];
			sysdata->msg_next = -1;
		}

		// if we still dont have a message object, then we need to go through the
		// list and find one.
		for (i=0; i < sysdata->msg_max && msg == NULL; i++) {
			assert(sysdata->msg_list[i]);
			if (sysdata->msg_list[i]->flags == 0) {
				msg = sysdata->msg_list[i];
				assert(sysdata->msg_next == -1);
			}
		}

		assert(msg);
	}
	else {
		// the list is full of active messages, so we need to create more.
		assert(sysdata->msg_next == -1);
		assert(0);

		// increase the size of the list.
		sysdata->msg_list = (message_t **) realloc(sysdata->msg_list, sizeof(message_t *) * (sysdata->msg_max + 1));
		assert(sysdata->msg_list);
		msg = (message_t *) malloc(sizeof(message_t));
		assert(msg);
		message_init(msg, sysdata->msg_max);
		sysdata->msg_list[sysdata->msg_max] = msg;
		sysdata->msg_max ++;
	}

	// at this point, we should have a message object.
	assert(msg);

	// increment the count.
	sysdata->msg_used ++;
	assert(sysdata->msg_used <= sysdata->msg_max);

	// mark the message as active.
	assert(msg->flags == 0);
	BIT_SET(msg->flags, FLAG_MSG_ACTIVE);

	// the node provided would have to be the source, so we should assign it as the source node.
	assert(msg->source_node == NULL);
	assert(node);
	msg->source_node = node;

	// we're done.  Return the message object.
	assert(msg);
	return(msg);
}


//-----------------------------------------------------------------------------
// A request has been received for a queue.  We need take it and pass it to a
// node that can handle the request.
void cmdRequest(void *base)
{
	node_t *node = (node_t *) base;
	message_t *msg;
	queue_t *q;
	stats_t *stats;

 	assert(node);
 	assert(node->handle >= 0);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d REQUEST (flags:%x, mask:%x)", node->handle, node->data.flags, node->data.mask);

	assert(node->sysdata->queues);
	assert(node->sysdata->bufpool);

	// make sure we have the required data. At least payload, and a queueid or queue.
	if (BIT_TEST(node->data.mask, DATA_MASK_PAYLOAD) && (BIT_TEST(node->data.mask, DATA_MASK_QUEUE) || BIT_TEST(node->data.mask, DATA_MASK_QUEUEID))) {

		// create the message object to hold the data.
		assert(node->sysdata->msg_list);
		msg = next_message(node);
		assert(msg);

		// The node should have received a payload command.  It would have been
		// assigned to an appropriate buffer.  We need to move that buffer to the
		// message, where it will be handled from there.
		assert(node->data.payload);
		assert(msg->data == NULL);
		msg->data = node->data.payload;
		node->data.payload = NULL;
		
		// if message is NOREPLY, then we dont need some bits.  However, we will need to send a DELIVERED.
		if (BIT_TEST(node->data.flags, DATA_FLAG_NOREPLY)) {
			BIT_SET(msg->flags, FLAG_MSG_NOREPLY);
			assert(msg->source_id == 0);

			// the source_node would have been set when the message object was
			// obtained.  But since we dont want it in this mode, we set it to
			// NULL.
			assert(msg->source_node != NULL);
			msg->source_node = NULL;

			assert(BIT_TEST(node->data.mask, DATA_MASK_ID));
			sendDelivered(node, node->data.id);
		}
		else {
			assert(BIT_TEST(msg->flags, FLAG_MSG_NOREPLY) == 0);
		
			// make a note in the msg object, the source node. If a reply is
			// expected, a messageid should also have been supplied, use that for
			// the node_side.
			assert(msg->source_node == node);
			assert(BIT_TEST(node->data.mask, DATA_MASK_ID));
			message_set_origid(msg, node->data.id);
		}
		
		if (BIT_TEST(node->data.mask, DATA_MASK_TIMEOUT)) {
			// set the timeout... 
			message_set_timeout(msg, node->data.timeout);
		}
		
		// find the q object for this queue.
		if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {
			q = queue_get_name(node->sysdata->queues, expbuf_string(&node->data.queue));
		}
		else if (BIT_TEST(node->data.mask, DATA_MASK_QUEUEID)) {
			q = queue_get_id(node->sysdata->queues, node->data.qid);
		}
		else {
			assert(0);
		}
	
		if (q == NULL) {
			// we dont have a queue, so we will need to create one.
			q = queue_create(node->sysdata, expbuf_string(&node->data.queue));
		}
		assert(q);
		assert(ll_count(node->sysdata->queues) > 0);
		
		// add the message to the queue.

		logger(node->sysdata->logging, 2, "processRequest: node:%d, msg_id:%d, q:%d", node->handle, msg->id, q->qid);
		assert(q->sysdata);
		queue_addmsg(q, msg);

		stats = node->sysdata->stats;
		assert(stats);
		stats->requests ++;
	}
	else {
		// required data was not found.
		// need to return some sort of error
		assert(0);
	}
}


void cmdReply(void *base)
{
	node_t *node = (node_t *) base;
	msg_id_t id;
	message_t *msg;
	stats_t *stats;
	queue_t *q;

 	assert(node);
	assert(node->handle >= 0);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3, 
		"node:%d REPLY (flags:%x, mask:%x)", node->handle, node->data.flags, node->data.mask);

	// make sure that we have the minimum information that we need.
	if (BIT_TEST(node->data.mask, DATA_MASK_ID) && BIT_TEST(node->data.mask, DATA_MASK_PAYLOAD)) {

		id = node->data.id;
		assert(id >= 0);

		// find the message that the reply belongs to.
		assert(node->sysdata);
		assert(node->sysdata->msg_list);
		assert(id < node->sysdata->msg_max);
		msg = node->sysdata->msg_list[id];
		assert(msg);
		assert(msg->id == id);
		assert(BIT_TEST(msg->flags, FLAG_MSG_ACTIVE));
		assert(msg->target_node == node);

		// apply the payload which is part of the reply, replacing the payload which was the request.
		assert(node->sysdata);
		assert(node->sysdata->bufpool);
		assert(node->data.payload);
		assert(msg->data == NULL);
		msg->data = node->data.payload;
		node->data.payload = NULL;
		
		// send the payload to the source node of the message.
		assert(msg->source_node);
		sendReply(msg->source_node, msg);

		// tell the queue that the node has finished processing a message.  This
		// will find the node, and remove it from the node_busy list.
		assert(msg->queue);
		assert(msg->target_node);
		queue_msg_done(msg->queue, msg->target_node);

		// then remove from the queue->msg_proc list
		assert(msg->queue);
		q = msg->queue;
		ll_remove(&q->msg_proc, msg);
		msg->queue = NULL;

		assert(msg->data);
		assert(node->sysdata);
		assert(node->sysdata->bufpool);
		expbuf_clear(msg->data);
		expbuf_pool_return(node->sysdata->bufpool, msg->data);
		msg->data = NULL;


		// set action to remove the message.
		msg->source_node = NULL;
		msg->target_node = NULL;
		assert(BIT_TEST(msg->flags, FLAG_MSG_ACTIVE));
		message_clear(msg);
		assert(node->sysdata->msg_used > 0);
		node->sysdata->msg_used --;
		assert(node->sysdata->msg_used >= 0);
		node->sysdata->msg_next = msg->id;

		// if there are more messages in the queue, then we need to deliver them.
		if (ll_count(&q->msg_pending) > 0) {
			logger(node->sysdata->logging, 2, "delivery: setting delivery action.");
			queue_deliver(q);
		}
		else {
			logger(node->sysdata->logging, 2, "delivery: no items to deliver.");
		}


		stats = node->sysdata->stats;
		assert(stats);
		stats->replies ++;
	}
	else {
		// we should handle failure a bit better, and log the information.
		assert(0);
	}
	
}

void cmdBroadcast(void *base)
{
	node_t *node = (node_t *) base;
	queue_t *q = NULL;
	message_t *msg;
	
 	assert(node);
	assert(node->handle >= 0);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d BROADCAST (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	// do we have a queue name, or a qid?
	if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE) || BIT_TEST(node->data.mask, DATA_MASK_QUEUEID)) {

		if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {
			q = queue_get_name(node->sysdata->queues, expbuf_string(&node->data.queue));
		}
		else if (BIT_TEST(node->data.mask, DATA_MASK_QUEUEID)) {
			q = queue_get_id(node->sysdata->queues, node->data.qid);
		}

		if (q == NULL) {
			q = queue_create(node->sysdata, expbuf_string(&node->data.queue));
		}

		// by this point, we should have 'q'.
		assert(q);
		
		// create message object.
		msg = next_message(node);
		assert(msg);

		// now that we have a message structure completely filled out with the
		// data from the node, then we need to do something with it.
		assert(0);
	}
	else {
		// we didn't have a queue name, or a queue id.   We need to handle this gracefully.
		assert(0);
	}
}

void cmdNoReply(void *base)
{
	node_t *node = (node_t *) base;
 	
 	assert(node);
 	assert(node->handle >= 0);

 	// set our specific flag.
	BIT_SET(node->data.flags, DATA_FLAG_NOREPLY);
	
	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d NOREPLY (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);
}


void cmdExclusive(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node);

 	// set our specific flag.
	BIT_SET(node->data.flags, DATA_FLAG_EXCLUSIVE);

	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d EXCLUSIVE (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);
}


void cmdClosing(void *base)
{
	node_t *node = (node_t *) base;
 	
 	assert(node);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d CLOSING (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	// if the node is a regular consumer then we cancel all the queues that do
	// not have pending requests for this node.
	queue_cancel_node(node);

	// if we have messages still in the list, we need to go through them, to see
	// if there are any for this node that we need to set timeouts on.
	if (node->sysdata->msg_used > 0) {
		assert(0);
	}

	// mark the node as closing so that as soon as all the messages have
	// completed, the node can be shutdown.
	assert(BIT_TEST(node->flags, FLAG_NODE_CLOSING) == 0);
	BIT_SET(node->flags, FLAG_NODE_CLOSING);

	// if this node is a controller, then we may need to do something to handle
	// it.  Although it is likely that nothign would be done until the
	// connection is actually lost.
	if (BIT_TEST(node->flags, FLAG_NODE_CONTROLLER)) {
		// what do we need to do special to handle the closing of a controller node?
		// nothing really... that would be done when the connection is actually closed.
	}
}


//-----------------------------------------------------------------------------
// When a node indicates that it wants to consume a queue,the node needs to be
// added to the queue list.  If this is the first time this queue is being
// consumed, then we need to create an action so that it can be consumed on
// other servers.
void cmdConsume(void *base)
{
	node_t *node = (node_t *) base;
 	queue_t *q=NULL;
 	int max;
 	int priority;
 	unsigned int qflags;
 	
 	assert(node);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d CONSUME (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	// make sure that we have the minimum information that we need.
	if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {

		logger(node->sysdata->logging, 2, 
			"Processing QUEUE request from node:%d", node->handle);

		assert(BIT_TEST(node->data.mask, DATA_MASK_QUEUE));
		assert(BUF_LENGTH(&node->data.queue) > 0);
	
		// check to see if we already have a queue with this name, in our list.		
		assert(q == NULL);
		if (node->sysdata->queues)
			q = queue_get_name(node->sysdata->queues, expbuf_string(&node->data.queue));
		
		if (q == NULL) {
			// we didn't find the queue...
			logger(node->sysdata->logging, 2, 
				"Didn't find queue '%s', creating new entry.", expbuf_string(&node->data.queue));
			q = queue_create(node->sysdata, expbuf_string(&node->data.queue));
		}
	
	
		// at this point we have a queue object.  We dont yet know if this
		// consumption request can continue, because we haven't looked at the
		// queue options.
		assert(q != NULL);

		max = 0;
		priority = 0;
		qflags = 0;

		if (BIT_TEST(node->data.mask, DATA_MASK_MAX))
			max = node->data.max;
			
		if (BIT_TEST(node->data.mask, DATA_MASK_PRIORITY))
			priority = node->data.priority;
			
		if (BIT_TEST(node->data.flags, DATA_FLAG_EXCLUSIVE))
			BIT_SET(qflags, QUEUE_FLAG_EXCLUSIVE);
		
		if (queue_add_node(q, node, max, priority, qflags) > 0) {
			// send reply back to the node.
			sendConsumeReply(node, q->name, q->qid);
		}

		// need to check the queue to see if there are messages pending.  If there
		// are, then send some to this node.
		if (ll_count(&q->msg_pending) > 0) {
			queue_deliver(q);
		}
	}
}

void cmdCancelQueue(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);

	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3,
		"node:%d CANCEL QUEUE (flags:%x, mask:%x)", node->handle, node->data.flags, node->data.mask);

	assert(0);
}

void cmdId(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;

 	assert(node);
 	assert(value >= 0);
 	assert(node->handle >= 0);

	BIT_SET(node->data.mask, DATA_MASK_ID);
	node->data.id = value;
	
	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d ID (%d) (flags:%x, mask:%x)",
		node->handle, value, node->data.flags, node->data.mask);
}

void cmdQueueID(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;
 	assert(node);
 	assert(value >= 0 && value <= 0xffff);
	node->data.qid = value;
	BIT_SET(node->data.mask, DATA_MASK_QUEUEID);

	assert(node->sysdata);
	logger(node->sysdata->logging, 3, 
		"node:%d QUEUEID (%d)", node->handle, value);
}




void cmdTimeout(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;
 	assert(node != NULL);
 	assert(value >= 0 && value <= 0xffff);
	node->data.timeout = value;
	node->data.mask |= (DATA_MASK_TIMEOUT);

	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3,
		"node:%d TIMEOUT (%d)", node->handle, value);
}

void cmdMax(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;
 	assert(node != NULL);
 	assert(value >= 0 && value <= 0xffff);
	node->data.max = value;
	node->data.mask |= (DATA_MASK_MAX);

	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3,
		"node:%d MAX (%d)", node->handle, value);
}

void cmdPriority(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;
 	assert(node != NULL);
 	assert(value >= 0 && value <= 0xffff);
	node->data.mask |= (DATA_MASK_PRIORITY);
	node->data.priority = value;

	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3,
		"node:%d PRIORITY (%d)", node->handle, value);
}


void cmdQueue(void *base, risp_length_t length, risp_char_t *data)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
 	assert(length > 0);
 	assert(data != NULL);
 	expbuf_set(&node->data.queue, data, length);
 	BIT_SET(node->data.mask, DATA_MASK_QUEUE);
	
	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3, 
		"node:%d QUEUE (%s)", node->handle, expbuf_string(&node->data.queue));
}

//-----------------------------------------------------------------------------
// Payload data is handled slightly differently.  Because we need the actual
// data to be handled by different parts of the system, and we dont want to do
// memory copies all the time.  So the payload buffer itself will be moved.
// Therefore, each time we handle a payload, we will get a new buffer from the
// bufpool.  When all the commands are being executed, the payload buffer will
// be transferred to the message object that is created for it.  When
// everything has completed processing, then the buffer will be returned to the
// buffpool to be re-used.
void cmdPayload(void *base, risp_length_t length, risp_char_t *data)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
 	assert(length > 0);
 	assert(data != NULL);

	assert(node->sysdata);
	assert(node->sysdata->bufpool);
	
	assert(node->data.payload == NULL);
 	assert(BIT_TEST(node->data.mask, DATA_MASK_PAYLOAD) == 0);

	node->data.payload = expbuf_pool_new(node->sysdata->bufpool, length);
 	assert(node->data.payload);
 	expbuf_set(node->data.payload, data, length);
 	BIT_SET(node->data.mask, DATA_MASK_PAYLOAD);
	
	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3,
		"node:%d PAYLOAD (len:%d, flags:%x, mask:%x)",
			node->handle,
			length,
			node->data.flags,
			node->data.mask);
}


void cmdDelivered(void *base)
{
	node_t *node = (node_t *) base;
	msg_id_t msgid;
	message_t *msg;
	queue_t *q;
 	
 	assert(node);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3, 
		"node:%d DELIVERED (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	// get the messageID
	assert(BIT_TEST(node->data.mask, DATA_MASK_ID));
	msgid = node->data.id;
	assert(msgid >= 0);

	logger(node->sysdata->logging, 2, "processDelivered.  Node:%d, msg_id:%d", node->handle, msgid);

	// find message in node->out_msg
	msg = node_findoutmsg(node, msgid);
	if (msg == NULL) {
		// didn't find the message that is being marked as delivered.
		assert(0);
	}
	else {

		if (BIT_TEST(msg->flags, FLAG_MSG_NOREPLY)) {
			// message is NOREPLY,

			logger(node->sysdata->logging, 2, "delivery(%d): Noreply.", msgid);
		
			// assert that message doesnt have source-node.
			assert(msg->source_node == NULL);
			
			// tell the queue that the node has finished processing a message.  This
			// will find the node, and remove it from the node_busy list.
			assert(msg->queue);
			assert(msg->target_node);
			queue_msg_done(msg->queue, msg->target_node);
						
			// then remove from the queue->msg_proc list
			assert(msg->queue);
			q = msg->queue;
			ll_remove(&q->msg_proc, msg);
			msg->queue = NULL;
			
			// set action to remove the message.
			assert(BIT_TEST(msg->flags, FLAG_MSG_ACTIVE));
			message_clear(msg);
			assert(node->sysdata->msg_used > 0);
			node->sysdata->msg_used --;
			assert(node->sysdata->msg_used >= 0);
			node->sysdata->msg_next = msg->id;

			// if there are more messages in the queue, then we need to deliver them.
			if (ll_count(&q->msg_pending) > 0) {
				logger(node->sysdata->logging, 2, "delivery(%d): setting delivery action.", msgid);
				queue_deliver(q);
			}
			else {
				logger(node->sysdata->logging, 2, "delivery(%d): no items to deliver.", msgid);
			}
		}
		else {
			// message is expecting a reply, so we need to tell the source that it was delivered.
			
			// mark the message as delivered.
			assert(BIT_TEST(msg->flags, FLAG_MSG_DELIVERED) == 0);
			BIT_SET(msg->flags, FLAG_MSG_DELIVERED);

			// send delivery message back to source.
			assert(msg->source_node);
			assert(msg->source_id >= 0);
			sendDelivered(msg->source_node, msg->source_id);

			// but we dont need to original payload anymore, so we can release that back into the bufpool.
			assert(msg->data);
			assert(node->sysdata);
			assert(node->sysdata->bufpool);
			expbuf_clear(msg->data);
			expbuf_pool_return(node->sysdata->bufpool, msg->data);
			msg->data = NULL;
		}
	}
}

void cmdConsuming(void *base)
{
	node_t *node = (node_t *) base;
 	
 	assert(node);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3, 
		"node:%d CONSUMING (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	assert(BIT_TEST(node->data.mask, DATA_MASK_QUEUEID));
	assert(BIT_TEST(node->data.mask, DATA_MASK_QUEUE));

	assert(node->sysdata);
	assert(node->sysdata->queues);
	queue_set_id(node->sysdata->queues, expbuf_string(&node->data.queue), node->data.qid);
}


void command_init(risp_t *risp)
{
  assert(risp);
	risp_add_invalid(risp, &cmdInvalid);
	risp_add_command(risp, RQ_CMD_CLEAR,        &cmdClear);
	risp_add_command(risp, RQ_CMD_PING,         &cmdPing);
	risp_add_command(risp, RQ_CMD_PONG,         &cmdPong);
	risp_add_command(risp, RQ_CMD_REQUEST,      &cmdRequest);
	risp_add_command(risp, RQ_CMD_REPLY,        &cmdReply);
	risp_add_command(risp, RQ_CMD_DELIVERED,    &cmdDelivered);
	risp_add_command(risp, RQ_CMD_BROADCAST,    &cmdBroadcast);
	risp_add_command(risp, RQ_CMD_NOREPLY,      &cmdNoReply);
	risp_add_command(risp, RQ_CMD_CONSUME,      &cmdConsume);
	risp_add_command(risp, RQ_CMD_CANCEL_QUEUE, &cmdCancelQueue);
	risp_add_command(risp, RQ_CMD_CONSUMING,    &cmdConsuming);
	risp_add_command(risp, RQ_CMD_CLOSING,      &cmdClosing);
	risp_add_command(risp, RQ_CMD_EXCLUSIVE,    &cmdExclusive);
	risp_add_command(risp, RQ_CMD_QUEUEID,      &cmdQueueID);
	risp_add_command(risp, RQ_CMD_ID,           &cmdId);
	risp_add_command(risp, RQ_CMD_TIMEOUT,      &cmdTimeout);
	risp_add_command(risp, RQ_CMD_MAX,          &cmdMax);
	risp_add_command(risp, RQ_CMD_PRIORITY,     &cmdPriority);
	risp_add_command(risp, RQ_CMD_QUEUE,        &cmdQueue);
	risp_add_command(risp, RQ_CMD_PAYLOAD,      &cmdPayload);
}

