// process.c

#include "logging.h"
#include "message.h"
#include "process.h"
#include "queue.h"
#include "send.h"
#include "stats.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

//-----------------------------------------------------------------------------
// A request has been received for a queue.  We need take it and pass it to a
// node that can handle the request.
void processRequest(node_t *node)
{
	message_t *msg;
	queue_t *q;
	stats_t *stats;

	assert(node);
	assert(node->sysdata);
	assert(node->sysdata->msgpool);
	assert(node->sysdata->queues);
	assert(node->sysdata->bufpool);

	// This function should not be called for a Broadcast message.
	assert(BIT_TEST(node->data.flags, DATA_FLAG_BROADCAST) == 0);

	// make sure we have the required data. At least payload, and a queueid or queue.
	if (BIT_TEST(node->data.mask, DATA_MASK_PAYLOAD) && (BIT_TEST(node->data.mask, DATA_MASK_QUEUE) || BIT_TEST(node->data.mask, DATA_MASK_QUEUEID))) {

		// create the message object to hold the data.
		msg = mempool_get(node->sysdata->msgpool, sizeof(message_t));
		if (msg == NULL) {
			msg = (message_t *) malloc(sizeof(message_t));
			mempool_assign(node->sysdata->msgpool, msg, sizeof(message_t));
		}
		message_init(msg, node->sysdata);

		// assign the message payload.
		// TODO: Need to improve this so that we can detach a buffer and assign
		//       it, without having to do a copy.
		msg->data = expbuf_pool_new(node->sysdata->bufpool, node->data.payload.length);
		expbuf_set(msg->data, node->data.payload.data, node->data.payload.length);
		
		// if message is NOREPLY, then we dont need some bits.  However, we will need to send a DELIVERED.
		if (BIT_TEST(node->data.flags, DATA_FLAG_NOREPLY)) {
			BIT_SET(msg->flags, FLAG_MSG_NOREPLY);
			assert(msg->source_node == NULL);
			assert(msg->source_id == 0);

			if (BIT_TEST(node->data.mask, DATA_MASK_ID)) {
				sendDelivered(node, node->data.id);
			}
		}
		else {
			assert(BIT_TEST(msg->flags, FLAG_MSG_NOREPLY) == 0);
		
			// make a note in the msg object, the source node.
			message_set_orignode(msg, node);
		
			// if a messageid has been supplied, use that for the node_side.
			if (BIT_TEST(node->data.mask, DATA_MASK_ID)) {
				message_set_origid(msg, node->data.id);
			}

			// we are expecting a reply, so we will need to add this message to the node's msg list.
			ll_push_head(&node->in_msg, msg);
			assert(msg->source_node);
		}
		
		if (BIT_TEST(node->data.mask, DATA_MASK_TIMEOUT)) {
			// set the timeout... action should be fired.
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

void processReply(node_t *node)
{
	assert(0);
}


//-----------------------------------------------------------------------------
// When a node indicates that it wants to consume a queue,the node needs to be
// added to the queue list.  If this is the first time this queue is being
// consumed, then we need to create an action so that it can be consumed on
// other servers.
void processConsume(node_t *node)
{
 	queue_t *q=NULL;
 	int max;
 	int priority;
 	unsigned int flags;
	
	assert(node);
	assert(node->sysdata);
	assert(node->sysdata->verbose >= 0);
	assert(BIT_TEST(node->data.flags, DATA_FLAG_CONSUME));
	
	// make sure that we have the minimum information that we need.
	if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {

		logger(node->sysdata->logging, 2, 
			"Processing QUEUE request from node:%d", node->handle);

		assert(BIT_TEST(node->data.mask, DATA_MASK_QUEUE));
		assert(node->data.queue.length > 0);
	
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
		flags = 0;

		if (BIT_TEST(node->data.mask, DATA_MASK_MAX))
			max = node->data.max;
			
		if (BIT_TEST(node->data.mask, DATA_MASK_PRIORITY))
			priority = node->data.priority;
			
		if (BIT_TEST(node->data.flags, DATA_FLAG_EXCLUSIVE))
			BIT_SET(flags, QUEUE_FLAG_EXCLUSIVE);
		
		if (queue_add_node(q, node, max, priority, flags) > 0) {
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

void processCancelQueue(node_t *node)
{
	assert(0);
}

void processClosing(node_t *node)
{
	assert(node);

	// if the node is a regular consumer then we cancel all the queues that do
	// not have pending requests for this node.
	queue_cancel_node(node);

	// if this is a regular consumer, we would close the socket if there are no replies that are ready to go down the pipe.
	if (ll_count(&node->out_msg) > 0) {
		assert(0);
	}

	// mark the node as closing so that as soon as all the messages have completed, the node can be shutdown.
	assert(BIT_TEST(node->flags, FLAG_NODE_CLOSING) == 0);
	BIT_SET(node->flags, FLAG_NODE_CLOSING);

	// if this node is a controller, then we may need to do something to handle it.  Although it is likely that nothign would be done until the connection is actually lost.
	if (BIT_TEST(node->flags, FLAG_NODE_CONTROLLER)) {
		// what do we need to do special to handle the closing of a controller node?
		// nothing really... that would be done when the connection is actually closed.
	}
}

void processServerFull(node_t *node)
{
	// we've attempted to communicate with another controller, but it is telling us that it is full... what should we do?  Wait a while and try to connect again?   How do we manage this waiting state?
	assert(0);
}


//-----------------------------------------------------------------------------
// we would only get these from other controllers after we have made CONSUME
// requests.   We need to go thru the queue list and find the one specified and
// give it the qid.
void processQueueLink(node_t *node)
{
	assert(node);
	assert(BIT_TEST(node->data.mask, DATA_MASK_QUEUEID));
	assert(BIT_TEST(node->data.mask, DATA_MASK_QUEUE));

	assert(node->sysdata);
	assert(node->sysdata->queues);
	queue_set_id(node->sysdata->queues, expbuf_string(&node->data.queue), node->data.qid);
}


//-----------------------------------------------------------------------------
// We've received a broadcast message that is not a request, and doesn't
// require a reply...
void processBroadcast(node_t *node)
{
	queue_t *q = NULL;
	message_t *msg;
	
	assert(node != NULL);
	assert(BIT_TEST(node->data.flags, DATA_FLAG_BROADCAST));
	assert(BIT_TEST(node->data.flags, DATA_FLAG_NOREPLY));
	assert(BIT_TEST(node->data.flags, DATA_FLAG_REQUEST) == 0);

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
		assert(node->sysdata);
		assert(node->sysdata->msgpool);
		msg = mempool_get(node->sysdata->msgpool, sizeof(message_t));
		if (msg == NULL) {
			msg = (message_t *) malloc(sizeof(message_t));
			message_init(msg, node->sysdata);
			mempool_assign(node->sysdata->msgpool, msg, sizeof(message_t));
		}
		assert(msg);

		// now that we have a message structure completely filled out with the
		// data from the node, then we need to add it to an action and fire it.
		assert(0);
	}
	else {
		// we didn't have a queue name, or a queue id.   We need to handle this gracefully.
		assert(0);
	}
}


void processDelivered(node_t *node)
{
	msg_id_t msgid;
	message_t *msg;
	queue_t *q;
	
	assert(node != NULL);

	// get the messageID
	assert(BIT_TEST(node->data.mask, DATA_MASK_ID));
	msgid = node->data.id;
	assert(msgid > 0);

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
			
			// then remove from node->out_msg
			assert(msg->target_node);
			ll_remove(&node->out_msg, msg, NULL);
			msg->target_node = NULL;
			
			// then remove from the queue->msg_proc list
			assert(msg->queue);
			q = msg->queue;
			ll_remove(&q->msg_proc, msg, NULL);
			msg->queue = NULL;
			
			// set action to remove the message.
			message_delete(msg);

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
			// message is expecting a reply.

			// mark the message as delivered.
			assert(0);	
		}
	}	
}


void processReceived(node_t *node)
{
	assert(node != NULL);
	assert(0);
}


