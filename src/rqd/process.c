// process.c

#include "process.h"
#include "send.h"
#include "queue.h"
#include "message.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

//-----------------------------------------------------------------------------
// A request has been received for a queue.  We need take it and pass it to a
// node that can handle the request.
void processRequest(node_t *node)
{
	message_t *msg;
	char *qname;
	queue_id_t qid;
	queue_t *q;
	
	
	assert(node);
	assert(node->sysdata);
	assert(node->sysdata->msgpool);

	// make sure we have the required data.
	if ((BIT_TEST(node->data.mask, DATA_MASK_QUEUE) || BIT_TEST(node->data.mask, DATA_MASK_QUEUEID))
				&& BIT_TEST(node->data.mask, DATA_MASK_PAYLOAD)) {

		// create the message object to hold the data.
		msg = mempool_get(node->sysdata->msgpool, sizeof(message_t));
		if (msg == NULL) {
			msg = (message_t *) malloc(sizeof(message_t));
			mempool_assign(node->sysdata->msgpool, msg, sizeof(message_t));
		}
		message_init(msg, node->sysdata);
		
		// make a note in the msg object, the source node.
		message_set_orignode(msg, node);
	
		// if a messageid has been supplied, use that for the node_side.
		assert(0);
		
		// find the queue object.
		// do we have a queue name, or a qid?

		// assert: test code... was pasted in, but hasn't been checked.
		assert(0);

		
		qname = NULL;
		qid = 0;
		if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {
			qname = expbuf_string(&node->data.queue);
			qid = queue_id(node->sysdata->queuelist, qname);
		}
		else if (BIT_TEST(node->data.mask, DATA_MASK_QUEUEID)) {
			qid = node->data.qid;
		}
		assert(qid > 0);
	
		// get the pointer to the queue structure based on the qid or the qname.
		assert(node->sysdata->queuelist != NULL);
		q = queue_get(node->sysdata->queuelist, qid);
		if (q == NULL) {
			// we dont have a queue.
			assert(qid == 0);
	
			// if we have a 'KEEP' setting, then we need to create the queue, and
			// add the message to it, so that it can be delivered when a consumer
			// signs on.
			q = queue_create(node->sysdata->queuelist, qname);
		}
		
		// add the message to the queue.
		assert(0);
	
		// create an action to process the messages on the queue (if action not already pending).
		assert(0);
	}
	else {
		// required data was not found.
		// need to return some sort of error
		assert(0);
	}
	assert(0);
}

void processReply(node_t *node)
{
	assert(0);
}


// a node has requested to consume a particular queue.
void processConsume(node_t *node)
{
	queue_id_t qid;
	
	assert(node != NULL);
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	assert(node->data.flags & DATA_FLAG_CONSUME);
	
	// make sure that we have the minimum information that we need.
	if (node->data.mask & DATA_MASK_QUEUE) {

		if (node->sysdata->verbose)
			printf("Processing QUEUE request from node:%d\n", node->handle);

		assert(node->sysdata->queuelist != NULL);
		qid = queue_consume((queue_list_t *)node->sysdata->queuelist, node);
		
		if (qid > 0) {
			// we have the queue-id, so we need to reply with it.
			sendConsumeReply(node, expbuf_string(&node->data.queue), qid);
	
			if (node->sysdata->verbose > 1)
				printf("processConsume - Done\n");
		}
		else {
			if (node->sysdata->verbose > 1)
				printf("processConsume - Defered, queue already consumed exclusively.\n");
		}
	}
}

void processCancelQueue(node_t *node)
{
	assert(0);
}

void processClosing(node_t *node)
{
	// if the node is a regular consumer then we cancel all the queues that do not have pending requests for this node.
	assert(0);
}

void processServerFull(node_t *node)
{
	// we've attempted to communicate with another controller, but it is telling us that it is full... what should we do?  Wait a while and try to connect again?   How do we manage this waiting state?
	assert(0);
}

void processQueueLink(node_t *node)
{
	// we would only get these from other controllers after we have made CONSUME requests.
	assert(0);
}

void processController(node_t *node)
{
	// the node is announcing that it is a controller.  We therefore need to
	// send it a CONSUME QUEUE request for every queue that we have in our list..
	assert(0);
}

//-----------------------------------------------------------------------------
// We've received a broadcast message that is not a request, and doesn't
// require a reply...
void processBroadcast(node_t *node)
{
	message_t *msg;
	queue_t *q;
	char *qname;
	int qid;
	int sent;
	int i;
	
	assert(node != NULL);
	assert(BIT_TEST(node->data.flags, DATA_FLAG_BROADCAST));
	assert(BIT_TEST(node->data.flags, DATA_FLAG_NOREPLY));
	assert(BIT_TEST(node->data.flags, DATA_FLAG_REQUEST) == 0);

	// do we have a queue name, or a qid?
	if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE) || BIT_TEST(node->data.mask, DATA_MASK_QUEUEID)) {

		qname = NULL;
		qid = 0;
		if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {
			qname = expbuf_string(&node->data.queue);
			qid = queue_id(node->sysdata->queuelist, qname);
		}
		else if (BIT_TEST(node->data.mask, DATA_MASK_QUEUEID)) {
			qid = node->data.qid;
		}
		assert(qid > 0);

		// get the pointer to the queue structure based on the qid or the qname.
		assert(node->sysdata->queuelist != NULL);
		q = queue_get(node->sysdata->queuelist, qid);
		if (q == NULL) {
			// we dont have a queue.
			assert(qid == 0);

			// if we have a 'KEEP' setting, then we need to create the queue, and
			// add the message to it, so that it can be delivered when a consumer
			// signs on.
			q = queue_create(node->sysdata->queuelist, qname);
		}
	
		// go thru the list of nodes for the queue.
// 	nodequeue_t **nodelist;
// 	int nodes;
		
/* doesnt work... fix

		assert((q->nodes == 0 && q->nodelist == NULL) || (q->nodes > 0 && q->nodelist != NULL));
		sent = 0;
		for (i=0; i<q->nodes; i++) {
			if (q->nodelist[i] != NULL) {

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


				// add message to the node.
				assert(0);

				// do we want to assign the message to the node?  Do we really need to do that?  Or should we just put the message in the nodes outgoing buffer?  There is no real reason to track it any further because there is no reply expected.
				
				assert(0);

				sent++;
			}
		}
		assert(0);

*/
	
		// now that we have a message structure completely filled out with the
		// data from the node, then we need to add it to an action and fire it.
		assert(0);

	
		// now that we have the queue squared away, we can add the message to it, and vice-versa.
// 		assert(q != NULL && msg != NULL);
// 		message_set_queue(msg, q);
// 		queue_addmsg(q, msg);

	}
	else {
		// we didn't have a queue name, or a queue id.   We need to handle this gracefully.
		assert(0);
	}
}


void processDelivered(node_t *node)
{
	assert(node != NULL);
	assert(0);
}


void processReceived(node_t *node)
{
	assert(node != NULL);
	assert(0);
}


