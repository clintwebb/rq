// process.c

#include "actions.h"
#include "process.h"
#include "send.h"
#include "queue.h"
#include "message.h"

#include <assert.h>
#include <evactions.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

//-----------------------------------------------------------------------------
// A request has been received for a queue.  We need take it and pass it to a
// node that can handle the request.
void processRequest(node_t *node)
{
	message_t *msg;
	char *qname;
	queue_id_t qid;
	queue_t *q, *tmp;
	
	
	assert(node);
	assert(node->sysdata);
	assert(node->sysdata->msgpool);
	assert(node->sysdata->queues);

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
		if (BIT_TEST(node->data.mask, DATA_MASK_ID)) {
			message_set_origid(msg, node->data.id);
		}
		
		// find the q object for this queue.
		qname = NULL;
		qid = 0;
		if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {
			qname = expbuf_string(&node->data.queue);
			q = queue_get_name(node->sysdata->queues, qname);
		}
		else if (BIT_TEST(node->data.mask, DATA_MASK_QUEUEID)) {
			qid = node->data.qid;
			q = queue_get_id(node->sysdata->queues, qid);
		}
		assert(qid > 0 || qname);
	
		if (q == NULL) {
			// we dont have a queue, so we will need to create one.
			assert(qid == 0);
	
			q = (queue_t *) malloc(sizeof(queue_t));
			queue_init(q);
			if (node->sysdata->queues) {
				tmp = node->sysdata->queues;
				assert(tmp->qid > 0);
				q->qid = tmp->qid + 1;
				tmp->prev = q;
				q->next = tmp;
			}
			else {
				assert(q->next == NULL);
				q->qid = 1;
			}
			node->sysdata->queues = q;
			assert(q->prev == NULL);			
		}
		assert(q);
		
		// add the message to the queue.
		queue_addmsg(q, msg);
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
 	node_queue_t *nq;
	
	assert(node);
	assert(node->sysdata);
	assert(node->sysdata->verbose >= 0);
	assert(BIT_TEST(node->data.flags, DATA_FLAG_CONSUME));
	
	// make sure that we have the minimum information that we need.
	if (BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {

		if (node->sysdata->verbose)
			printf("Processing QUEUE request from node:%d\n", node->handle);

		assert(BIT_TEST(node->data.mask, DATA_MASK_QUEUE));
		assert(node->data.queue.length > 0);
	
		// check to see if we already have a queue with this name, in our list.		
		assert(q == NULL);
		if (node->sysdata->queues)
			q = queue_get_name(node->sysdata->queues, expbuf_string(&node->data.queue));
		
		if (q == NULL) {
			// we didn't find the queue...
			printf("Didn't find queue '%s', creating new entry.\n", expbuf_string(&node->data.queue));
			q = queue_create(node->sysdata, expbuf_string(&node->data.queue));
		}
	
	
		// at this point we have a queue object.  We dont yet know if this
		// consumption request can continue, because we haven't looked at the
		// queue options.
		assert(q != NULL);


		// We need to create a node_queue_t object to hold the node details in it.
		nq = (node_queue_t *) malloc(sizeof(node_queue_t));
		nq->node = node;
		nq->max = 0;
		nq->priority = 0;
		nq->waiting = 0;
		nq->next = NULL;
		nq->prev = NULL;

		if (BIT_TEST(node->data.mask, DATA_MASK_MAX)) nq->max = node->data.max;
		if (BIT_TEST(node->data.mask, DATA_MASK_PRIORITY)) nq->priority = node->data.priority;

		// check to see if the current queue settings are for it to be
		// exclusive.   If so, then we will need to add this node to the waiting
		// list.
		if (BIT_TEST(q->flags, QUEUE_FLAG_EXCLUSIVE) || q->ready_head || q->busy) {
			// The queue is already in exclusive mode (or already has members that are not exlusive).  So this node would need to
			// be added to the waiting list.

			assert(nq->prev == NULL);
			nq->next = q->waitinglist;
			if (q->waitinglist) {
				assert(q->waitinglist->prev == NULL);
				q->waitinglist->prev = nq;
			}
			q->waitinglist = nq;

			if (node->sysdata->verbose > 1)
				printf("processConsume - Defered, queue already consumed exclusively.\n");
		}
		else {
			
			// if the consume request was for an EXCLUSIVE queue (and since we got
			// this far it means that no other nodes are consuming this queue yet),
			// then we mark it as exclusive.
			if (BIT_TEST(node->data.flags, DATA_FLAG_EXCLUSIVE)) {
				assert(q->ready_head == NULL);
				assert(q->ready_tail == NULL);
				assert(q->busy == NULL);
				q->flags |= QUEUE_FLAG_EXCLUSIVE;
				if (node->sysdata->verbose)
					printf("Consuming Queue '%s' in EXCLUSIVE mode.\n", expbuf_string(&node->data.queue));
			}

			// add the node to the appropriate list.
			assert(nq->prev == NULL);
			nq->next = q->ready_head;
			if (q->ready_tail == NULL) q->ready_tail = nq;
			if (q->ready_head) {
				assert(q->ready_head->prev == NULL);
				q->ready_head->prev = nq;
			}
			q->ready_head = nq;
			
			
			// send reply back to the node.
			sendConsumeReply(node, q->name, q->qid);
			
			if (node->sysdata->verbose)
				printf("Consuming queue: qid=%d\n", q->qid);
		}

	
		// need to check the queue to see if there are messages pending.  If there are, then send some to this node.
		if (q->msghead != NULL) {
	
			// not done.
			assert(0);
	
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
	assert(node != NULL);
	assert(0);
}


void processReceived(node_t *node)
{
	assert(node != NULL);
	assert(0);
}


