// commands.c


#include "commands.h"
#include "node.h"
#include "process.h"
#include "send.h"

#include <assert.h>
#include <evlogging.h>
#include <risp.h>
#include <rq.h>
#include <stdio.h>


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




void cmdRequest(void *base)
{
	node_t *node = (node_t *) base;

 	assert(node != NULL);
 	assert(node->handle >= 0);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d REQUEST (flags:%x, mask:%x)", node->handle, node->data.flags, node->data.mask);

	processRequest(node);	
}


void cmdReply(void *base)
{
	node_t *node = (node_t *) base;
 	
 	assert(node != NULL);
	assert(node->handle >= 0);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3, 
		"node:%d REPLY (flags:%x, mask:%x)", node->handle, node->data.flags, node->data.mask);

	processReply(node);
}

void cmdBroadcast(void *base)
{
	node_t *node = (node_t *) base;
	
 	assert(node);
	assert(node->handle >= 0);
	assert(node->sysdata);
	logger(node->sysdata->logging, 3,
		"node:%d BROADCAST (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	processBroadcast(node);
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

	processClosing(node);
}



void cmdConsume(void *base)
{
	node_t *node = (node_t *) base;
 	
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3,
		"node:%d CONSUME (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	processConsume(node);		
}

void cmdCancelQueue(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);

	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3,
		"node:%d CANCEL QUEUE (flags:%x, mask:%x)", node->handle, node->data.flags, node->data.mask);

	processCancelQueue(node);
}

void cmdId(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;

 	assert(node);
 	assert(value > 0);
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

void cmdPayload(void *base, risp_length_t length, risp_char_t *data)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
 	assert(length > 0);
 	assert(data != NULL);
 	
 	expbuf_set(&node->data.payload, data, length);
 	BIT_SET(node->data.mask, DATA_MASK_PAYLOAD);
	
	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3,
		"node:%d PAYLOAD (len:%d, flags:%x, mask:%x)",
			node->handle,
			length,
			node->data.flags,
			node->data.mask);
}


#if (0)
void cmdReceived(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);

 	// ensure only the flags that are valid.
 	node->data.flags &= (DATA_FLAG_RECEIVED);
 	// set our specific flag.
	node->data.flags |= DATA_FLAG_RECEIVED;
	// ensure only the legal data is used.
	node->data.mask &= (DATA_MASK_ID | DATA_MASK_QUEUEID | DATA_MASK_QUEUE);

	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3, 
		"node:%d RECEIVED (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);
}
#endif


void cmdDelivered(void *base)
{
	node_t *node = (node_t *) base;
 	
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3, 
		"node:%d DELIVERED (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	processDelivered(node);
}

void cmdConsuming(void *base)
{
	node_t *node = (node_t *) base;
 	
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	logger(node->sysdata->logging, 3, 
		"node:%d CONSUMING (flags:%x, mask:%x)",
		node->handle, node->data.flags, node->data.mask);

	processQueueLink(node);
}


void command_init(risp_t *risp)
{
  assert(risp);
	risp_add_invalid(risp, &cmdInvalid);
	risp_add_command(risp, RQ_CMD_CLEAR,        &cmdClear);
// 	risp_add_command(risp, RQ_CMD_EXECUTE,      &cmdExecute);
	risp_add_command(risp, RQ_CMD_PING,         &cmdPing);
	risp_add_command(risp, RQ_CMD_PONG,         &cmdPong);
	risp_add_command(risp, RQ_CMD_REQUEST,      &cmdRequest);
	risp_add_command(risp, RQ_CMD_REPLY,        &cmdReply);
// 	risp_add_command(risp, RQ_CMD_RECEIVED,     &cmdReceived);
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

