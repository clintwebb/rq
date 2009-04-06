// commands.c


#include "commands.h"
#include "node.h"
#include "process.h"

#include <assert.h>
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
	assert(node->sysdata->verbose >= 0);
	
	if (node->sysdata->verbose > 0)
		printf("Received invalid (%d)): [%d, %d, %d]\n", len, cast[0], cast[1], cast[2]);
		
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
 	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	data_clear(&node->data);
	if (node->sysdata->verbose > 1) printf("node:%d CLEAR\n", node->handle);
}


// This callback function is called when the CMD_EXECUTE command is received.  
// It should look at the data received so far, and figure out what operation 
// needs to be done on that data.  Since this is a simulation, and our 
// protocol doesn't really do anything useful, we will not really do much in 
// this example.   
void cmdExecute(void *base) 
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	
 	assert(node->sysdata->stats != NULL);
	if (node->sysdata->verbose > 1)
		printf("node:%d EXECUTE (flags:%X, mask:%X)\n", node->handle, node->data.flags, node->data.mask);

	if (BIT_TEST(node->data.flags, DATA_FLAG_REQUEST)) {
		processRequest(node);
	}
	else if (BIT_TEST(node->data.flags, DATA_FLAG_REPLY)) {
		processReply(node);
	}
	else if (BIT_TEST(node->data.flags, DATA_FLAG_BROADCAST) && BIT_TEST(node->data.flags, DATA_FLAG_NOREPLY)) {
		// if we receive a BROADCAST, but not a REQUEST, then it is not a requesst, and should also not expect a reply.
		assert(! BIT_TEST(node->data.flags, DATA_FLAG_REQUEST));
// 		printf("node:%d Processing broadcast message.\n", node->handle);
		processBroadcast(node);
	}
	else if (BIT_TEST(node->data.flags, DATA_FLAG_CONSUME)) {
		processConsume(node);
	}
	else if (BIT_TEST(node->data.flags, DATA_FLAG_CANCEL_QUEUE)) {
		processCancelQueue(node);
	}
	else if (BIT_TEST(node->data.flags, DATA_FLAG_CLOSING)) {
		processClosing(node);
	}
	else if (BIT_TEST(node->data.flags, DATA_FLAG_SERVER_FULL)) {
		processServerFull(node);
	}
	else if (BIT_TEST(node->data.flags, DATA_FLAG_CONTROLLER)) {
		processController(node);
	}
	else if (BIT_TEST(node->data.mask, DATA_MASK_QUEUEID) && BIT_TEST(node->data.mask, DATA_MASK_QUEUE)) {
		processQueueLink(node);
	}
	else {
		// while primary development is occuring will leave this assert in to catch some simple programming issues.  However, once this goes to production, this will need to be removed, because we want to ignore actions taht we dont understand.  This could mean that a new protocol has a command we dont know how to understand.  So we ignore it.
		
		if (node->sysdata->verbose)
			printf("node:%d EXECUTE failed (flags:%x, mask:%x)\n",
				node->handle, node->data.flags, node->data.mask);
		
		assert(0);
	}
}

void cmdRequest(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);

 	// ensure only the flags that are valid.
 	node->data.flags &= (DATA_FLAG_BROADCAST | DATA_FLAG_NOREPLY);
 	// set our specific flag.
	node->data.flags |= DATA_FLAG_REQUEST;
	// ensure only the legal data is used.
	node->data.mask &= (DATA_MASK_ID | DATA_MASK_TIMEOUT | DATA_MASK_QUEUEID | DATA_MASK_QUEUE | DATA_MASK_PAYLOAD);

	if (node->sysdata->verbose > 1)
		printf("node:%d REQUEST (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}


void cmdReply(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);

	// a REPLY command should not have any other flags.
	node->data.flags = DATA_FLAG_REPLY;
	
	// ensure only the legal data is used.
	node->data.mask &= (DATA_MASK_ID | DATA_MASK_PAYLOAD);

	if (node->sysdata->verbose > 1)
		printf("node:%d REPLY (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}

void cmdBroadcast(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);

 	// ensure only the flags that are valid.
 	node->data.flags &= (DATA_FLAG_REQUEST | DATA_FLAG_NOREPLY);
 	// set our specific flag.
	node->data.flags |= DATA_FLAG_BROADCAST;
	node->data.flags |= DATA_FLAG_NOREPLY;		// broadcast implies noreply.
	// ensure only the legal data is used.
	node->data.mask &= (DATA_MASK_ID | DATA_MASK_TIMEOUT | DATA_MASK_QUEUEID | DATA_MASK_QUEUE | DATA_MASK_PAYLOAD);

	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d BROADCAST (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}

void cmdNoReply(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);

 	// ensure only the flags that are valid.
 	node->data.flags &= (DATA_FLAG_REQUEST | DATA_FLAG_BROADCAST);
 	// set our specific flag.
	node->data.flags |= DATA_FLAG_NOREPLY;
	// ensure only the legal data is used.
	node->data.mask &= (DATA_MASK_ID | DATA_MASK_TIMEOUT | DATA_MASK_QUEUEID | DATA_MASK_QUEUE | DATA_MASK_PAYLOAD);

	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d NOREPLY (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}


void cmdExclusive(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);

 	// ensure only the flags that are valid.
 	node->data.flags &= (DATA_FLAG_CONSUME);
 	// set our specific flag.
	node->data.flags |= DATA_FLAG_EXCLUSIVE;
	// ensure only the legal data is used.
	node->data.mask &= ( DATA_MASK_QUEUE | DATA_MASK_MAX | DATA_MASK_PRIORITY);

	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d EXCLUSIVE (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}


void cmdConsume(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);

 	// ensure only the flags that are valid.
 	node->data.flags &= (DATA_FLAG_EXCLUSIVE);
 	
	// a CONSUME command should not have any other flags.
	node->data.flags |= DATA_FLAG_CONSUME;
	
	// ensure only the legal data is used.
	node->data.mask &= (DATA_MASK_QUEUE | DATA_MASK_MAX | DATA_MASK_PRIORITY);

	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d CONSUME (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}

void cmdCancelQueue(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);

	// a CONSUME command should not have any other flags.
	node->data.flags = DATA_FLAG_CANCEL_QUEUE;
	
	// ensure only the legal data is used.
	node->data.mask &= (DATA_MASK_QUEUE | DATA_MASK_QUEUEID);

	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d CANCEL QUEUE (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}

void cmdId(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;
 	assert(node != NULL);
 	assert(value > 0);
	node->data.id = value;
	node->data.mask |= (DATA_MASK_ID);
	
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d ID (%d)\n", node->handle, value);
}

void cmdTimeout(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;
 	assert(node != NULL);
 	assert(value >= 0 && value <= 0xffff);
	node->data.timeout = value;
	node->data.mask |= (DATA_MASK_TIMEOUT);
	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d TIMEOUT (%d)\n", node->handle, value);
}

void cmdMax(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;
 	assert(node != NULL);
 	assert(value >= 0 && value <= 0xffff);
	node->data.max = value;
	node->data.mask |= (DATA_MASK_MAX);

	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d MAX (%d)\n", node->handle, value);
}

void cmdPriority(void *base, risp_int_t value)
{
	node_t *node= (node_t *) base;
 	assert(node != NULL);
 	assert(value >= 0 && value <= 0xffff);
	node->data.mask |= (DATA_MASK_PRIORITY);
	node->data.priority = value;

	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d PRIORITY (%d)\n", node->handle, value);
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
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d QUEUE (%s)\n", node->handle, expbuf_string(&node->data.queue));
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
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d PAYLOAD (len:%d, flags:%x, mask:%x)\n",
			node->handle,
			length,
			node->data.flags,
			node->data.mask);
}



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
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d RECEIVED (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}

void cmdDelivered(void *base)
{
	node_t *node = (node_t *) base;
 	assert(node != NULL);

 	// ensure only the flags that are valid.
 	node->data.flags &= (DATA_FLAG_DELIVERED);
 	// set our specific flag.
	node->data.flags |= DATA_FLAG_DELIVERED;
	// ensure only the legal data is used.
	node->data.mask &= (DATA_MASK_ID | DATA_MASK_QUEUEID | DATA_MASK_QUEUE);

	assert(node->sysdata != NULL);
	assert(node->sysdata->verbose >= 0);
	if (node->sysdata->verbose > 1)
		printf("node:%d DELIVERED (flags:%x, mask:%x)\n", node->handle, node->data.flags, node->data.mask);
}



