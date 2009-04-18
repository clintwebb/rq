// send.c

#include "send.h"
#include "queue.h"

#include <assert.h>
#include <errno.h>
#include <rispbuf.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// 
void sendConsumeReply(node_t *node, char *queue, int qid)
{
	assert(node != NULL);
	assert(queue != NULL);
	assert(qid > 0 && qid <= 0xffff);
	assert(node->build);
	assert(node->build->length == 0);

	// add the commands to the out queue.
	addCmd(node->build, RQ_CMD_CLEAR);
	addCmdInt(node->build, RQ_CMD_QUEUEID, qid);
	addCmdShortStr(node->build, RQ_CMD_QUEUE, strlen(queue), queue);
	addCmd(node->build, RQ_CMD_EXECUTE);

	node_write_now(node, node->build->length, node->build->data);
	expbuf_clear(node->build);
}

//-----------------------------------------------------------------------------
// Send a message to the node.  
void sendMessage(node_t *node, message_t *msg)
{
	queue_t *q;
	
	assert(node != NULL);
	assert(msg != NULL);
	assert(node->sysdata == msg->sysdata);
	assert(node->build != NULL);
	assert(node->build->length == 0);

	assert(msg->data);
	assert(msg->queue);

	q = msg->queue;
	assert(q->qid > 0);

	if (node->sysdata->verbose > 1) printf("sendMessage.  Node:%d, msg_id:%d\n", node->handle, msg->id);


	// add the commands to the out queue.
	addCmd(node->build, RQ_CMD_CLEAR);

	if (BIT_TEST(msg->flags, FLAG_MSG_BROADCAST)) {
		// We are sending a broadcast message
		assert(BIT_TEST(msg->flags, FLAG_MSG_NOREPLY));
		assert(msg->id == 0);
		assert(msg->target_node == NULL);

		addCmd(node->build, RQ_CMD_BROADCAST);
		addCmd(node->build, RQ_CMD_NOREPLY);
	}
	else {
		assert(msg->target_node);

		if (BIT_TEST(msg->flags, FLAG_MSG_NOREPLY)) 
			addCmd(node->build, RQ_CMD_NOREPLY);
		
		addCmd(node->build, RQ_CMD_REQUEST);

		assert(msg->id > 0);
		addCmdLargeInt(node->build, RQ_CMD_ID, msg->id);
	}

	addCmdInt(node->build, RQ_CMD_QUEUEID, q->qid);
	addCmdLargeStr(node->build, RQ_CMD_PAYLOAD, msg->data->length, msg->data->data);
	addCmd(node->build, RQ_CMD_EXECUTE);

	node_write_now(node, node->build->length, node->build->data);
	expbuf_clear(node->build);
}



void sendUndelivered(node_t *node, message_t *msg)
{
	assert(node != NULL);
	assert(msg != NULL);
	assert(node->build);
	assert(node->build->length == 0);

	// add the commands to the out queue.
	addCmd(node->build, RQ_CMD_CLEAR);
	addCmd(node->build, RQ_CMD_UNDELIVERED);
	
	assert(0);
// 	addCmdInt(node->build, RQ_CMD_QUEUEID, qid);
// 	addCmdShortStr(node->build, RQ_CMD_QUEUE, strlen(queue), queue);
	addCmd(node->build, RQ_CMD_EXECUTE);

	node_write_now(node, node->build->length, node->build->data);
	expbuf_clear(node->build);
}


//-----------------------------------------------------------------------------
// send a message to the node stating that the server is closing, and that the
// node needs to attempt to connect to the secondary controller.
void sendClosing(node_t *node)
{
	assert(node != NULL);
	assert(node->build);
	assert(node->build->length == 0);

	// add the commands to the out queue.
	addCmd(node->build, RQ_CMD_CLEAR);
	addCmd(node->build, RQ_CMD_CLOSING);
	addCmd(node->build, RQ_CMD_EXECUTE);
	
	node_write_now(node, node->build->length, node->build->data);
	expbuf_clear(node->build);
}


//-----------------------------------------------------------------------------
// The node is a controller, and we are making a consume request for a new
// queue that another node is consuming.
void sendConsume(node_t *node, char *queue, short int max, unsigned char priority)
{
	assert(node);
	assert(queue);
	assert(max >= 0);

	assert(BIT_TEST(node->flags, FLAG_NODE_CONTROLLER));

	// if we dont yet have a 'build' buffer then we will get one.
	if (node->build == NULL) {
		assert(node->sysdata != NULL);
		assert(node->sysdata->bufpool != NULL);
		node->build = expbuf_pool_new(node->sysdata->bufpool, 64);
	}
	assert(node->build != NULL);

	// add the commands to the out queue.
	assert(node->build->length == 0);
	addCmd(node->build, RQ_CMD_CLEAR);
	addCmdShortStr(node->build, RQ_CMD_QUEUE, strlen(queue), queue);
	addCmdInt(node->build, RQ_CMD_MAX, max);
	addCmdShortInt(node->build, RQ_CMD_PRIORITY, priority);
	addCmd(node->build, RQ_CMD_EXECUTE);

	assert(node->build->length > 0);
	assert(node->build->length <= node->build->max);
	assert(node->build->data != NULL);
	node_write_now(node, node->build->length, node->build->data);
	expbuf_clear(node->build);
}

