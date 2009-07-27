// send.c

#include "send.h"
#include "queue.h"

#include <assert.h>
#include <errno.h>
#include <evlogging.h>
#include <rispbuf.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// 
void sendConsumeReply(node_t *node, char *queue, int qid)
{
	expbuf_t *build;

	assert(node != NULL);
	assert(queue != NULL);
	assert(qid > 0 && qid <= 0xffff);

	assert(node->sysdata);
	assert(node->sysdata->build_buf);
	build = node->sysdata->build_buf;
	assert(build->length == 0);

	// add the commands to the out queue.
	addCmd(build, RQ_CMD_CLEAR);
	addCmdInt(build, RQ_CMD_QUEUEID, qid);
	addCmdShortStr(build, RQ_CMD_QUEUE, strlen(queue), queue);
	addCmd(build, RQ_CMD_CONSUMING);

	node_write_now(node, build->length, build->data);
	expbuf_clear(build);
}

//-----------------------------------------------------------------------------
// Send a message to the node.  
void sendMessage(node_t *node, message_t *msg)
{
	queue_t *q;
	expbuf_t *build;
	
	assert(node != NULL);
	assert(msg != NULL);
	assert(node->sysdata == msg->sysdata);

	assert(node->sysdata);
	assert(node->sysdata->build_buf);
	build = node->sysdata->build_buf;
	assert(build->length == 0);

	assert(msg->data);
	assert(msg->queue);

	q = msg->queue;
	assert(q->qid > 0);

	logger(node->sysdata->logging, 2, "sendMessage.  Node:%d, msg_id:%d", node->handle, msg->id);


	// add the commands to the out queue.
	addCmd(build, RQ_CMD_CLEAR);
	addCmdInt(build, RQ_CMD_QUEUEID, q->qid);
	addCmdLargeStr(build, RQ_CMD_PAYLOAD, msg->data->length, msg->data->data);


	if (BIT_TEST(msg->flags, FLAG_MSG_BROADCAST)) {
		// We are sending a broadcast message
		assert(msg->id == 0);
		assert(msg->target_node == NULL);
		addCmd(build, RQ_CMD_BROADCAST);
	}
	else {
		assert(msg->target_node);

		if (BIT_TEST(msg->flags, FLAG_MSG_NOREPLY)) 
			addCmd(build, RQ_CMD_NOREPLY);
		
		assert(msg->id > 0);
		addCmdLargeInt(build, RQ_CMD_ID, msg->id);
		addCmd(build, RQ_CMD_REQUEST);
	}

	node_write_now(node, build->length, build->data);
	expbuf_clear(build);
}


//-----------------------------------------------------------------------------

void sendDelivered(node_t *node, message_id_t msgid)
{
	expbuf_t *build;
	
	assert(node);
	assert(msgid > 0);

	assert(node->sysdata);
	assert(node->sysdata->build_buf);
	build = node->sysdata->build_buf;
	assert(build->length == 0);

	// add the commands to the out queue.
	addCmd(build, RQ_CMD_CLEAR);
	addCmdLargeInt(build, RQ_CMD_ID, (short int) msgid);
	addCmd(build, RQ_CMD_DELIVERED);

	node_write_now(node, build->length, build->data);
	expbuf_clear(build);

	logger(node->sysdata->logging, 2,
			"sendDelivered.  node=%d, msgid=%d",
			node->handle, msgid);
}


//-----------------------------------------------------------------------------
// Tell a node that a message was not delivered.
void sendUndelivered(node_t *node, message_id_t msgid)
{
	expbuf_t *build;
	
	assert(node);
	assert(msgid > 0);

	assert(node->sysdata);
	assert(node->sysdata->build_buf);
	build = node->sysdata->build_buf;
	assert(build->length == 0);

	// add the commands to the out queue.
	addCmd(build, RQ_CMD_CLEAR);
	addCmdLargeInt(build, RQ_CMD_ID, (short int) msgid);
	addCmd(build, RQ_CMD_UNDELIVERED);

	node_write_now(node, build->length, build->data);
	expbuf_clear(build);
	
	logger(node->sysdata->logging, 2,
			"sendUndelivered.  node=%d, msgid=%d",
			node->handle, msgid);
}






//-----------------------------------------------------------------------------
// send a message to the node stating that the server is closing, and that the
// node needs to attempt to connect to the secondary controller.
void sendClosing(node_t *node)
{
	expbuf_t *build;
	
	assert(node != NULL);
	
	assert(node->sysdata);
	assert(node->sysdata->build_buf);
	build = node->sysdata->build_buf;
	assert(build->length == 0);

	// add the commands to the out queue.
	addCmd(build, RQ_CMD_CLEAR);
	addCmd(build, RQ_CMD_CLOSING);
	
	node_write_now(node, build->length, build->data);
	expbuf_clear(build);
}


//-----------------------------------------------------------------------------
// The node is a controller, and we are making a consume request for a new
// queue that another node is consuming.
void sendConsume(node_t *node, char *queue, short int max, unsigned char priority, short int exclusive)
{
	expbuf_t *build;
	
	assert(node);
	assert(queue);
	assert(max >= 0);

	assert(node->sysdata);
	assert(node->sysdata->build_buf);
	build = node->sysdata->build_buf;
	assert(build->length == 0);

	assert(BIT_TEST(node->flags, FLAG_NODE_CONTROLLER));

	// add the commands to the out queue.
	addCmd(build, RQ_CMD_CLEAR);
	addCmdShortStr(build, RQ_CMD_QUEUE, strlen(queue), queue);
	addCmdInt(build, RQ_CMD_MAX, max);
	addCmdShortInt(build, RQ_CMD_PRIORITY, priority);
	if (exclusive != 0) { addCmd(build, RQ_CMD_EXCLUSIVE); }
	addCmd(build, RQ_CMD_CONSUME);

	node_write_now(node, build->length, build->data);
	expbuf_clear(build);
}



//-----------------------------------------------------------------------------
// Send the ping command.
void sendPing(node_t *node)
{
	expbuf_t *build;
	
	assert(node != NULL);

	assert(node->sysdata);
	assert(node->sysdata->build_buf);
	build = node->sysdata->build_buf;
	assert(build->length == 0);

	// add the commands to the out queue.
	addCmd(build, RQ_CMD_PING);
	
	node_write_now(node, build->length, build->data);
	expbuf_clear(build);
}

//-----------------------------------------------------------------------------
// Send the pong command.
void sendPong(node_t *node)
{
	expbuf_t *build;
	
	assert(node != NULL);
	assert(node->sysdata);
	assert(node->sysdata->build_buf);
	build = node->sysdata->build_buf;
	assert(build->length == 0);

	// add the commands to the out queue.
	addCmd(build, RQ_CMD_PONG);
	
	node_write_now(node, build->length, build->data);
	expbuf_clear(build);
}

