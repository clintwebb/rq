// node.c

#include "data.h"
#include "logging.h"
#include "node.h"
#include "queue.h"
#include "send.h"
#include "server.h"
#include "stats.h"

#include <assert.h>
#include <errno.h>
#include <event.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>



//-----------------------------------------------------------------------------
// used to initialise an invalid node structure.  The values currently in the
// structure are unknown.   We will assign a handle, because the only time we 
// ever need to initiate a newly created struct is when we have received a 
// socket, and the
void node_init(node_t *node, system_data_t *sysdata)
{
	assert(node != NULL);
	assert(sysdata != NULL);
	
	assert(sysdata->stats != NULL);
	assert(sysdata->risp != NULL);
	assert(sysdata->evbase != NULL);

	node->sysdata = sysdata;
	node->handle = INVALID_HANDLE;
	
	assert(sysdata->bufpool != NULL);
	
  assert(DEFAULT_BUFFSIZE > 0);
	node->waiting = expbuf_pool_new(sysdata->bufpool, DEFAULT_BUFFSIZE);
	node->out     = expbuf_pool_new(sysdata->bufpool, DEFAULT_BUFFSIZE);

	ll_init(&node->in_msg);
	ll_init(&node->out_msg);

	data_init(&node->data);
	
	node->flags = 0;
	
	assert(node->handle == INVALID_HANDLE);
	node->read_event = NULL;
	node->write_event = NULL;
	node->idle = 0;
	node->controller = NULL;
}


//-----------------------------------------------------------------------------
// prepare a node for de-allocation.  This means freeing buffers too.
void node_free(node_t *node)
{
	assert(node != NULL);
	system_data_t *sysdata;
	
	assert(node != NULL);
	assert(node->out != NULL);

	assert(node->sysdata != NULL);
	sysdata = node->sysdata;
	
	assert(sysdata->bufpool != NULL);
	assert(node->controller == NULL);

	node->flags = 0;
	
	assert(node->handle == INVALID_HANDLE);
	
	if (node->read_event)  {
		event_del(node->read_event);
		event_free(node->read_event);
		node->read_event = NULL;
	}

	assert(node->write_event == NULL);
	
	assert(node->out);
	expbuf_clear(node->out);
	expbuf_pool_return(sysdata->bufpool, node->out);
	node->out = NULL;
	
	assert(node->waiting);
	expbuf_clear(node->waiting);
	expbuf_pool_return(sysdata->bufpool, node->waiting);
	node->waiting = NULL;

	// make sure that all the messages for this node have been processed first.
	// And then free the memory that was used for the list.
	ll_free(&node->in_msg);
	ll_free(&node->out_msg);

	// make sure that this node has been removed from all consumer queues.
	if (node->sysdata->queues)
		queue_cancel_node(node);
	
	data_clear(&node->data);

	assert(node->sysdata != NULL);
	node->sysdata = NULL;
	
	data_free(&node->data);
}


//-----------------------------------------------------------------------------
// This function should be called when a node has been closed.  It should clean
// up queue entries, and any messages that were pending to go to the node.  It
// will also cancel any requests that were pending replies to the node.
static void node_closed(node_t *node)
{
	message_t *msg;
	system_data_t *sysdata;
	controller_t *ct;
	
	assert(node);
	assert(node->sysdata);
	assert(node->sysdata->queues);
	assert(node->out);

	// we've definately lost connection to the node, so we need to mark it by
	// clearing the handle.
	assert(node->handle == INVALID_HANDLE);

	if (node->controller) {
		assert(BIT_TEST(node->flags, FLAG_NODE_CONTROLLER));

		ct = node->controller;
		assert(ct);
		assert(BIT_TEST(ct->flags, FLAG_CONTROLLER_CONNECTING) == 0);

		// we need to remove the consume on the queues.
		queue_cancel_node(node);

		assert(ct->connect_event == NULL);
		ct->node = NULL;
		node->controller = NULL;

		controller_connect(ct);
	}
	else {
		// we need to remove the consume on the queues.
		assert(BIT_TEST(node->flags, FLAG_NODE_CONTROLLER) == 0);
		queue_cancel_node(node);
	}

	// we need to remove (return) any messages that this node was processing.
	while ((msg = ll_pop_head(&node->out_msg)) != NULL) {
		// we have a message that we need to deal with.

		// if this node was a destination for the message then we need to return
		// a FAILURE to the source.
		assert(0);

		// if this node was a source, we need to send a CANCEL to the destination.
		assert(0);

		// detatch message from node, and the node from the message and then set
		// an action to deal with the message.
		assert(0);
	}
	
	// we need to do something with the messages that this node is waiting replies for.,
	while ((msg = ll_pop_head(&node->in_msg)) != NULL) {
		// we have a message that we need to deal with.
		assert(0);
	}

	if (node->write_event) {
		event_del(node->write_event);
		event_free(node->write_event);
		node->write_event = NULL;
	}

	if (node->read_event) {
		event_del(node->read_event);
		event_free(node->read_event);
		node->read_event = NULL;
	}

	// need to indicate that node is not active.
	BIT_CLEAR(node->flags, FLAG_NODE_ACTIVE);
	
	// remove the node from the server object.
	sysdata = node->sysdata;
	assert(sysdata->nodelist);
	assert(ll_count(sysdata->nodelist) > 0);
	ll_remove(sysdata->nodelist, node, NULL);

	// free the resources used by the node.
	node_free(node);
	free(node);
}



//-----------------------------------------------------------------------------
// write out data to the socket.  If we have data waiting in the 'out' buffer,
// then we will just add this data to it.  If the out buffer is empty, then we
// will attempt to send this to the socket now.  If there is any data that
// wasn't sent, then it will be put in the out-buffer.
void node_write_now(node_t *node, int length, char *data)
{
	stats_t *stats;
	int res;
	
	assert(node);
	assert(length > 0);
	assert(data);

	assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));
	assert(node->sysdata);
	assert(node->sysdata->stats);
	assert(node->sysdata->bufpool);
	
	stats = node->sysdata->stats;

	assert(node->out);
	if (node->out && node->out->length > 0) {
		// we already have data in the outbuffer, so we will add this new data to
		// it, and wait for the event to fire.
		assert(node->write_event);
		expbuf_add(node->out, data, length);
	}
	else {
		// nothing already in the out-buffer, so we can try and send it now.

		assert(node->handle != INVALID_HANDLE);
		res = send(node->handle, data, length, 0);
		if (res > 0) {
			// we managed to send some, or maybe all....
			assert(res <= length);
			stats->out_bytes += res;
			if (res < length) {
				// not everything was sent, so we need to put the remainder in the 'out' queue.
				expbuf_add(node->out, &data[res], length-res);
				assert(node->out->length > 0);
			
				// we have ended up with data in the out-buffer, so we need to set the
				// event so that we can be notified when it is safe to send more.
				assert(node->write_event == NULL);
				assert(node->sysdata->evbase);
				node->write_event = event_new(node->sysdata->evbase, node->handle, EV_WRITE | EV_PERSIST, node_write_handler, (void *)node);
				event_add(node->write_event, 0);
			}
		}
		else if (res == 0) {
			logger(node->sysdata->logging, 2, 
				"Node[%d] closed while writing.", node->handle);
			assert(node->out);
			node->handle = INVALID_HANDLE;
			node_closed(node);
			assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
		}
		else {
			assert(res == -1);
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				logger(node->sysdata->logging, 2, 
					"Node[%d] closed while writing - because of error: %d", node->handle, errno);
				close(node->handle);
				node->handle = INVALID_HANDLE;
				node_closed(node);
				assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
			}
		}
	}
}



//-----------------------------------------------------------------------------
// this function is called when we have received data on our node socket.
void node_read_handler(int hid, short flags, void *data)
{
	node_t *node = (node_t *) data;
	int res, empty;
	stats_t *stats;
	expbuf_t *in;

	assert(hid >= 0);
	assert(node);
	assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));
	assert(node->sysdata);
	assert(node->handle == hid);
	assert(node->sysdata->stats);
	assert(node->sysdata->bufpool);
	assert(node->read_event);

	assert(node->sysdata->in_buf);
	in = node->sysdata->in_buf;
	
	stats = node->sysdata->stats;
	assert(stats);

	if (flags & EV_TIMEOUT) {
		stats->te ++;

		// idle
		assert(node->idle >= 0);
		node->idle ++;
		if (node->idle == 3) {
			sendPing(node);
		}
		else if (node->idle == 6) {
			BIT_SET(node->flags, FLAG_NODE_BUSY);
		} 
	}
	else {
		assert(flags & EV_READ);

		stats->re ++;

		// if we have data, then obviously we are not idle...
		assert(node->idle >= 0);
		node->idle = 0;
	
		empty = 0;
		while (empty == 0) {
			assert(in->length == 0 && in->max > 0 && in->data != NULL);
			assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));
			assert(node->handle >= 0);
			
			res = read(node->handle, in->data, in->max);
			if (res > 0) {
				assert(res <= in->max);
				stats->in_bytes += res;
				in->length = res;
	
				// if we pulled out the max we had avail in our buffer, that means we
				// can pull out more at a time, so increase our incoming buffer.
				if (res == in->max) {
					expbuf_shrink(in, in->max + DEFAULT_BUFFSIZE);
					assert(empty == 0);
				}
				else { empty = 1; }
	
				assert(node->waiting);
				if (node->waiting->length > 0) {
					// we have data left in the in-buffer, so we add the content of the node->in buffer
					assert(in->length > 0);
					expbuf_add(node->waiting, in->data, in->length);
					expbuf_clear(in);
					assert(in->length == 0);
					assert(node->waiting->length > 0 && node->waiting->data != NULL);
	
					assert(node->sysdata->risp != NULL);
					res = risp_process(node->sysdata->risp, node, node->waiting->length, (unsigned char *) node->waiting->data);
					assert(res <= node->waiting->length);
					assert(res >= 0);
					if (res > 0) { expbuf_purge(node->waiting, res); }
				}
				else {
					// there is no data in the waiting-buffer, we will process the node->in buffer by itself.
					assert(node->sysdata->risp != NULL);
					res = risp_process(node->sysdata->risp, node, in->length, (unsigned char *) in->data);
					assert(res <= in->length);
					assert(res >= 0);
					if (res > 0) { expbuf_purge(in, res); }
	
					// if there is data left over, then we need to add it to our in-buffer.
					if (in->length > 0) {
						assert(node->waiting);
						expbuf_add(node->waiting, in->data, in->length);
						expbuf_clear(in);
					}
				}
			}
			else {

				// either the socket has closed, or it would have blocked, even
				// though we got an event to say it is ready.
			
				assert(empty == 0);
				empty = 1;
				
				if (res == 0) {
					logger(node->sysdata->logging, 3, 
						"Node[%d] closed while reading.", node->handle);
					assert(node->out);
					node->handle = INVALID_HANDLE;
					node_closed(node);
					assert(empty != 0);
				}
				else {
					assert(res == -1);
					if (errno != EAGAIN && errno != EWOULDBLOCK) {
						logger(node->sysdata->logging, 3, 
							"Node[%d] closed while reading- because of error: %d", node->handle, errno);
						close(node->handle);
						node->handle = INVALID_HANDLE;
						node_closed(node);
						assert(empty != 0);
					}
				}
			}
		}
	}
}


//-----------------------------------------------------------------------------
// if there is data waiting to be sent, we will send it.  This function will
// write data that is placed in the outgoing buffer (normally because it
// couldn't be sent before by node_write_now()).  It will try and send
// everything in one go, and if it succeeds, then it will clear the 'write'
// event.
void node_write_handler(int hid, short flags, void *data)
{
	node_t *node = (node_t *) data;
	stats_t *stats;
	int res;

	assert(hid >= 0);
	assert(node);
	assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));
	assert(node->sysdata);
	assert(node->handle == hid);
	assert(flags & EV_WRITE);
	assert(node->write_event);

	stats = node->sysdata->stats;
	assert(stats);
	stats->we ++;

	// we've requested the event, so we should have data to process.
	assert(node->out);
	assert(node->out->length > 0);
	assert(node->out->length <= node->out->max);
	assert(node->out->data);
	assert(node->handle > 0 && node->handle != INVALID_HANDLE);
	assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));

	res = send(node->handle, node->out->data, node->out->length, 0);
	if (res > 0) {
		// we managed to send some, or maybe all....
		assert(res <= node->out->length);
		stats->out_bytes += res;
		expbuf_purge(node->out, res);

		// if we have sent everything, then we can remove the write event for this node.
		assert(node->out != NULL);
		if (node->out->length == 0) {
			event_del(node->write_event);
			event_free(node->write_event);
			node->write_event = NULL;
		}
	}
	else if (res == 0) {
		logger(node->sysdata->logging, 3, "Node[%d] closed while writing.", node->handle);
		assert(node->out);
		node->handle = INVALID_HANDLE;
		node_closed(node);
		assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
	}
	else {
		assert(res == -1);
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			logger(node->sysdata->logging, 3, "Node[%d] closed while writing - because of error: %d", node->handle, errno);
			close(node->handle);
			node->handle = INVALID_HANDLE;
			node_closed(node);
			assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
		}
	}
}


//-----------------------------------------------------------------------------
// shutdown the node.  If the node still has pending requests, then we need to
// wait until the requests have been returned or have timed out.
void node_shutdown(node_t *node)
{
	system_data_t *sysdata;

	assert(node);
	assert(node->sysdata);
	
	sysdata = node->sysdata;

	// if the node is still connected, then we will attempt to first tell it that
	// we are closing the node.  If the node closed the connection first, then we
	// wont have a chance to do this.
	if (node->handle != INVALID_HANDLE && BIT_TEST(node->flags, FLAG_NODE_CLOSING) == 0) {
		sendClosing(node);
		BIT_SET(node->flags, FLAG_NODE_CLOSING);
	}

	// We are still connected to the node, so we need to put a timeout on the
	// messages the node is servicing.
	if (node->handle != INVALID_HANDLE && (ll_count(&node->in_msg) > 0 || ll_count(&node->out_msg) > 0)) {
		// need to set a timeout on the messages.
		assert(0);
	}
	else {

		// if we have done all we need to do, but still have a valid handle, then we should close it, and delete the event.
		if (node->handle != INVALID_HANDLE && node->out->length == 0) {
			assert(node->out->length == 0);
			assert(ll_count(&node->in_msg) == 0);
			assert(ll_count(&node->out_msg) == 0);
			assert(node->handle > 0);
			close(node->handle);
			node->handle = INVALID_HANDLE;
			node_closed(node);
		}
	}
}


//-----------------------------------------------------------------------------
// we have an array of node connections.  This function will be used to create
// a new node connection and put it in the array.  This only has to be done
// once for each slot.  Once it is created, it is re-used.
node_t * node_create(system_data_t *sysdata, int handle)
{
	node_t *node;
	struct timeval five_seconds = {5,0};
	
	assert(handle > 0);
	assert(sysdata);
	assert(sysdata->evbase);

	node = (node_t *) malloc(sizeof(node_t));
	assert(node);
	if (node) {

		assert(sysdata->bufpool != NULL);
		node_init(node, sysdata);
		node->handle = handle;
				
		assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
		BIT_SET(node->flags, FLAG_NODE_ACTIVE);

		// should we add the node to the node list?
		// setup the event handling...
		assert(sysdata->evbase != NULL);
		assert(node->read_event == NULL);
		node->read_event = event_new(
			sysdata->evbase,
			handle,
			EV_READ | EV_PERSIST,
			node_read_handler,
			(void *) node);
		event_add(node->read_event, &five_seconds);

		// add the node to the nodelist.
		ll_push_head(sysdata->nodelist, node);
	}
	
	return(node);
}


//-----------------------------------------------------------------------------
// Find a message in the out-queue for the node.  Return the message if it was
// found (which it should have, right?)
message_t * node_findoutmsg(node_t *node, msg_id_t msgid)
{
	message_t *msg, *tmp;
	void *next;

	assert(node);
	assert(msgid > 0);
	
	msg = NULL;
	next = ll_start(&node->out_msg);
	tmp = ll_next(&node->out_msg, &next);
	while (tmp) {
		assert(tmp->id > 0);
		if (tmp->id == msgid) {
			msg = tmp;
			tmp = NULL;
		}
		else {
			tmp = ll_next(&node->out_msg, &next);
		}
	}

	return(msg);
}
