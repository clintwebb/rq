// node.c

#include "node.h"
#include "data.h"
#include "stats.h"
#include "queue.h"

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
	
	node->in = NULL;
	node->waiting = NULL;
	node->out = NULL;
	node->build = NULL;

	node->msglist = NULL;

	node->next = NULL;
	node->prev = NULL;

	data_init(&node->data);
	
	node->flags = 0;
	
	assert(node->handle == INVALID_HANDLE);
	memset(&node->event, 0, sizeof(node->event));
}


//-----------------------------------------------------------------------------
// prepare a node for de-allocation.  This means freeing buffers too.
void node_free(node_t *node)
{
	assert(node != NULL);
	system_data_t *sysdata;
	
	assert(node != NULL);

	assert(node->next == NULL);
	assert(node->prev == NULL);

	assert(node->sysdata != NULL);
	sysdata = node->sysdata;
	
	assert(sysdata->bufpool != NULL);
	
	if (BIT_TEST(node->flags, FLAG_NODE_ACTIVE)) {
		assert(node->event.ev_base != NULL);
		event_del(&node->event);
	}

	node->flags = 0;
	
	assert(node->handle == INVALID_HANDLE);
	memset(&node->event, 0, sizeof(node->event));

	if (node->in != NULL) {
		assert(node->in->length == 0);
		expbuf_pool_return(sysdata->bufpool, node->in);
		node->in = NULL;
	}
	if (node->out != NULL) {
		expbuf_clear(node->out);
		expbuf_pool_return(sysdata->bufpool, node->out);
		node->out = NULL;
	}
	if (node->waiting != NULL) {
		expbuf_clear(node->waiting);
		expbuf_pool_return(sysdata->bufpool, node->waiting);
		node->waiting = NULL;
	}
	if (node->build != NULL) {
		assert(node->build->length == 0);
		expbuf_pool_return(sysdata->bufpool, node->build);
		node->build = NULL;
	}

	// make sure that all the messages for this node have been processed first.
	// And then free the memory that was used for the list.
	assert(node->msglist == NULL);

	// make sure that this node has been removed from all consumer queues.
	if (node->sysdata->queues)
		queue_cancel_node(node->sysdata->queues, node);
	
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
	
	assert(node != NULL);
	assert(node->sysdata != NULL);

	// we need to remove the consume on the queues.
	if (node->sysdata->queues)
		queue_cancel_node(node->sysdata->queues, node);

	// we need to remove (return) any messages that this node was processing.
	msg = node->msglist;
	while (msg) {
		// we have a message that we need to deal with.

		// if this node was a destination for the message then we need to return a FAILURE to the source.
		assert(0);

		// if this node was a source, we need to send a CANCEL to the destination.
		assert(0);

		// detatch message from node, and the node from the message and then set an action to deal with the message.
		assert(0);
		
		msg = msg->next;
	}
	
	node->handle = INVALID_HANDLE;
	assert(node->flags == 0);

	// need to fire an action to actually delete this node from the server nodeslist.
	assert(0);
}


//-----------------------------------------------------------------------------
// read all the data from the socket, and process it.  It will keep reading
// until there is no more to read.  If there is any data that couldn't be
// processed, then it gets added to a buffer.
static void node_read(node_t *node)
{
	int res, empty;
	stats_t *stats;

	assert(node != NULL);
	assert(node->sysdata != NULL);
	assert(node->sysdata->stats != NULL);
	assert(node->sysdata->bufpool != NULL);

	stats = node->sysdata->stats;

	if (node->in == NULL) {
		assert(DEFAULT_BUFFSIZE > 0);
		node->in = expbuf_pool_new(node->sysdata->bufpool, DEFAULT_BUFFSIZE);
	}
	assert(node->in != NULL);

	empty = 0;
	while (empty == 0) {
		assert(node->in->length == 0);
		assert(node->in->max > 0);
		assert(node->in->data != NULL);
		assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));
		assert(node->handle != INVALID_HANDLE);
		assert(node->handle > 0);
		
		if (node->sysdata->verbose) printf("node(%d) - reading data\n", node->handle);
		
		res = read(node->handle, node->in->data, node->in->max);
		if (res > 0) {
			assert(res <= node->in->max);
			stats->in_bytes += res;
			node->in->length = res;

			if (node->sysdata->verbose) printf("node(%d) - data received (%d)\n", node->handle, res);

			// if we pulled out the max we had avail in our buffer, that means we can pull out more at a time.
			if (res == node->in->max) {
				expbuf_shrink(node->in, node->in->max + DEFAULT_BUFFSIZE);
				assert(empty == 0);
			}
			else { empty = 1; }
			
			if (node->waiting != NULL && node->waiting->length > 0) {
				// we have data left in the in-buffer, so we add the content of the node->in buffer
				assert(node->in->length > 0);
				assert(node->waiting != NULL);
				expbuf_add(node->waiting, node->in->data, node->in->length);
				expbuf_clear(node->in);
				assert(node->in->length == 0);
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
				res = risp_process(node->sysdata->risp, node, node->in->length, (unsigned char *) node->in->data);
				assert(res <= node->in->length);
				assert(res >= 0);
				if (res > 0) { expbuf_purge(node->in, res); }

				// if there is data left over, then we need to add it to our in-buffer.
				if (node->in->length > 0) {
					if (node->waiting == NULL) {
						assert(node->sysdata->bufpool != NULL);
						node->waiting = expbuf_pool_new(node->sysdata->bufpool, node->in->length);
					}
					assert(node->waiting != NULL);
					expbuf_add(node->waiting, node->in->data, node->in->length);
					expbuf_clear(node->in);
				}
			}
		}
		else {
			assert(empty == 0);
			empty = 1;
			
			if (res == 0) {
				if (node->sysdata->verbose)
					printf("Node[%d] closed while reading.\n", node->handle);
				node_closed(node);
				assert(empty != 0);
				if (node->sysdata->verbose)
					printf("Finished clearing node.\n");
			}
			else {
				assert(res == -1);
				if (errno != EAGAIN && errno != EWOULDBLOCK) {
					if (node->sysdata->verbose)
						printf("Node[%d] closed while reading- because of error: %d\n", node->handle, errno);
					close(node->handle);
					node_closed(node);
					assert(empty != 0);
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
void node_write(node_t *node)
{
	stats_t *stats;
	int res;
	
	
	assert(node != NULL);
	assert(node->sysdata != NULL);

	stats = node->sysdata->stats;
	assert(stats != NULL);

	// we've requested the event, so we should have data to process.
	assert(node->out != NULL);
	assert(node->out->length > 0);
	assert(node->out->length <= node->out->max);
	assert(node->out->data != NULL);
	assert(node->handle > 0);
	assert(node->handle != INVALID_HANDLE);

	res = send(node->handle, node->out->data, node->out->length, 0);
	if (res > 0) {
		// we managed to send some, or maybe all....
		assert(res <= node->out->length);
		stats->out_bytes += res;
		expbuf_purge(node->out, res);
	}
	else if (res == 0) {
		printf("Node[%d] closed while writing.\n", node->handle);
		node->handle = INVALID_HANDLE;
		node_closed(node);
		assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
	}
	else {
		assert(res == -1);
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			printf("Node[%d] closed while writing - because of error: %d\n", node->handle, errno);
			close(node->handle);
			node->handle = INVALID_HANDLE;
			node_closed(node);
			assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
		}
	}
		
	// if we have sent everything, then we dont need to wait for a WRITE event
	// anymore, so we need to re-establish the events with only the READ flag.
	assert(node->out != NULL);
	if (BIT_TEST(node->flags, FLAG_NODE_ACTIVE) && node->out->length == 0) {
		if (event_del(&node->event) != -1) {
			assert(node->handle != INVALID_HANDLE && node->handle > 0);
			event_set(&node->event, node->handle, EV_READ | EV_PERSIST, node_event_handler, (void *)node);
			event_base_set(node->event.ev_base, &node->event);
			event_add(&node->event, 0);
		}
	}
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
	
	assert(node != NULL);
	assert(length > 0);
	assert(data != NULL);

	assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));
	assert(node->sysdata != NULL);
	assert(node->sysdata->stats != NULL);

	stats = node->sysdata->stats;

	if (node->out != NULL && node->out->length > 0) {
		// we already have data in the outbuffer, so we will add this new data to
		// it, and wait for the event to fire.

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
				assert(node->sysdata->bufpool != NULL);
				if (node->out == NULL)
					node->out = expbuf_pool_new(node->sysdata->bufpool, length-res);
				assert(node->out != NULL);
				expbuf_add(node->out, &data[res], length-res);
			}
		}
		else if (res == 0) {
			if (node->sysdata->verbose)
				printf("Node[%d] closed while writing.\n", node->handle);
			node->handle = INVALID_HANDLE;
			node_closed(node);
			assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
		}
		else {
			assert(res == -1);
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				if (node->sysdata->verbose)
					printf("Node[%d] closed while writing - because of error: %d\n", node->handle, errno);
				close(node->handle);
				node->handle = INVALID_HANDLE;
				node_closed(node);
				assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
			}
		}
			
		// if we have ended up with data in the out-buffer, then we need to set
		// the event so that we can be notified when it is safe to send more.
		if (BIT_TEST(node->flags, FLAG_NODE_ACTIVE) && node->out != NULL && node->out->length == 0) {
			if (event_del(&node->event) != -1) {
				assert(node->handle > 0);
				event_set(&node->event, node->handle, EV_WRITE | EV_READ | EV_PERSIST, node_event_handler, (void *)node);
				event_base_set(node->event.ev_base, &node->event);
				event_add(&node->event, 0);
			}
		}
	}
}



//-----------------------------------------------------------------------------
// this function is called when we have received data on our node socket.
void node_event_handler(int hid, short flags, void *data)
{
	node_t *node;
	node = (node_t *) data;

	assert(hid >= 0);
	assert(node != NULL);
	assert(node->handle == hid);
	assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE));
	assert(node->sysdata != NULL);
	
	if (flags & EV_READ) {
		if (node->sysdata->verbose)
			printf("Reading from socket.  hid=%d, flags=%u\n", node->handle, flags);
		node_read(node);
		if (node->sysdata->verbose)
			printf("Finished Reading\n");
	}
		
	if ((flags & EV_WRITE) && BIT_TEST(node->flags, FLAG_NODE_ACTIVE)) {
		node_write(node);
	}
}
