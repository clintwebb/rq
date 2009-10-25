// controllers.c

#include "controllers.h"
#include "node.h"
#include "queue.h"
#include "send.h"
#include "server.h"
#include "system_data.h"

#include <assert.h>
#include <errno.h>
#include <evlogging.h>
#include <rq.h>
#include <stdlib.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// Initialise a controller object, and add the target to it.  The 'str' passed
// in is now controlled by this object and will be free'd when it is no longer
// needed.
void controller_init(controller_t *ct, const char *str)
{
	assert(ct && str);
	
	ct->target = (char *) str;	// this object now has control of this pointer.
	
	ct->node = NULL;
	ct->flags = 0;
	ct->sysdata = NULL;
	ct->connect_event = NULL;
}


//-----------------------------------------------------------------------------
// Free the resources created for this object.
void controller_free(controller_t *ct)
{
	assert(ct);
	assert(ct->node == NULL);
	if (ct->connect_event) {
		event_del(ct->connect_event);
		ct->connect_event = NULL;
	}

	assert(ct->target);
	free(ct->target);
	ct->target = NULL;
}

//-----------------------------------------------------------------------------
// When the controller could not connect, it is paused for a certain time, and
// then a connect attempt needs to be made again.
static void controller_wait_handler(int fd, short int flags, void *arg)
{
	controller_t *ct;

	assert(fd < 0);
	assert((flags & EV_TIMEOUT) == EV_TIMEOUT);
	assert(arg);
	
	ct = (controller_t *) arg;
	assert(ct->connect_event);
	event_free(ct->connect_event);
	ct->connect_event = NULL;

	// only retry the connect if the controller is not marked as FAILED.
	if (BIT_TEST(ct->flags, FLAG_CONTROLLER_FAILED) == 0) {
		controller_connect(ct);
	}
}



//-----------------------------------------------------------------------------
// callback function that will fire when the connection to the controller is reached.
static void controller_connect_handler(int fd, short int flags, void *arg)
{
	controller_t *ct = (controller_t *) arg;
	node_t *node;
	system_data_t *sysdata;
	queue_t *q;
	int error;
	socklen_t foo;
	struct timeval t = {.tv_sec = 1, .tv_usec = 0};
	short int exclusive;

	assert(fd >= 0);
	assert(flags != 0);
	assert(ct);
	assert(ct->node == NULL);
	assert(ct->target);
	assert(ct->sysdata);
	sysdata = ct->sysdata;
	
	// we only need to detect EV_WRITE for a connect event.
	assert(flags == EV_WRITE);

	// remove the connect event
	assert(ct->connect_event);
	event_free(ct->connect_event);
	ct->connect_event = NULL;

	// we are no longer connected.  We are either connected, or not.
	assert(BIT_TEST(ct->flags, FLAG_CONTROLLER_CONNECTING));
	BIT_CLEAR(ct->flags, FLAG_CONTROLLER_CONNECTING);

	// check to see if we really are connected.
	foo = sizeof(error);
	getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &foo);
	if (error == ECONNREFUSED) {
		// we were connecting, but couldn't.
		BIT_SET(ct->flags, FLAG_CONTROLLER_CLOSED);

		// close the socket that didn't connect.
		close(fd);

		//set the action so that we can attempt to reconnect.
		assert(ct->connect_event == NULL);
		assert(sysdata->evbase);
		ct->connect_event = evtimer_new(sysdata->evbase, controller_wait_handler, (void *) ct);
		evtimer_add(ct->connect_event, &t);
	}
	else {
		logger(((system_data_t *)ct->sysdata)->logging, 2,
			"connected to remote controller: %s", ct->target);
	
		// create the node object.
		node = node_create(sysdata, fd);
	
		// add node to the main nodes list.
		ct->node = node;
		assert(node->controller == NULL);
		node->controller = ct;
		BIT_SET(node->flags, FLAG_NODE_CONTROLLER);
	
		// we need to check to see if we have any queues with active consumers,
		// and if we do, then we need to send consume requests to this node for
		// each one.
		assert(sysdata->queues);
		ll_start(sysdata->queues);
		while ((q = ll_next(sysdata->queues))) {
			assert(q->qid > 0);
			assert(q->name);
	
			if (ll_count(&q->nodes_busy) > 0 || ll_count(&q->nodes_ready) > 0) {
				logger(((system_data_t *)ct->sysdata)->logging, 2, 
					"Sending queue consume ('%s') to alternate controller at %s",
					q->name, ct->target );

				exclusive = 0;
				if (BIT_TEST(q->flags, QUEUE_FLAG_EXCLUSIVE))
					exclusive = 1;
				
				sendConsume(node, q->name, 1, RQ_PRIORITY_LOW, exclusive);
				ll_push_head(&q->nodes_consuming, node);
			}
		}
		ll_finish(sysdata->queues);
	}
}





void controller_connect(controller_t *ct)
{
	evutil_socket_t sock;
	int result;
	int len;

	assert(ct);
	assert(ct->target);
	assert(ct->node == NULL);
	assert(BIT_TEST(ct->flags, FLAG_CONTROLLER_CONNECTED) == 0);
	assert(BIT_TEST(ct->flags, FLAG_CONTROLLER_FAILED) == 0);
	assert(BIT_TEST(ct->flags, FLAG_CONTROLLER_CONNECTING) == 0);

	// if not already resolved.... resolve the target.
	if (BIT_TEST(ct->flags, FLAG_CONTROLLER_RESOLVED) == 0) {
		assert(ct->flags == 0);
		
		logger(((system_data_t *)ct->sysdata)->logging, 3, "resolving controller %s.", ct->target);

		len = sizeof(ct->saddr);
		if (evutil_parse_sockaddr_port(ct->target, &ct->saddr, &len) == 0) {
			BIT_SET(ct->flags, FLAG_CONTROLLER_RESOLVED);
		}
		else {
			BIT_SET(ct->flags, FLAG_CONTROLLER_FAILED);
		}
	}
	

	if (BIT_TEST(ct->flags, FLAG_CONTROLLER_FAILED) == 0) {
		assert(BIT_TEST(ct->flags, FLAG_CONTROLLER_RESOLVED));

		BIT_SET(ct->flags, FLAG_CONTROLLER_CONNECTING);

		sock = socket(AF_INET,SOCK_STREAM,0);
		assert(sock >= 0);
						
		// Before we attempt to connect, set the socket to non-blocking mode.
		evutil_make_socket_nonblocking(sock);

		logger(((system_data_t *)ct->sysdata)->logging, 3, "Attempting Remote connect to %s.", ct->target);

		result = connect(sock, &ct->saddr, sizeof(struct sockaddr));
		assert(result < 0);
		assert(errno == EINPROGRESS);

		// connect process has been started.  Now we need to create an event so that we know when the connect has completed.
		assert(ct->connect_event == NULL);
		assert(ct->sysdata);
		assert(((system_data_t *)ct->sysdata)->evbase);
		ct->connect_event = event_new(((system_data_t *)ct->sysdata)->evbase, sock, EV_WRITE, controller_connect_handler, ct);
		event_add(ct->connect_event, NULL);
	}
	else {
		assert(ct->target);
		logger(((system_data_t *)ct->sysdata)->logging, 2, "Remote connect to %s has failed.", ct->target);
	}
}




