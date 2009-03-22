// server.c

#include "server.h"
#include "commands.h"
#include "queue.h"
#include "settings.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void server_init(server_t *server, system_data_t *sysdata)
{
	int index;
	settings_t *settings;
	queue_list_t *ql;

	assert(server != NULL);
	assert(sysdata != NULL);
	
	server->sysdata = sysdata;
	assert(sysdata->evbase != NULL);
	assert(sysdata->settings != NULL);

	settings = sysdata->settings;
	assert(settings->maxconns > 0);

	for(index=0; index<MAX_SERVERS; index++) {
		server->servers[index].handle = INVALID_HANDLE;
	}

	// We will create an array of empty pointers for our nodes.  A pointer
	// doesn't take up much space, and we already know what our maximum is.
	// It will save a lot of detail in having to resize this array dynamically.
	assert(settings->maxconns > 0);
	server->maxconns = settings->maxconns;
	server->nodes = (node_t **) malloc(sizeof(node_t *) * server->maxconns);
	for (index=0; index < server->maxconns; index++) {
		server->nodes[index] = NULL;
	}

	server->shutdown = 0;

	ql = (queue_list_t *) malloc(sizeof(queue_list_t));
	queue_list_init(ql);
	assert(sysdata->queuelist == NULL);
	sysdata->queuelist = ql;
}




static int new_socket(struct addrinfo *ai) {
	int sfd = INVALID_HANDLE;
	int flags;
	
	assert(ai != NULL);
	
	// bind the socket, and then set the socket to non-blocking mode.
	if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) >= 0) {
		if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 || fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
			perror("setting O_NONBLOCK");
			close(sfd);
			sfd = INVALID_HANDLE;
		}
	}
	
	return sfd;
}



//-----------------------------------------------------------------------------
// Initialise and return a server struct that we will use to control the nodes 
// that we are connected to.   We will bind the listening port on the socket.
void server_listen(server_t *server, int port, char *address)
{
  struct linger ling = {0, 0};
	struct addrinfo *ai;
	struct addrinfo *next;
	struct addrinfo hints;
	char port_buf[NI_MAXSERV];
	int error;
	int index;
	int flags;
	
	assert(server != NULL);
	assert(port > 0);
	assert(address == NULL || (address != NULL && address[0] != '\0'));
	assert(server->sysdata != NULL);
	
	memset(&hints, 0, sizeof (hints));
	hints.ai_flags = AI_PASSIVE|AI_ADDRCONFIG;
	hints.ai_family = AF_UNSPEC;
	hints.ai_protocol = IPPROTO_TCP;
	hints.ai_socktype = SOCK_STREAM;

	snprintf(port_buf, NI_MAXSERV, "%d", port);
  error = getaddrinfo(address, port_buf, &hints, &ai);
	if (error != 0) {
		if (error != EAI_SYSTEM)
			fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
		else
			perror("getaddrinfo()");
		return;
	}


	assert(MAX_SERVERS > 0);
	index	= 0;
	for (next=ai; next && index < MAX_SERVERS; next=next->ai_next) {
	
		assert(server);
		assert(server->servers[index].handle == INVALID_HANDLE);
	
		// create the new socket.  if that fails, free the memory we've already allocated, and return NULL.
		server->servers[index].handle = new_socket(next);
		if (server->servers[index].handle == INVALID_HANDLE) {
			freeaddrinfo(ai);
			free(server);
			return;
		}

		flags = 1;
		setsockopt(server->servers[index].handle, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
		setsockopt(server->servers[index].handle, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
		setsockopt(server->servers[index].handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
// 		setsockopt(server->servers[index].handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));

    if (bind(server->servers[index].handle, next->ai_addr, next->ai_addrlen) == -1) {
			if (errno != EADDRINUSE) {
				perror("bind()");
				close(server->servers[index].handle);
				freeaddrinfo(ai);
				free(server);
				return;
			}
            
			close(server->servers[index].handle);
			server->servers[index].handle = INVALID_HANDLE;
			continue;
		} else {
			if (listen(server->servers[index].handle, 1024) == -1) {
				perror("listen()");
				close(server->servers[index].handle);
				freeaddrinfo(ai);
				free(server);
				return;
			}
			else {
	
				if (server->sysdata->verbose) { printf("setting server read events (%d, %d)\n", server->servers[index].handle, index); }
				assert(server->sysdata->evbase != NULL);
				
				assert(server->servers[index].handle >= 0);
				event_set(&server->servers[index].event, server->servers[index].handle, (EV_READ | EV_PERSIST), server_event_handler, (void *)server);
				event_base_set(server->sysdata->evbase, &server->servers[index].event);
				if (event_add(&server->servers[index].event, NULL) == -1) {
					perror("event_add");
				}

				index++;
			}
    }
	}

	assert(server->servers[0].handle != INVALID_HANDLE);
    
	freeaddrinfo(ai);
}

void server_cleanup(server_t *server)
{
	int i;
	assert(server != NULL);
	assert(server->sysdata != NULL);


	// the server listeners should already have been closed and cleaned up as
	// the firt phase of the shutdown process, so here we will check that they
	// have been.
	assert(MAX_SERVERS > 0);
	for (i=0; i<MAX_SERVERS; i++) {
		assert(server->servers[i].handle == INVALID_HANDLE);
		// event should be deleted as well, not sure how to check that.
	}

	// cleanup and free all of the allocated nodes which should all be cleaned out and idle now;
	assert(server->active == 0);
	assert(server->maxconns > 0);
	assert(server->nodes != NULL);
	for (i=0; i<server->maxconns; i++) {
		if (server->nodes[i] != NULL) {
			node_free(server->nodes[i]);
			server->nodes[i] = NULL;
		}
	}
	free(server->nodes);
	server->nodes = NULL;

	// cleanup the list of queues.
	assert(server->sysdata->queuelist != NULL);
	queue_list_free((queue_list_t *)server->sysdata->queuelist);
	free(server->sysdata->queuelist);
	server->sysdata->queuelist = NULL;

	server->sysdata = NULL;
}

//-----------------------------------------------------------------------------
// we have an array of node connections.  This function will be used to create
// a new node connection and put it in the array.  This only has to be done
// once for each slot.  Once it is created, it is re-used.
static node_t * create_node(server_t *server, int handle, int slot)
{
	node_t *node;
	
	assert(handle > 0);
	assert(server != NULL);
	assert(slot >= 0);
	assert(slot < server->maxconns);
	assert(server->sysdata != NULL);
	assert(server->sysdata->evbase != NULL);
	
	node = (node_t *) malloc(sizeof(node_t));
	assert(node != NULL);
	if (node != NULL) {

		assert(server->sysdata->bufpool != NULL);
		node_init(node, server->sysdata);
	
		server->nodes[slot] = node;
		node->handle = handle;
				
		assert(BIT_TEST(node->flags, FLAG_NODE_ACTIVE) == 0);
		BIT_SET(node->flags, FLAG_NODE_ACTIVE);
	}
	
	return(node);
}


//-----------------------------------------------------------------------------
// this function is called when we have received a new socket connection.   We
// need to create a new node, and add it to our node list.  We need to pass to
// the node any pointers to other sub-systems that it will need to have, and
// then we insert the node into the 'node-circle' somewhere.  Finally, we need
// to add the new node to the event base.
//
// If we have reached our limit, we would want to accept the socket, send out
// a 'FULL' command and then close the socket.   That way the client knows
// what the problem is, and can connect to the other server.
//
// ** Ideally, we want to stop receiving events when we have reached our
//    maximum, and let TCP handle the busy state.
void server_event_handler(int hid, short flags, void *data)
{
	server_t *server;
	socklen_t addrlen;
	struct sockaddr_storage addr;
	int sfd;
	node_t *node = NULL;
	int i, nodes;
	char tbuf[4];
	
	assert(hid >= 0);
	assert(data != NULL);
	
  server = (server_t *) data;
	assert(server != NULL);
	assert(server->sysdata != NULL);
	assert(server->sysdata->verbose >= 0);


	addrlen = sizeof(addr);
	sfd = accept(hid, (struct sockaddr *)&addr, &addrlen);
	if (sfd == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
				/* these are transient, so don't log anything */
		} else if (errno == EMFILE) {
			if (server->sysdata->verbose > 0)
					fprintf(stderr, "Too many open connections\n");
		} else {
			perror("accept()");
		}
		return;
	}
	

	if (server->sysdata->verbose) printf("New Connection [%d]\n", sfd);

	node = NULL;

	if (sfd < server->maxconns) {
		if (server->nodes[sfd] == NULL) {
			if (server->sysdata->verbose) printf("Creating a new node (%d)\n", sfd);
	
			node = create_node(server, sfd, sfd);
			
			if (server->sysdata->verbose) printf(" -- node(%d) initialised\n", sfd);
		}
		else if (BIT_TEST(server->nodes[sfd]->flags, FLAG_NODE_ACTIVE) == 0) {
			if (server->sysdata->verbose) printf("Re-using an existing node (%d)\n", sfd);

			node = server->nodes[sfd];
			node->handle = sfd;
			BIT_SET(node->flags, FLAG_NODE_ACTIVE);
			assert(node->sysdata != NULL);
			
			// clear our base out... just to be sure.
			cmdClear(node);
		}
	}

	// Go thru the list of nodes and count the number of active ones.  If we
	// didn't find a slot easily, we can use the first available one we find.
	nodes = 0;
	for (i=0; i<server->maxconns; i++) {
		if (server->nodes[i] != NULL) {
			if (BIT_TEST(server->nodes[i]->flags, FLAG_NODE_ACTIVE)) {
				nodes++;
			}
			else {
				if (node == NULL) {
					if (server->sysdata->verbose) printf("Re-using an existing node (%d) in slot (%d)\n", sfd, i);
		
					node = server->nodes[sfd];
					node->handle = sfd;
					BIT_SET(node->flags, FLAG_NODE_ACTIVE);
					
					// clear our base out... just to be sure.
					cmdClear(node);

					nodes++;
				}
			}
		}
		else {
			if (node == NULL) {
				if (server->sysdata->verbose) printf("Creating a new node (%d) in slot (%d)\n", sfd, i);
				node = create_node(server, sfd, i);
				nodes++;
			}
		}
	}
	server->active = nodes;

	assert(nodes <= server->maxconns);
	assert(nodes > 0);

	if (node == NULL) {
		assert(nodes == server->maxconns);

		if (server->sysdata->verbose) printf("Server is full.\n");

		// we've reached our limit.
		// TODO: we should really use a proper function to send this message out.
		tbuf[0] = RQ_CMD_CLEAR;
		tbuf[1] = RQ_CMD_SERVER_FULL;
		tbuf[2] = RQ_CMD_EXECUTE;
		send(sfd, tbuf, 3, 0);
		close(sfd);
	}
	else {
		
		// mark socket as non-blocking
		if (server->sysdata->verbose) printf(" -- node(%d) setting non-blocking mode\n", sfd);
		if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 || fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
				perror("setting O_NONBLOCK");
				close(sfd);
				// TODO: There are more things we should do to recover from this....  this is kinda bad, but unlikely to happen.
		}
		
		// setup the event handling...
		if (server->sysdata->verbose) printf(" -- node(%d) setting read event flags\n", sfd);
		assert(server->sysdata->evbase != NULL);
		event_set(&node->event, sfd, EV_READ | EV_PERSIST, node_event_handler, (void *)node);
		event_base_set(server->sysdata->evbase, &node->event);
		event_add(&node->event, 0);

		server->active ++;
		assert(server->active > 0);
	}
}


