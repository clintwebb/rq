// server.c

#include "server.h"
#include "commands.h"
#include "queue.h"
#include "settings.h"

#include <assert.h>
#include <errno.h>
#include <evlogging.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

//-----------------------------------------------------------------------------
// Initialise the server component.
void server_init(server_t *server, system_data_t *sysdata)
{
	assert(server);
	assert(sysdata);
	assert(sysdata->evbase);
	assert(sysdata->settings);
	assert(sysdata->queues == NULL);

	assert(sysdata->settings->maxconns > 0);

	server->sysdata = sysdata;
	server->handle = INVALID_HANDLE;
	server->event = NULL;
}


//-----------------------------------------------------------------------------
// free up resources allocated for an instance.   The server instances would
// already have been cleaned up by this point, because it is needed to get out
// of the events system.
void server_free(server_t *server)
{
	assert(server);
	assert(server->handle == INVALID_HANDLE);
	assert(server->event == NULL);
}




static int new_socket(struct addrinfo *ai) {
	int sfd = INVALID_HANDLE;
	int flags;
	
	assert(ai != NULL);
	
	// bind the socket, and then set the socket to non-blocking mode.
	if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) >= 0) {
		if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 || fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
			close(sfd);
			sfd = INVALID_HANDLE;
		}
	}
	
	return sfd;
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
static void server_event_handler(int hid, short flags, void *data)
{
	server_t *server;
	socklen_t addrlen;
	struct sockaddr_storage addr;
	int sfd;
	node_t *node = NULL;
	
	assert(hid >= 0);
	assert(data != NULL);
	
  server = (server_t *) data;
	assert(server != NULL);
	assert(server->sysdata != NULL);

	addrlen = sizeof(addr);
	sfd = accept(hid, (struct sockaddr *)&addr, &addrlen);
	if (sfd == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
				/* these are transient, so don't log anything */
		} else if (errno == EMFILE) {
			fprintf(stderr, "Too many open connections\n");
			assert(0);
		} else {
			perror("accept()");
		}
		return;
	}

	logger(server->sysdata->logging, 2, "New Connection [%d]", sfd);
	node = node_create(server->sysdata, sfd);
	assert(node);

	// mark socket as non-blocking
	evutil_make_socket_nonblocking(sfd);
}



static void server_listen_ai(server_t *server, struct addrinfo *ai)
{
  struct linger ling = {0, 0};
	int flags;

	assert(server);
	assert(ai);

	assert(server->handle == INVALID_HANDLE);
	assert(server->event == NULL);
	
	// create the new socket.  if that fails, free the memory we've already allocated, and return NULL.
	server->handle = new_socket(ai);
	assert(server->handle != INVALID_HANDLE);

	flags = 1;
	setsockopt(server->handle, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
	setsockopt(server->handle, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
	setsockopt(server->handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

	if (bind(server->handle, ai->ai_addr, ai->ai_addrlen) == -1) {
		close(server->handle);
		server->handle = INVALID_HANDLE;
	} else {
		if (listen(server->handle, 1024) == -1) {
			perror("listen()");
			close(server->handle);
			server->handle = INVALID_HANDLE;
		}
		else {
			// Now that we are actually listening on the socket, we need to set the event.
			assert(server->handle >= 0);
			assert(server->event == NULL);
			assert(server->sysdata->evbase);
			server->event = event_new(server->sysdata->evbase, server->handle, EV_READ | EV_PERSIST, server_event_handler, (void *)server);
			event_add(server->event, NULL);
		}
	}

	assert((server->handle == INVALID_HANDLE && server->event == NULL) || (server->handle >= 0 && server->event));
}


//-----------------------------------------------------------------------------
// Initialise and return a server struct that we will use to control the nodes 
// that we are connected to.   We will bind the listening port on the socket.
void server_listen(server_t *server, int port, char *address)
{
	struct addrinfo *ai;
	struct addrinfo *next;
	struct addrinfo hints;
	char port_buf[NI_MAXSERV];
	server_t *sub;
	int error;

	assert(server);
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
	}
	else {
	
		for (next=ai; next; next=next->ai_next) {
		
			if (server->handle == INVALID_HANDLE) {
				// we haven't used this object yet...
				server_listen_ai(server, next);
			}
			else {
				// this object is already in use, so we will create a new server instance and action that instead.
				sub = (server_t *) malloc(sizeof(server_t));
				server_init(sub, server->sysdata);
				assert(server->sysdata->servers);
				ll_push_tail(server->sysdata->servers, sub);
	
				server_listen_ai(sub, next);
			}
		}
		
		freeaddrinfo(ai);
	}
	
	assert(server->handle != INVALID_HANDLE);
	assert(server->event);
   
}


// When the system is shutting down, it will close all listening servers.
void server_shutdown(server_t *server)
{
	assert(server);
	assert(server->sysdata);
	logger(server->sysdata->logging, 1, "Closing socket %d.", server->handle);

	assert(server->event);
	event_free(server->event);
	server->event = NULL;
	
	assert(server->handle != INVALID_HANDLE);
	close(server->handle);
	server->handle = INVALID_HANDLE;
}





