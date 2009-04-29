// server.c
// RQ-http

#include "server.h"

#include <assert.h>

//-----------------------------------------------------------------------------
// Initialise the server object.
void server_init(server_t *server)
{
	assert(server);

	server->rq = NULL;
}


//-----------------------------------------------------------------------------
// Cleanup the server object.
void server_cleanup(server_t *server)
{
	assert(server);

	server->rq = NULL;
}

