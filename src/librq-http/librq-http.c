//-----------------------------------------------------------------------------
// librq-http
// 
// Library to interact with the rq-http service.   It is used by the consumers
// that rq-http sends requests to.   The rq-http daemon also uses it to handle
// the results from the consumers.
//-----------------------------------------------------------------------------


#include "rq-http.h"

// #include <arpa/inet.h>
#include <assert.h>
// #include <errno.h>
// #include <fcntl.h>
// #include <rispbuf.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <sys/socket.h>
// #include <time.h>
// #include <unistd.h>


void rq_http_init(rq_http_t *http)
{
	assert(http);
}

void rq_http_free(rq_http_t *http)
{
	assert(http);
}



/*

// this function is only used in 'direct' mode.  It takes a formed packet in
// 'buffer' and adds it as a Queue broadcaste message into the 'pending'
// 'buffer.  'buffer' is not cleared.
static void add_pending_packet(rq_log_t *log)
{
	assert(log != NULL);
	assert(log->rq == NULL);

	assert(log->packet.length > 0);
	assert(log->queue != NULL);

	addCmd(&log->pending, RQ_CMD_CLEAR);
 	addCmd(&log->pending, RQ_CMD_REQUEST);
	addCmd(&log->pending, RQ_CMD_NOREPLY);
	addCmdShortStr(&log->pending, RQ_CMD_QUEUE, strlen(log->queue), log->queue);
	addCmdLargeStr(&log->pending, RQ_CMD_PAYLOAD, log->packet.length, log->packet.data);
	addCmd(&log->pending, RQ_CMD_EXECUTE);

	assert(log->pending.length > 0 && log->pending.max >= log->pending.length && log->pending.data != NULL);
}



*/


