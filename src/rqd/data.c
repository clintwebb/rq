// data.c

#include "data.h"

#include <assert.h>
#include <rq.h>



//-----------------------------------------------------------------------------
// clear the received data... normally done when a RQ_CMD_CLEAR is received.
// Assumes we are using a structure that has been initialised and maintains
// integrity.
void data_clear(data_t *data)
{
	assert(data != NULL);

	data->mask = 0;
	data->flags = 0;
	data->id = 0;
	data->timeout = 0;
	data->max = 0;
	data->priority = RQ_PRIORITY_NONE;
	
	expbuf_clear(&data->queue);
	expbuf_clear(&data->payload);
}


//-----------------------------------------------------------------------------
// Initialise a data structure.  Assumes that structure does not contain any
// valid values.
void data_init(data_t *data)
{
	assert(data != NULL);
	expbuf_init(&data->queue, 0);
	expbuf_init(&data->payload, 0);
	data_clear(data);
}


//-----------------------------------------------------------------------------
// prepare the data structure for de-allocation.
void data_free(data_t *data)
{
	assert(data != NULL);

	expbuf_free(&data->queue);
	expbuf_free(&data->payload);
}


