#ifndef __DATA_H
#define __DATA_H

#include <expbuf.h>
#include <risp.h>

// this mask is used to show which data elements have been provided.  If the
// appropriate flag mask has not been set, then you cannot assume that the
// data field has valid data.
#define DATA_MASK_ID        1
#define DATA_MASK_TIMEOUT   2
#define DATA_MASK_MAX       4
#define DATA_MASK_PRIORITY  8
#define DATA_MASK_QUEUEID   16
#define DATA_MASK_QUEUE     32
#define DATA_MASK_PAYLOAD   64

// operational flags.
#define DATA_FLAG_REQUEST       1
#define DATA_FLAG_REPLY         2
#define DATA_FLAG_BROADCAST     4
#define DATA_FLAG_NOREPLY       8
#define DATA_FLAG_CONSUME       16
#define DATA_FLAG_CANCEL_QUEUE  32
#define DATA_FLAG_CLOSING       64
#define DATA_FLAG_SERVER_FULL   128
#define DATA_FLAG_CONTROLLER    256
#define DATA_FLAG_RECEIVED      512
#define DATA_FLAG_DELIVERED     1024
#define DATA_FLAG_EXCLUSIVE     2048



typedef struct {
	int id;
	short int timeout;
	short int max;
	short int priority;
	short int qid;
	
	expbuf_t queue;
	expbuf_t payload;

	unsigned int mask;
	unsigned int flags;

} data_t;


void data_init(data_t *data);
void data_clear(data_t *data);
void data_free(data_t *data);


#endif

