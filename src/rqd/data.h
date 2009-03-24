#ifndef __DATA_H
#define __DATA_H

#include <expbuf.h>
#include <risp.h>

// this mask is used to show which data elements have been provided.  If the
// appropriate flag mask has not been set, then you cannot assume that the
// data field has valid data.
#define DATA_MASK_ID        0x01
#define DATA_MASK_TIMEOUT   0x02
#define DATA_MASK_MAX       0x04
#define DATA_MASK_PRIORITY  0x08
#define DATA_MASK_QUEUEID   0x10
#define DATA_MASK_QUEUE     0x20
#define DATA_MASK_PAYLOAD   0x40

// operational flags.
#define DATA_FLAG_REQUEST       0x0001
#define DATA_FLAG_REPLY         0x0002
#define DATA_FLAG_BROADCAST     0x0004
#define DATA_FLAG_NOREPLY       0x0008
#define DATA_FLAG_CONSUME       0x0010
#define DATA_FLAG_CANCEL_QUEUE  0x0020
#define DATA_FLAG_CLOSING       0x0040
#define DATA_FLAG_SERVER_FULL   0x0080
#define DATA_FLAG_CONTROLLER    0x0100
#define DATA_FLAG_RECEIVED      0x0200
#define DATA_FLAG_DELIVERED     0x0400
#define DATA_FLAG_EXCLUSIVE     0x0800



typedef struct {
	short int id;
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

