#ifndef __COMMANDS_H
#define __COMMANDS_H

#include <risp.h>


void cmdInvalid(void *base, void *data, risp_length_t len);

void cmdClear(void *base);
void cmdExecute(void *base);
void cmdRequest(void *base);
void cmdReply(void *base);
void cmdBroadcast(void *base);
void cmdNoReply(void *base);
void cmdConsume(void *base);
void cmdCancelQueue(void *base);
void cmdReceived(void *base);
void cmdDelivered(void *base);
void cmdExclusive(void *base);

void cmdPing(void *base);
void cmdPong(void *base);

void cmdId(void *base, risp_int_t value);
void cmdTimeout(void *base, risp_int_t value);
void cmdMax(void *base, risp_int_t value);
void cmdPriority(void *base, risp_int_t value);

void cmdQueue(void *base, risp_length_t length, risp_char_t *data);
void cmdPayload(void *base, risp_length_t length, risp_char_t *data);


#endif

