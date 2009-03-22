#ifndef __PROCESS_H
#define __PROCESS_H

#include "node.h"

void processRequest(node_t *node);
void processReply(node_t *node);
void processConsume(node_t *node);
void processCancelQueue(node_t *node);
void processClosing(node_t *node);
void processServerFull(node_t *node);
void processQueueLink(node_t *node);
void processController(node_t *node);
void processBroadcast(node_t *node);
void processDelivered(node_t *node);
void processReceived(node_t *node);

#endif

