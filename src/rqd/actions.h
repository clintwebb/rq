#ifndef __ACTIONS_H
#define __ACTIONS_H

#include <evactions.h>

void ah_server_shutdown(action_t *action);
void ah_node_shutdown(action_t *action);
void ah_queue_shutdown(action_t *action);
void ah_stats(action_t *action);
void ah_message(action_t *action);
void ah_queue_notify(action_t *action);
void ah_queue_deliver(action_t *action);


#endif


