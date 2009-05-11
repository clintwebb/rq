#ifndef __SERVER_H
#define __SERVER_H


#include <event.h>
#include <expbuf.h>
#include <linklist.h>
#include <risp.h>

#include "system_data.h"

#define MAX_SERVERS 5



typedef struct {
	struct {
		int handle;					          // socket handle
		struct event *event;
	} servers[MAX_SERVERS];
	int                active;			// number of active nodes.
	int                maxconns;		// max number of nodes.
	system_data_t     *sysdata;
} server_t;




void server_init(server_t *server, system_data_t *sysdata);
void server_listen(server_t *server, int port, char *address);
void server_cleanup(server_t *server);
void server_event_handler(int hid, short flags, void *data);



#endif 

