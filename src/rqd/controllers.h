#ifndef __CONTROLLERS_H
#define __CONTROLLERS_H

#include <event.h>
#include <linklist.h>
#include <sys/socket.h>


#define FLAG_CONTROLLER_RESOLVED   1
#define FLAG_CONTROLLER_CONNECTING 2
#define FLAG_CONTROLLER_CONNECTED  4
#define FLAG_CONTROLLER_CLOSING    8
#define FLAG_CONTROLLER_CLOSED     16
#define FLAG_CONTROLLER_FAILED     32

typedef struct {
	char *target;
	struct sockaddr saddr;
	void *node;
	unsigned short flags;
	void *sysdata;
	struct event *connect_event;
} controller_t;


void controller_init(controller_t *ct, const char *str);
void controller_free(controller_t *ct);

void controller_connect(controller_t *ct);

#endif


