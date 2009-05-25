#ifndef __SIGNALS_H
#define __SIGNALS_H

#include <event.h>

void sighup_handler(evutil_socket_t fd, short what, void *arg);
void sigint_handler(evutil_socket_t fd, short what, void *arg);
void sigusr1_handler(evutil_socket_t fd, short what, void *arg);
void sigusr2_handler(evutil_socket_t fd, short what, void *arg);

#endif


