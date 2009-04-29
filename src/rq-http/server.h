/* server.h
*/

#ifndef __RQ_HTTP_SERVER_H
#define __RQ_HTTP_SERVER_H

#include "settings.h"

#include <rq.h>

typedef struct {
	rq_t *rq;
	settings_t *settings;
} server_t;

void server_init(server_t *server);
void server_cleanup(server_t *server);

#endif


