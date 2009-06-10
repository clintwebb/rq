#ifndef __MASTER_DATA_H
#define __MASTER_DATA_H

#include "config.h"
#include "settings.h"

#include <event.h>
#include <rq.h>


typedef struct {
	struct event_base *evbase;
	rq_t              *rq;
	settings_t        *settings;
	config_t          *config;

	struct event *sigint_event;
	struct event *sighup_event;
	struct event *sigusr1_event;
	struct event *sigusr2_event;

} control_t;


#endif


