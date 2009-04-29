// settings.c

#include "settings.h"

#include <assert.h>
#include <rq.h>

void settings_init(settings_t *ptr)
{
	assert(ptr != NULL);

	ptr->verbose = false;
	ptr->daemonize = false;
	ptr->username = NULL;
	ptr->pid_file = NULL;

	ptr->primary = NULL;
	ptr->priport = RQ_DEFAULT_PORT;
	ptr->secondary = NULL;
	ptr->secport = RQ_DEFAULT_PORT;
	
	ptr->configfile = NULL;
	ptr->logqueue = NULL;
	ptr->statsqueue = NULL;
	ptr->gzipqueue = NULL;
	ptr->blacklist = NULL;
}

void settings_cleanup(settings_t *ptr) 
{
	assert(ptr != NULL);
}





