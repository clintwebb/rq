// settings.c

#include "settings.h"

#include <assert.h>
#include <rq.h>
#include <stdlib.h>

void settings_init(settings_t *ptr)
{
	assert(ptr != NULL);

	ptr->verbose = false;
	ptr->daemonize = false;
	ptr->username = NULL;
	ptr->pid_file = NULL;

	ptr->configfile = NULL;
	ptr->logqueue = NULL;
	ptr->statsqueue = NULL;
	ptr->gzipqueue = NULL;
	ptr->blacklist = NULL;

	ptr->controllers = (list_t *) malloc(sizeof(list_t));
	ll_init(ptr->controllers);
}

void settings_free(settings_t *ptr)
{
	assert(ptr != NULL);

	if (ptr->configfile) { free(ptr->configfile); ptr->configfile = NULL; }
	if (ptr->logqueue)   { free(ptr->logqueue);   ptr->logqueue = NULL; }
	if (ptr->statsqueue) { free(ptr->statsqueue); ptr->statsqueue = NULL; }
	if (ptr->gzipqueue)  { free(ptr->gzipqueue);  ptr->gzipqueue = NULL; }
	if (ptr->blacklist)  { free(ptr->blacklist);  ptr->blacklist = NULL; }

	assert(ptr->controllers);
	ll_free(ptr->controllers);
	free(ptr->controllers);
	ptr->controllers = NULL;
}





