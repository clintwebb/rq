// settings.c

#include "settings.h"

#include <assert.h>
#include <rq.h>
#include <stdlib.h>




//-----------------------------------------------------------------------------
// Initialise the settings structure.
void settings_init(settings_t *ptr)
{
	assert(ptr != NULL);

	ptr->port = RQ_DEFAULT_PORT;
	ptr->maxconns = DEFAULT_MAXCONNS;
	ptr->verbose = false;
	ptr->daemonize = false;
	ptr->username = NULL;
	ptr->pid_file = NULL;

	ptr->interfaces = (list_t *) malloc(sizeof(list_t));
	ll_init(ptr->interfaces);

	ptr->controllers = (list_t *) malloc(sizeof(list_t));
	ll_init(ptr->controllers);

	ptr->logfile = NULL;
}


//-----------------------------------------------------------------------------
// Cleanup the resources allocated int he settings object.   Currently this
// just includes the list of controllers.
void settings_cleanup(settings_t *ptr) 
{
	char *str;
	
	assert(ptr != NULL);

	while ((str = ll_pop_head(ptr->interfaces))) { free(str); }
	ll_free(ptr->interfaces);
	free(ptr->interfaces);
	ptr->interfaces = NULL;
	
	while ((str = ll_pop_head(ptr->controllers))) { free(str); }
	ll_free(ptr->controllers);
	free(ptr->controllers);
	ptr->controllers = NULL;
}


