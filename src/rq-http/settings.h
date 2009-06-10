#ifndef __RQ_HTTP_SETTINGS
#define __RQ_HTTP_SETTINGS

#include <linklist.h>

typedef struct {
	// running params
	short verbose;
	short daemonize;
	char *username;
	char *pid_file;

	// connections to the controllers.
	list_t *controllers;

	// TODO: do we want this as a simple string, or as a list?
	char *interface;

	// unique settings.
	char *configfile;
	char *logqueue;
	char *statsqueue;
	char *gzipqueue;
	char *blacklist;
} settings_t;


void settings_init(settings_t *ptr);
void settings_free(settings_t *ptr);


#endif


