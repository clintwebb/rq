#ifndef __SETTINGS_H
#define __SETTINGS_H

#include <rq.h>

#define MAX_INTERFACES	5
#define DEFAULT_MAXCONNS 128

typedef struct {
	// running params
	int port;
	int maxconns;
	bool verbose;
	bool daemonize;
	char *username;
	char *pid_file;

	char *interface[MAX_INTERFACES];
	int interfaces;

	// connections to other controllers.
	char *primary;
	char *secondary;
	int priport;
	int secport;

	// logfile
	char *logfile;
} settings_t;


void settings_init(settings_t *ptr);
void settings_cleanup(settings_t *ptr);



#endif


