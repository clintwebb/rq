#ifndef __SETTINGS_H
#define __SETTINGS_H

#include <linklist.h>
#include <sys/socket.h>

#define MAX_INTERFACES	5
#define DEFAULT_MAXCONNS 128





typedef struct {
	// running params
	int maxconns;
	char verbose;
	char daemonize;
	char *username;
	char *pid_file;

	// interfaces that we will bind to.
	char *interface[MAX_INTERFACES];
	int interfaces;

	// port that we will listen on, on all interfaces.
	int port;

	// connections to other controllers.
	list_t controllers;

	// logfile
	char *logfile;
} settings_t;


void settings_init(settings_t *ptr);
void settings_cleanup(settings_t *ptr);



#endif


