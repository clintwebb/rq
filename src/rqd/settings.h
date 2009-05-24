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
	int port;
	list_t *interfaces;
	list_t *controllers;
	char *logfile;
} settings_t;


void settings_init(settings_t *ptr);
void settings_cleanup(settings_t *ptr);



#endif


