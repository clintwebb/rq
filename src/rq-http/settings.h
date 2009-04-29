#ifndef __RQ_HTTP_SETTINGS
#define __RQ_HTTP_SETTINGS

typedef struct {
	// running params
	short verbose;
	short daemonize;
	char *username;
	char *pid_file;
	char *interface;

	// connections to the controllers.
	char *primary;
	char *secondary;
	int priport;
	int secport;

	// unique settings.
	char *configfile;
	char *logqueue;
	char *statsqueue;
	char *gzipqueue;
	char *blacklist;
} settings_t;


void settings_init(settings_t *ptr);
void settings_cleanup(settings_t *ptr);


#endif


