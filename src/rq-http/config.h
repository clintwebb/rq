#ifndef __CONFIG_H
#define __CONFIG_H

#include <sqlite3.h>

typedef struct {
	char    *configfile;
	sqlite3 *dbh;
	int      rc;
} config_t;


int config_init(config_t *config, const char *configfile);
void config_free(config_t *config);

#endif
