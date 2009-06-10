// config 

#include "config.h"

#include <assert.h>
#include <stdlib.h>


// Initialise the 
int config_init(config_t *config, const char *configfile)
{
	int rc;
	sqlite3 *dbh;
	
	assert(config);
	assert(configfile);

	config->configfile = (char *) configfile;

	
	dbh = NULL;
	rc = sqlite3_open(configfile, &dbh);
	assert(dbh);
	if (rc != SQLITE_OK) {
		sqlite3_close(dbh);
		return(-1);
	}
	else {
		assert(rc == SQLITE_OK);

		// We connected to the config database ok.

		// get the list of hosts from the database.
		assert(0);

		// get the list of aliases from the database.
		assert(0);

		// get the list of paths from the database.
		assert(0);

		// close the database connection.
		assert(0);
		
		return(0);
	}
}

void config_free(config_t *config)
{
	assert(config);

	config->configfile = NULL;
}
