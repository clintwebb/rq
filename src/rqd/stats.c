// stats.c

#include "stats.h"

#include <assert.h>

void stats_init(stats_t *stats)
{
	assert(stats != NULL);
	
	stats->out_bytes = 0;
	stats->in_bytes = 0;
	stats->requests = 0;
	stats->replies = 0;
	stats->broadcasts = 0;

	stats->logfile = NULL;
	stats->shutdown = 0;
}
