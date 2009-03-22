#ifndef __STATS_H
#define __STATS_H

#include <stdio.h>

typedef struct {
	unsigned int out_bytes;
	unsigned int in_bytes;
	unsigned int requests;
	unsigned int replies;
	unsigned int broadcasts;
	FILE *logfile;
	short shutdown;
} stats_t;


void stats_init(stats_t *stats);


#endif

