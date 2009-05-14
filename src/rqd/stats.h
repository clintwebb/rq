#ifndef __STATS_H
#define __STATS_H

#include "system_data.h"

#include <stdio.h>


typedef struct {
	unsigned int out_bytes;
	unsigned int in_bytes;
	unsigned int requests;
	unsigned int replies;
	unsigned int broadcasts;
	unsigned int re, we;
	FILE *logfile;
	short shutdown;
} stats_t;


void stats_init(stats_t *stats);
void stats_display(stats_t *stats, system_data_t *sysdata);

#endif

