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
	system_data_t *sysdata;
	struct event *stats_event;
} stats_t;


void stats_init(stats_t *stats);
void stats_start(stats_t *stats);

#endif

