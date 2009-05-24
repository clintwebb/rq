#ifndef __STATS_H
#define __STATS_H

#include <stdio.h>


typedef struct {
	unsigned int out_bytes;
	unsigned int in_bytes;
	unsigned int requests;
	unsigned int replies;
	unsigned int broadcasts;
	unsigned int re, we, te;
	short shutdown;
	void *sysdata;
	struct event *stats_event;
} stats_t;


void stats_init(stats_t *stats);
void stats_start(stats_t *stats);

#endif

