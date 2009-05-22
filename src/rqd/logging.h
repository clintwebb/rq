#ifndef __LOGGING_H
#define __LOGGING_H

#include <expbuf.h>

typedef struct {
	struct event_base *evbase;
	struct event *log_event;
	char *filename;
	short int loglevel;
	expbuf_t *outbuf, *buildbuf, *tmpbuf;
} logging_t;

void logging_init(logging_t *logging, struct event_base *evbase, char *logfile, short int loglevel);
void logging_free(logging_t *logging);
void logging_direct(logging_t *logging);

void logger(logging_t *logging, short int level, const char *format, ...);


#endif


