// logging.c

#include "logging.h"

#include <assert.h>
#include <event.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>


// Initialise the logging system with all the info that we need,
void logging_init(logging_t *logging, struct event_base *evbase, char *logfile, short int loglevel)
{
	assert(logging);
	assert(loglevel >= 0);

	logging->evbase = evbase;
	logging->log_event = NULL;
	logging->filename = logfile;
	logging->loglevel = loglevel;

	logging->outbuf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(logging->outbuf, 0);
	
	logging->buildbuf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(logging->buildbuf, 64);

	logging->tmpbuf = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(logging->tmpbuf, 32);
	
}

void logging_free(logging_t *logging)
{
	assert(logging);

	assert(logging->log_event == NULL);
	
	assert(logging->outbuf);
	expbuf_free(logging->outbuf);
	free(logging->outbuf);
	logging->outbuf = NULL;
	
	assert(logging->buildbuf);
	expbuf_free(logging->buildbuf);
	free(logging->buildbuf);
	logging->buildbuf = NULL;

	assert(logging->tmpbuf);
	expbuf_free(logging->tmpbuf);
	free(logging->tmpbuf);
	logging->tmpbuf = NULL;
}




//-----------------------------------------------------------------------------
// actually write the contents of a buffer to the log file.  It will need to
// create the file if it is not there, otherwise it will append to it.
static void logger_print(logging_t *logging, expbuf_t *buf)
{
	FILE *fp;
	
	assert(logging);
	assert(buf);
	
	assert(buf->length > 0);
	assert(buf->data);
	
	assert(logging->filename);
	fp = fopen(logging->filename, "a");
	if (fp) {
		fputs(expbuf_string(buf), fp);
		fclose(fp);
	}
}

//-----------------------------------------------------------------------------
// Mark the logging system so that it no longer uses events to buffer the
// output.  it will do this by simply removing the evbase pointer.
void logging_direct(logging_t *logging)
{
	assert(logging);
	assert(logging->evbase);

	// Delete the log event if there is one.
	if (logging->log_event) {
		event_free(logging->log_event);
		logging->log_event = NULL;
	}

	// remove the base from the log structure.
	logging->evbase = NULL;

	// if we having pending data to write, then we should write it now (because
	// there wont be any more events, and there might not be any more log
	// entries to push it out)
	if (logging->outbuf->length > 0) {
		logger_print(logging, logging->outbuf);
		expbuf_clear(logging->outbuf);
	}
}


//-----------------------------------------------------------------------------
// callback function that is fired after 5 seconds from when the first log
// entry was made.  It will write out the contents of the outbuffer.
static void logger_handler(int fd, short int flags, void *arg)
{
	logging_t *logging;

	assert(fd < 0);
	assert(arg);

	logging = (logging_t *) arg;

	// clear the timeout event.
	assert(logging->log_event);
	event_free(logging->log_event);
	logging->log_event = NULL;

	assert(logging->outbuf->length > 0);

	logger_print(logging, logging->outbuf);
	expbuf_clear(logging->outbuf);
}



//-----------------------------------------------------------------------------
// either log directly to the file (slow), or buffer and set an event.
void logger(logging_t *logging, short int level, const char *format, ...)
{
	char buffer[30], timebuf[48];
	struct timeval tv;
	time_t curtime;
  va_list ap;
  int redo;
  int n;
 	struct timeval t = {.tv_sec = DEFAULT_LOG_TIMER, .tv_usec = 0};

	
	assert(logging);
	assert(level >= 0);
	assert(format);

	// first check if we should be logging this entry
	if (level <= logging->loglevel && logging->filename) {

		// calculate the time.
		gettimeofday(&tv, NULL);
		curtime=tv.tv_sec;
		strftime(buffer, 30, "%Y-%m-%d %T.", localtime(&curtime));
		snprintf(timebuf, 48, "%s%06ld ", buffer, tv.tv_usec);

		assert(logging->buildbuf);
		assert(logging->buildbuf->length == 0);
		assert(logging->buildbuf->max > 0);

		// process the string. Apply directly to the buildbuf.  If buildbuf is not big enough, increase the size and do it again.
		redo = 1;
		while (redo) {
			va_start(ap, format);
			n = vsnprintf(logging->tmpbuf->data, logging->tmpbuf->max, format, ap);
			va_end(ap);

			assert(n > 0);
			if (n > logging->tmpbuf->max) {
				// there was not enough space, so we need to increase it, and try again.
				expbuf_shrink(logging->tmpbuf, n + 1);
			}
			else {
				assert(n <= logging->tmpbuf->max);
				logging->tmpbuf->length = n;
				redo = 0;
			}
		}

		// we now have the built string in our tmpbuf.  We need to add it to the complete built buffer.
		assert(logging->tmpbuf->length > 0);
		assert(logging->buildbuf->length == 0);
			
		expbuf_set(logging->buildbuf, timebuf, strlen(timebuf));
		expbuf_add(logging->buildbuf, logging->tmpbuf->data, logging->tmpbuf->length);
		expbuf_add(logging->buildbuf, "\n", 1);

		// if evbase is NULL, then we will need to write directly.
		if (logging->evbase == NULL) {
			if (logging->outbuf->length > 0) {
				logger_print(logging, logging->outbuf);
				expbuf_free(logging->outbuf);
			}
			
			logger_print(logging, logging->buildbuf);
		}
		else {
			// we have an evbase, so we need to add our build data to the outbuf.
			expbuf_add(logging->outbuf, logging->buildbuf->data, logging->buildbuf->length);
		
			// if the log_event is null, then we need to set the timeout event.
			if (logging->log_event == NULL) {
				logging->log_event = evtimer_new(logging->evbase, logger_handler, (void *) logging);
				evtimer_add(logging->log_event, &t);
			}
		}
		
		expbuf_clear(logging->buildbuf);
	}
}

