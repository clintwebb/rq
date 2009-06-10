
#include "master-data.h"
#include "signals.h"

#include <assert.h>
#include <rq.h>

//-----------------------------------------------------------------------------
void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// need to initiate an RQ shutdown.
	assert(control->rq);
	rq_shutdown(control->rq);

	// delete the signal events.
	assert(control->sigint_event);
	event_free(control->sigint_event);
	control->sigint_event = NULL;

	assert(control->sighup_event);
	event_free(control->sighup_event);
	control->sighup_event = NULL;

	assert(control->sigusr1_event);
	event_free(control->sigusr1_event);
	control->sigusr1_event = NULL;

	assert(control->sigusr2_event);
	event_free(control->sigusr2_event);
	control->sigusr2_event = NULL;
}


//-----------------------------------------------------------------------------
// When SIGHUP is received, we need to re-load the config database.  At the
// same time, we should flush all caches and buffers to reduce the system's
// memory footprint.   It should be as close to a complete app reset as
// possible.
void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// clear out all cached objects.
	assert(0);

	// reload the config database file.
	assert(0);

}

//-----------------------------------------------------------------------------
// increase the log level.
void sigusr1_handler(evutil_socket_t fd, short what, void *arg)
{
// 	control_t *control = (control_t *) arg;

	assert(fd < 0);
	assert(arg);

	// increase the log level.
	assert(0);

	// write a log entry to indicate the new log level.
	assert(0);
}


//-----------------------------------------------------------------------------
// decrease the log level.
void sigusr2_handler(evutil_socket_t fd, short what, void *arg)
{
// 	control_t *control = (control_t *) arg;

	assert(fd < 0);
	assert(arg);

	// decrease the log level.
	assert(0);

	// write a log entry to indicate the new log level.
	assert(0);
}

