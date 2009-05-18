///// action.c

#include "actions.h"
#include "controllers.h"
#include "queue.h"
#include "send.h"
#include "server.h"
#include "stats.h"

#include <assert.h>
#include <evactions.h>
#include <stdlib.h>
#include <unistd.h>



//-----------------------------------------------------------------------------
// This action handler handles the stats output.  Even if we are not doing any
// output this needs to run so that it can keep the counters under control.
void ah_stats(action_t *action)
{
	stats_t *stats;
	system_data_t *sysdata;

	assert(action && action->data && action->shared);
	stats = action->data;
	sysdata = action->shared;
	
	stats_display(stats, sysdata);

	if (stats->shutdown == 0) {
		// since we are not shutting down, schedule the action again.
		action_reset(action);
	}
	else {
		// we are not firing the action again, so we should return it to the pool it came from.
		action_pool_return(action);
	}
}


//-----------------------------------------------------------------------------
// This action is created when a message is provided that has a timeout.  This
// event will fire every second, decrementing the counter of the message.  If
// the countdown gets to 0, then it will initiate a 'return' of the failed
// message.

void ah_message(action_t *action)
{
	assert(action);
	assert(0);
	action_pool_return(action);
}







