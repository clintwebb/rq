//-----------------------------------------------------------------------------
// Interface to the RQ blacklist service.  This service takes an IP address and
// looks up a list to see if we should deny it or not.  The library will keep a
// local cache of these addresses that it will keep for a short time so that it
// doesn't need to make a lot of service calls.  When the cached entry expires,
// it will need to remove it from the list and query the blacklist service.
//-----------------------------------------------------------------------------


#include "rq-blacklist.h"

#include <assert.h>
#include <stdlib.h>


#if (RQ_BLACKLIST_VERSION != 0x00010500)
	#error "Incorrect header version"
#endif



typedef struct {
	ev_uint32_t ip;
	rq_blacklist_status_t status;
	time_t expires;
} cache_entry_t;


typedef struct {
	rq_blacklist_id_t id;
	ev_uint32_t ip;
	void *arg;
	rq_blacklist_t *blacklist;
} cache_waiting_t;




void rq_blacklist_init(rq_blacklist_t *blacklist, rq_t *rq, const char *queue, int expires)
{
	assert(blacklist);
	assert(rq);
	assert(queue);
	assert(expires > 0);

	blacklist->rq = rq;
	blacklist->queue = (char *) queue;

  blacklist->expires = expires; 
  
  blacklist->cache = (list_t *) malloc(sizeof(list_t));
  ll_init(blacklist->cache);

  blacklist->waiting = (list_t *) malloc(sizeof(list_t));
  ll_init(blacklist->waiting);

}


void rq_blacklist_free(rq_blacklist_t *blacklist)
{
	cache_entry_t *tmp;
	
	assert(blacklist);
	assert(blacklist->rq);
	assert(blacklist->queue);
	assert(blacklist->expires > 0);
	assert(blacklist->cache);

	blacklist->rq = NULL;
	blacklist->queue = NULL;

	while ((tmp = ll_pop_head(blacklist->cache))) {
		free(tmp);
	}
	ll_free(blacklist->cache);
	free(blacklist->cache);
	blacklist->cache = NULL;
}

// return the next id we can use in the list.  We add new entries to the tail, and expect to fill requests from the head.
static rq_blacklist_id_t next_id(rq_blacklist_t *blacklist)
{
	rq_blacklist_id_t id;
	cache_waiting_t *waiting;

	assert(blacklist);
	assert(blacklist->waiting);

	waiting = ll_get_tail(blacklist->waiting);
	if (waiting) { id = waiting->id + 1; }
	else         { id = 1; }

	return(id);
}


//-----------------------------------------------------------------------------
// Handle the response from the blacklist service.
static void blacklist_handler(rq_message_t *reply, void *arg)
{
	cache_waiting_t *waiting = arg;

	assert(reply);
	assert(waiting);

	// not sure what to do with the reply yet.
	assert(0);
}


//-----------------------------------------------------------------------------
// make a blacklist query and call the handler function when we have an answer.
// 
// NOTE: that it is possible for the callback funcction to be called before
//       this function exits (if the result is already cached), so dont put
//       any important initialization of the passed in object after this call
//       is made.
rq_blacklist_id_t rq_blacklist_check
	(rq_blacklist_t *blacklist, struct sockaddr *address, int socklen, void (*handler)(rq_blacklist_status_t status, void *arg), void *arg)
{
	struct sockaddr_in *sin;
	ev_uint32_t ip;
	cache_entry_t *entry;
	struct timeval tv;
	time_t curtime;
	rq_message_t *msg;
	cache_waiting_t *waiting;
	rq_blacklist_id_t id;

	assert(blacklist);
	assert(address);
	assert(socklen > 0);
	assert(handler);
	assert(arg);

	// convert 'address' into a 32bit uint.
	sin = (struct sockaddr_in *) address;
	ip = sin->sin_addr.s_addr;

	// get the current time in seconds.
	gettimeofday(&tv, NULL);
	curtime=tv.tv_sec;

	// check the cache for the address.
	assert(blacklist->cache);
	ll_start(blacklist->cache);
	entry = ll_next(blacklist->cache);
	while (entry) {
		if (entry->ip == ip) {
			// check to see if entry has expired.
			assert(entry->expires > 0);
			if (entry->expires <= curtime) {
				// cached entry has expired, so we need to remove it from the list.
				ll_remove(blacklist->cache, entry);
				free(entry);
			}
			else {
				// entry is in the list, so we call the handler, and then we return 0.
				handler(entry->status, arg);
				ll_finish(blacklist->cache);
				return(0);
			}
			entry = NULL;
		}
		else {
			entry = ll_next(blacklist->cache);
		}
	}
	ll_finish(blacklist->cache);
	
	// if we got this far, then the entry was not found in the cache, so we need
	// to send a request to the queue.

	// get the next id.
	id = next_id(blacklist);

	// create the structure that will hold the information we are waiting on, and add it to the tail of the list.
	waiting = (cache_waiting_t *)  malloc(sizeof(cache_waiting_t));
	assert(waiting);
	waiting->id = id;
	waiting->ip = ip;
	waiting->arg = arg;
	waiting->blacklist = blacklist;
	ll_push_tail(blacklist->waiting, waiting);

	// now send the message
	assert(blacklist->queue);
	assert(blacklist->rq);
	msg = rq_msg_new(blacklist->rq, NULL);
	rq_msg_setqueue(msg, blacklist->queue);
	
	rq_msg_addcmd(msg, BL_CMD_CLEAR);
	rq_msg_addcmd_int(msg, BL_CMD_IP, ip);
	rq_msg_addcmd(msg, BL_CMD_CHECK);

	// message has been prepared, so send it.
	rq_send(blacklist->rq, msg, blacklist_handler, waiting);
	msg = NULL;

	return(id);
}


void rq_blacklist_cancel(rq_blacklist_t *blacklist, rq_blacklist_id_t id)
{
	assert(blacklist);
	assert(id > 0);

	// go thru the list of pending requests to find this ID.  When it is found, cancel the request to RQ.
	assert(0);
	
	// mark it so that if an RQ reply comes anyway, we dont call the callback (remove the arg pointer)
	assert(0);
}

