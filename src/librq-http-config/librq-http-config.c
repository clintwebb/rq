//-----------------------------------------------------------------------------
// librq-http-config
//
//	This library is used by 

//-----------------------------------------------------------------------------


#include "rq-http-config.h"

#if (RQ_HTTP_CONFIG_VERSION != 0x00010100)
	#error "Incorrect header version"
#endif


#include <assert.h>


//-----------------------------------------------------------------------------
void rq_hcfg_init(rq_hcfg_t *cfg, rq_t *rq, const char *queue)
{
	assert(cfg && rq && queue);

	cfg->rq = rq;
  cfg->queue = (char *) queue;
}

//-----------------------------------------------------------------------------
void rq_hcfg_free(rq_hcfg_t *cfg)
{
	assert(cfg);

	cfg->rq = NULL;
	cfg->queue = NULL;
}



//-----------------------------------------------------------------------------
// Lookup a host/path combo and call the handler when the information is
// available.  If the information was available in the cache, call the handler
// straight away, and then return a 0.  If we need to send a query to the
// config server, then return an id (that is greater than zero) which can be
// used to cancel the request (due to lost connection, or a blacklist deny,
// etc).
rq_hcfg_id_t rq_hcfg_lookup(rq_hcfg_t *cfg, const char *host, const char *path, void (*handler)(const char *queue, void *arg), void *arg)
{
	assert(cfg && host && path && handler);

	// process the path, to remove any arguments.
	assert(0);

	// if we have the host in the cache...
		// if we have the path in the cache
			// call the handler, return 0.
			assert(0);

	// otherwise, send a request to the config server.
		// create struture.
		// send request.
		// return id.
		assert(0);
}

void rq_hcfg_cancel_lookup(rq_hcfg_t *cfg, rq_hcfg_id_t id)
{
	assert(cfg);
	assert(id > 0);

	// look in the list of pending requests.

	// if id is in there cancel the request.
	assert(0);

	// remove entry from the list.
	assert(0);
	
}




//-----------------------------------------------------------------------------
// Internal function used to send a formatted string to the logger.  It will
// generate all the appropriate commands, and then send it.  If the send was
// not able to complete, then it will add it to the outgoing buffer to be sent
// the next time.
#if (0)
static void rq_log_send(rq_log_t *log, unsigned short level, char *data, int length)
{
	rq_message_t *msg;
	
	assert(log && data);
	assert(length > 0 && length < 0xffff);
	assert(level > 0);
	
	assert(log->queue);
	assert(log->rq);

	msg = rq_msg_new(log->rq);
	assert(msg);
	rq_msg_setqueue(msg, log->queue);
	
	rq_msg_addcmd(msg, LOG_CMD_CLEAR);
	rq_msg_addcmd_shortint(msg, LOG_CMD_LEVEL, level);
	rq_msg_addcmd_str(msg, LOG_CMD_TEXT, length, data);
	rq_msg_addcmd(msg, LOG_CMD_EXECUTE);

	// message has been prepared, so send it.
	rq_send(log->rq, msg, NULL, NULL);

	// msg is now controlled by the RQ system, so we set it to null so we dont
	// modify something we no longer control.
	msg = NULL;
}
#endif // deleted
