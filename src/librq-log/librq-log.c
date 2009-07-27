//-----------------------------------------------------------------------------
// librq-log
// External logging interface used to send log messages to the logging service.
//
// The logger can be used in one of two ways.
//
//  1) It uses an RQ structure that has already been established.  This is the
//     most beneficial way of doing it if you are using an RQ event loop.
//
//  2) Does not use an RQ event loop, but instead connects directly to a
//     controller (using its own socket, and send logs over that socket).
//     This will result in not needing to use an RQ event loop.
//-----------------------------------------------------------------------------


#include "rq-log.h"
#include <assert.h>


#if (RQ_LOG_VERSION != 0x00001000)
	#error "This version designed only for v0.10.00 of librq-log"
#endif


void rq_log_init(rq_log_t *log)
{
	assert(log);
	
	log->rq = NULL;
	log->queue = NULL;
	log->level = 0;
	expbuf_init(&log->buffer, 1024);
}

void rq_log_free(rq_log_t *log)
{
	assert(log);
	
	assert(log->rq == NULL);
	log->queue = NULL;
	expbuf_free(&log->buffer);
}


void rq_log_setqueue(rq_log_t *log, const char *queue)
{
	assert(log != NULL);
	assert(queue != NULL);

	log->queue = (char *)queue;
}


//-----------------------------------------------------------------------------
// Internal function used to send a formatted string to the logger.  It will
// generate all the appropriate commands, and then send it.  If the send was
// not able to complete, then it will add it to the outgoing buffer to be sent
// the next time.
static void rq_log_send(rq_log_t *log, unsigned short level, char *data, int length)
{
	rq_message_t *msg;
	rq_conn_t *conn;
	
	assert(log && data);
	assert(length > 0 && length < 0xffff);
	assert(level > 0);
	
	assert(log->queue);
	assert(log->rq);

	msg = rq_msg_new(log->rq, NULL);
	assert(msg);
	rq_msg_setqueue(msg, log->queue);
	
	rq_msg_addcmd(msg,          LOG_CMD_CLEAR);
	rq_msg_addcmd_shortint(msg, LOG_CMD_LEVEL, level);
	rq_msg_addcmd_str(msg,      LOG_CMD_TEXT, length, data);
	rq_msg_addcmd(msg,          LOG_CMD_EXECUTE);

	// message has been prepared, so send it.
	rq_send(log->rq, msg, NULL, NULL);

	// msg is now controlled by the RQ system, so we set it to null so we dont
	// modify something we no longer control.
	msg = NULL;
}



//-----------------------------------------------------------------------------
// This function is the main public-facing function that will be used.  It will
// take a printf-formatted string (and arguments), add some timestamp, and
// log-level info to it, and then send it to the logging queue.
void rq_log(rq_log_t *log, short int level, char *format, ...)
{
	va_list ap;
	short int done = 0;
	int res;
	expbuf_t *buf;

	assert(log);
	assert(level >= 0 && level < 256);
	assert(format);

	if (level >= log->level) {
		while (done == 0) {

			buf = &log->buffer;
			assert(BUF_LENGTH(buf) == 0 && BUF_MAX(buf) > 0 && BUF_DATA(buf));

			va_start(ap, format);
			res = vsnprintf(BUF_DATA(buf), BUF_MAX(buf), format, ap);
			va_end(ap);

			if (res >= BUF_MAX(buf)) {
				// we didn't have enough room to format all the data, so we need to
				// increase the size of our buffer.  Fortunately, the vsnprintf
				// returns the actual size of the buffer that we will need.  So we
				// simply increase the size of our buffer, and then try again.
				
				assert(BUF_LENGTH(buf) == 0);
				assert(BUF_MAX(buf) > 0);
				assert(BUF_DATA(buf));
				
				expbuf_shrink(buf, res+1);
				assert(done == 0);
			}
			else {
				// we successfully formatted the log output.
				done ++;
				assert(done > 0);
				BUF_LENGTH(buf) = res;
				assert(BUF_LENGTH(buf) < BUF_MAX(buf));
				assert(BUF_DATA(buf)[BUF_LENGTH(buf)] == '\0');
					
				// we dont need to do any trimmings, so we send it now.
				rq_log_send(log, level, BUF_DATA(buf), BUF_LENGTH(buf));
				expbuf_clear(buf);
			}
		}
	}
	
	assert(BUF_LENGTH(&log->buffer) == 0 && BUF_MAX(&log->buffer) > 0 && BUF_DATA(&log->buffer) != NULL);
}



