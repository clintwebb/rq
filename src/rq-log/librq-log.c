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

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <rispbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>


void rq_log_init(rq_log_t *log)
{
	assert(log != NULL);
	log->rq = NULL;
	log->handle = INVALID_HANDLE;
	log->host = NULL;
	log->port = RQ_DEFAULT_PORT;
	log->queue = NULL;
	log->level = 0;
	log->text = NULL;
	log->flags = 0;
	expbuf_init(&log->pending, 0);
	expbuf_init(&log->buffer, 1024);
	expbuf_init(&log->formatted, 0);
	expbuf_init(&log->packet, 0);
}

void rq_log_free(rq_log_t *log)
{
	assert(log != NULL);
	assert(log->rq == NULL);
	assert(log->handle == INVALID_HANDLE);
	log->queue = NULL;
	expbuf_free(&log->pending);
	expbuf_free(&log->buffer);
	expbuf_free(&log->formatted);
	expbuf_free(&log->packet);
}


void rq_log_setqueue(rq_log_t *log, const char *queue)
{
	assert(log != NULL);
	assert(queue != NULL);

	log->queue = (char *)queue;
}


int rq_log_connect(rq_log_t *log, const char *_host, int _port)
{
	int connected = -1;
	struct sockaddr_in sin;
	unsigned long ulAddress;
	struct hostent *hp;
	int opts;

	assert(log != NULL);
	assert((log->host == NULL && _host != NULL) || (log->host != NULL));

	if (_host != NULL) {
		assert(_port > 0);
		log->host = (char *) _host;
		if (_port > 0) log->port = _port;
	}
	
	assert(log->handle == INVALID_HANDLE);
	assert(log->rq == NULL);

	// First, assign the family and port.
	sin.sin_family = AF_INET;
	sin.sin_port = htons(log->port);

	// Look up by standard notation (xxx.xxx.xxx.xxx) first.
	ulAddress = inet_addr(log->host);
	if ( ulAddress != (unsigned long)(-1) )  {
		// Success. Assign, and we're done.  Since it was an actual IP address,
		// then we dont doany DNS lookup for that, so we can t do any checking for
		// any other address type (such as MX).
		sin.sin_addr.s_addr = ulAddress;
	}
	else {
		// If that didn't work, try to resolve host name by DNS.
    hp = gethostbyname(log->host);
    if( hp == NULL ) {
       // Didn't work. We can't resolve the address.
			return -1;
    }

    // Otherwise, copy over the converted address and return success.
    memcpy( &(sin.sin_addr.s_addr), &(hp->h_addr[0]), hp->h_length);
	}

	// Create the socket
	assert(log->handle == INVALID_HANDLE);
	log->handle = socket(AF_INET, SOCK_STREAM, 0);
	if (log->handle >= 0) {
		// Connect to the server
		if (connect(log->handle, (struct sockaddr*)&sin, sizeof(struct sockaddr)) >= 0) {

			connected = 0;

			assert(log->handle > 0);
			opts = fcntl(log->handle, F_GETFL);
			if (opts >= 0) {
				opts = (opts | O_NONBLOCK);
				fcntl(log->handle, F_SETFL, opts);
			}
		}
		else {
			printf("connect fail.\n");
			close(log->handle);
			log->handle = INVALID_HANDLE;
		}
	}

	return(connected);
}



// this function is only used in 'direct' mode.  It takes a formed packet in
// 'buffer' and adds it as a Queue broadcaste message into the 'pending'
// 'buffer.  'buffer' is not cleared.
static void add_pending_packet(rq_log_t *log)
{
	assert(log != NULL);
	assert(log->rq == NULL);

	assert(log->packet.length > 0);
	assert(log->queue != NULL);

	addCmd(&log->pending, RQ_CMD_CLEAR);
	addCmd(&log->pending, RQ_CMD_BROADCAST);
	addCmd(&log->pending, RQ_CMD_NOREPLY);
	addCmdShortStr(&log->pending, RQ_CMD_QUEUE, strlen(log->queue), log->queue);
	addCmdLargeStr(&log->pending, RQ_CMD_PAYLOAD, log->packet.length, log->packet.data);
	addCmd(&log->pending, RQ_CMD_EXECUTE);

	assert(log->pending.length > 0 && log->pending.max >= log->pending.length && log->pending.data != NULL);
}




// Internal function used to send a formatted string to the logger.  It will generate all the appropriate commands, and then send it.  If the send was not able to complete, then it will add it to the outgoing buffer to be sent the next time.
static void rq_log_send(rq_log_t *log, unsigned short level, char *data, int length)
{
	int res;
	int start;
	rq_message_t *msg;
	
	assert(log != NULL);
	assert(data != NULL);
	assert(length > 0);
	assert(length < 0xffff);
	assert(level > 0);

	assert((log->rq != NULL && log->pending.length == 0) || (log->rq == NULL));
	assert(log->queue != NULL);

	// we need to know if data is already in the 'pending' buffer.
	start = log->pending.length;


	// to make things a little faster, we should pre-allocate size for our buffer.  Not necessary, but will improve performance a bit.
	assert(log->packet.length == 0);
	expbuf_shrink(&log->packet, 8+length);
	
	addCmd(&log->packet, LOG_CMD_CLEAR);
	
// 	assert(LOG_CMD_LEVEL >= 98 && LOG_CMD_LEVEL <= 127);
	
	addCmdShortInt(&log->packet, LOG_CMD_LEVEL, level);
	addCmdStr(&log->packet, LOG_CMD_TEXT, length, data);
	addCmd(&log->packet, LOG_CMD_EXECUTE);

// 	assert(log->packet.length == 8+length);

	if (log->rq != NULL) {
		// we are using an RQ event queue.

		// create the message object.
		assert(log->pending.length == 0);
		msg = (rq_message_t *) malloc(sizeof(rq_message_t));
		assert(msg != NULL);
		rq_message_init(msg);
		rq_message_setqueue(msg, log->queue);
		rq_message_setbroadcast(msg);
		rq_message_setnoreply(msg);
		rq_message_setdata(msg, log->packet.length, log->packet.data);

		// the memory used in the packet buffer is now being controlled by the
		// message object.  So we re-initialise the buffer.
		expbuf_init(&log->packet, 0);
		
		// message has been prepared, so send it.  
		rq_send(log->rq, msg);

		// msg is now controlled by the RQ system, so we set it to null so we dont
		// modify something we no longer control.
		msg = NULL;		
	}
	else {

		if (log->handle == INVALID_HANDLE) {
			rq_log_connect(log, NULL, 0);
		}

		add_pending_packet(log);
		
		if (log->handle != INVALID_HANDLE) {
			assert(log->handle > 0);
			res = send(log->handle, log->pending.data, log->pending.length, 0);
			if (res > 0) {
				// we managed to send some, or maybe all....
				assert(res <= log->pending.length);
				expbuf_purge(&log->pending, res);
			}
			else if (res == 0) {
				// the connection was closed.  Since it was closed and we dont want to
				// send partial data, we need to clear it if we started off with pending
				// data.
				log->handle = INVALID_HANDLE;
				assert(start >= 0);
				if (start > 0) {
					expbuf_clear(&log->pending);
					start = 0;
					assert(log->packet.length > 0);
					add_pending_packet(log);
				}
			}
			else {
				assert(res == -1);
				if (errno != EAGAIN && errno != EWOULDBLOCK) {
					log->handle = INVALID_HANDLE;
					assert(start >= 0);
					if (start > 0) {
						expbuf_clear(&log->pending);
						start = 0;
						assert(log->packet.length > 0);
						add_pending_packet(log);
					}
				}
			}
		}
	}

	expbuf_clear(&log->packet);
	assert(log->packet.length == 0);
}

#define DEFAULT_TRIM_SIZE		1024

void rq_log(rq_log_t *log, short int level, char *format, ...)
{
	va_list ap;
	short int done = 0;
	int res;
	char trim[DEFAULT_TRIM_SIZE];
	struct timeval tv;
	struct timezone tz;
	struct tm tm;

	assert(log != NULL);
	assert(level >= 0 && level < 256);
	assert(format != NULL);

	if (level >= log->level) {
		while (done == 0) {
			
			assert(log->buffer.length == 0);
			assert(log->buffer.max > 0);
			assert(log->buffer.data != NULL);

			va_start(ap, format);
			res = vsnprintf(log->buffer.data, log->buffer.max, format, ap);
			va_end(ap);

			if (res >= log->buffer.max) {
				// we didn't have enough room to format all the data, so we need to
				// increase the size of our buffer.  Fortunately, the vsnprintf
				// returns the actual size of the buffer that we will need.  So we
				// simply increase the size of our buffer, and then try again.
				
				assert(log->buffer.length == 0);
				assert(log->buffer.max > 0);
				assert(log->buffer.data != NULL);
				
				expbuf_shrink(&log->buffer, res+1);
				assert(done == 0);
			}
			else {
				// we successfully formatted the log output.
				done ++;
				log->buffer.length = res;
				assert(log->buffer.length < log->buffer.max);
				assert(log->buffer.data[log->buffer.length] == '\0');
				
				// now we need to add the trimmings to the log output.
				if (log->flags == 0) {
					// we dont need to do any trimmings, so we send it now.
					rq_log_send(log, level, log->buffer.data, log->buffer.length);
					expbuf_clear(&log->buffer);
				}
				else {
					// we need to add the trimmings.

					assert(log->formatted.length == 0);

					if (log->flags & LOG_FLAG_DATESTAMP) {
						// get the current date, and add it to the 'formatted' buffer.
						
						if(gettimeofday(&tv, &tz) == 0) {
            	localtime_r(&(tv.tv_sec), &tm);
              snprintf(trim, DEFAULT_TRIM_SIZE, "%04d%02d%02d-%02d%02d%02d ",
                            tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

            	expbuf_add(&log->formatted, trim, strlen(trim));
						}
					}

					if (log->flags & LOG_FLAG_TEXT) {
						assert(log->text != NULL);
						expbuf_add(&log->formatted, log->text, strlen(log->text));
					}

					if (level == LOG_DEBUG) {
						expbuf_add(&log->formatted, LOG_DEBUG_T, strlen(LOG_DEBUG_T));
					}
					else if (level == LOG_INFO) {
						expbuf_add(&log->formatted, LOG_INFO_T, strlen(LOG_INFO_T));
					}
					else if (level == LOG_WARN) {
						expbuf_add(&log->formatted, LOG_WARN_T, strlen(LOG_WARN_T));
					}
					else if (level == LOG_ERROR) {
						expbuf_add(&log->formatted, LOG_ERROR_T, strlen(LOG_ERROR_T));
					}
					else if (level == LOG_FATAL) {
						expbuf_add(&log->formatted, LOG_FATAL_T, strlen(LOG_FATAL_T));
					}
					else {
						expbuf_add(&log->formatted, LOG_UNKNOWN_T, strlen(LOG_UNKNOWN_T));
					}

					expbuf_add(&log->formatted, log->buffer.data, log->buffer.length);
					expbuf_clear(&log->buffer);

					rq_log_send(log, level, log->formatted.data, log->formatted.length);
					expbuf_clear(&log->formatted);
				}
			}
		}
	}
	
	assert(log->buffer.length == 0 && log->buffer.max > 0 && log->buffer.data != NULL);
}



