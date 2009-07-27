// librq
// RISP-based queue system

#include "rq.h"

#include <rispbuf.h>

#include <assert.h>
#include <errno.h>
#include <event.h>
#include <fcntl.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>


#if (LIBRQ_VERSION != 0x00010505)
	#error "Incorrect rq.h header version."
#endif



// pre-declare the handlers otherwise we end up with a precedence loop.
static void rq_read_handler(int fd, short int flags, void *arg);
static void rq_write_handler(int fd, short int flags, void *arg);
static void rq_connect_handler(int fd, short int flags, void *arg);



typedef struct {
	struct event clockevent;
	void *handler;
	void *arg;
} rq_timeout_t;



//-----------------------------------------------------------------------------
// Since we will be limiting the number of connections we have, we will want to 
// make sure that the required number of file handles are avaialable.  For a 
// 'server', the default number of file handles per process might not be 
// 'enough, and this function will attempt to increase them, if necessary.
void rq_set_maxconns(int maxconns) 
{
	struct rlimit rlim;
	
	assert(maxconns > 5);

	if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
		fprintf(stderr, "failed to getrlimit number of files\n");
		exit(1);
	} else {
	
		// we need to allocate twice as many handles because we may be receiving data from a file for each node.
		if (rlim.rlim_cur < maxconns)
			rlim.rlim_cur = (2 * maxconns) + 3;
			
		if (rlim.rlim_max < rlim.rlim_cur)
			rlim.rlim_max = rlim.rlim_cur;
		if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
			fprintf(stderr, "failed to set rlimit for open files. Try running as root or requesting smaller maxconns value.\n");
			exit(1);
		}
	}
}

void rq_daemon(const char *username, const char *pidfile, const int noclose)
{
	struct passwd *pw;
	struct sigaction sa;
	int fd;
	FILE *fp;
	
	if (username != NULL) {
		assert(username[0] != '\0');
		if (getuid() == 0 || geteuid() == 0) {
			if (username == 0 || *username == '\0') {
				fprintf(stderr, "can't run as root without the -u switch\n");
				exit(EXIT_FAILURE);
			}
			pw = getpwnam((const char *)username);
			if (pw == NULL) {
				fprintf(stderr, "can't find the user %s to switch to\n", username);
				exit(EXIT_FAILURE);
			}
			if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
				fprintf(stderr, "failed to assume identity of user %s\n", username);
				exit(EXIT_FAILURE);
			}
		}
	}

	sa.sa_handler = SIG_IGN;
	sa.sa_flags = 0;
	if (sigemptyset(&sa.sa_mask) == -1 || sigaction(SIGPIPE, &sa, 0) == -1) {
		perror("failed to ignore SIGPIPE; sigaction");
		exit(EXIT_FAILURE);
	}

	switch (fork()) {
		case -1:
			exit(EXIT_FAILURE);
		case 0:
			break;
		default:
			_exit(EXIT_SUCCESS);
	}

	if (setsid() == -1)
			exit(EXIT_FAILURE);

	(void)chdir("/");

	if (noclose == 0 && (fd = open("/dev/null", O_RDWR, 0)) != -1) {
		(void)dup2(fd, STDIN_FILENO);
		(void)dup2(fd, STDOUT_FILENO);
		(void)dup2(fd, STDERR_FILENO);
		if (fd > STDERR_FILENO)
			(void)close(fd);
	}

	// save the PID in if we're a daemon, do this after thread_init due to a
	// file descriptor handling bug somewhere in libevent
	if (pidfile != NULL) {
		if ((fp = fopen(pidfile, "w")) == NULL) {
			fprintf(stderr, "Could not open the pid file %s for writing\n", pidfile);
			exit(EXIT_FAILURE);
		}

		fprintf(fp,"%ld\n", (long)getpid());
		if (fclose(fp) == -1) {
			fprintf(stderr, "Could not close the pid file %s.\n", pidfile);
			exit(EXIT_FAILURE);
		}
	}
}


//-----------------------------------------------------------------------------
// Given an address structure, will create a socket handle and set it for 
// non-blocking mode.
int rq_new_socket(struct addrinfo *ai) {
	int sfd = INVALID_HANDLE;
	int flags;
	
	assert(ai != NULL);
	
	// bind the socket, and then set the socket to non-blocking mode.
	if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) >= 0) {
		// TODO: use libevent non-blocking function instead.
		if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 || fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
			perror("setting O_NONBLOCK");
			close(sfd);
			sfd = INVALID_HANDLE;
		}
	}
	
	return sfd;
}






static void rq_data_init(rq_data_t *data, expbuf_pool_t *pool)
{
	assert(data && pool);
	
	data->flags = 0;
	data->mask = 0;
	
	data->id = 0;
	data->qid = 0;
	data->timeout = 0;
	data->priority = 0;

	// we dont allocate the payload object yet, because we need to move it to
	// the message processing it.  Therefore, we will always get new payload
	// buffers from the bufpool when needed.
	data->payload = NULL;
	
	data->queue = expbuf_pool_new(pool, 0);
	assert(data->queue);
}

static void rq_data_free(rq_data_t *data, expbuf_pool_t *pool)
{
	assert(data && pool);

	if (data->payload) {
		expbuf_clear(data->payload);
		expbuf_pool_return(pool, data->payload);
		data->payload = NULL;
	}

	assert(data->queue);
	expbuf_clear(data->queue);
	expbuf_pool_return(pool, data->queue);
	data->queue = NULL;
}



void rq_queue_init(rq_queue_t *queue)
{
	assert(queue != NULL);

	queue->queue = NULL;
	queue->qid = 0;
	queue->handler = NULL;
	queue->arg = NULL;
}

void rq_queue_free(rq_queue_t *queue)
{
	assert(queue != NULL);
	if (queue->queue != NULL) {
		free(queue->queue);
		queue->queue = NULL;
	}
	queue->qid = 0;
	queue->handler = NULL;
	queue->arg = NULL;
}







//-----------------------------------------------------------------------------
// Assuming that the rq structure has been properlly filled out, this function
// will initiate the connection process to a specified IP address.   Since the
// application may be using the event loop for other things, and since we need
// to support primary and secondary controllers, we need to connect in
// non-blocking mode.
//
// This function will ONLY attempt to connect to the connection at the top of
// the list.  To cycle through to a different controller, some other
// functionality will need to move the top conn to the tail.  This would either
// be because the controller sent a CLOSING instruction, or the socket
// connection failed.   This means that if the connection is dropped, that
// functionality should move the current conn to the tail, and the next connect
// attempt would be against the alternate controller.
static void rq_connect(rq_t *rq)
{
	rq_conn_t *conn;
	struct sockaddr saddr;
	int result;
	int len;

	assert(rq != NULL);

	assert(rq->evbase != NULL);
	assert(ll_count(&rq->connlist) > 0);

	// make a note of the first connection in the list.
	conn = ll_get_head(&rq->connlist);

	if (conn->shutdown == 0 && conn->closing == 0 && conn->connect_event == NULL && conn->active == 0) {
	
		assert(conn->hostname != NULL);
		assert(conn->hostname[0] != '\0');
		assert(conn->read_event == NULL);
		assert(conn->write_event == NULL);
		assert(conn->connect_event == NULL);
		assert(conn->handle == INVALID_HANDLE);
		
		len = sizeof(saddr);
		if (evutil_parse_sockaddr_port(conn->hostname, &saddr, &len) != 0) {
			// unable to parse the detail.  What do we need to do?
			assert(0);
		}
		else {

			// create the socket, and set to non-blocking mode.
									
			conn->handle = socket(AF_INET,SOCK_STREAM,0);
			assert(conn->handle >= 0);
			evutil_make_socket_nonblocking(conn->handle);

			result = connect(conn->handle, &saddr, sizeof(saddr));
			assert(result < 0);
			assert(errno == EINPROGRESS);
	
			assert(conn->inbuf == NULL);
			assert(conn->outbuf == NULL);
			assert(conn->readbuf == NULL);
			assert(conn->in_msgs == NULL);
			assert(conn->out_msgs == NULL);

			assert(conn->data == NULL);
	
			// connect process has been started.  Now we need to create an event so that we know when the connect has completed.
			assert(conn->rq);
			assert(conn->rq->evbase);
			conn->connect_event = event_new(conn->rq->evbase, conn->handle, EV_WRITE, rq_connect_handler, conn);
			assert(conn->connect_event);
			event_add(conn->connect_event, NULL);	// TODO: Should we set a timeout on the connect?
		}
	}
}



//-----------------------------------------------------------------------------
// This function is called only when we lose a connection to the controller.
// Since the connection to the controller has failed in some way, we need to
// move that connection to the tail of the list.
static void rq_conn_closed(rq_conn_t *conn)
{
	rq_message_t *msg;
	
	assert(conn);
	assert(conn->rq);
	assert(conn->rq->bufpool);

	assert(conn->handle != INVALID_HANDLE);
	close(conn->handle);
	conn->handle = INVALID_HANDLE;

	// free all the buffers.
	if (conn->readbuf) {
		assert(BUF_LENGTH(conn->readbuf) == 0);
		expbuf_pool_return(conn->rq->bufpool, conn->readbuf);
		conn->readbuf = NULL;
	}

	if (conn->inbuf) {
		expbuf_clear(conn->inbuf);
		expbuf_pool_return(conn->rq->bufpool, conn->inbuf);
		conn->inbuf = NULL;
	}
	
	if (conn->outbuf) {
		expbuf_clear(conn->outbuf);
		expbuf_pool_return(conn->rq->bufpool, conn->outbuf);
		conn->outbuf = NULL;
	}

	// cleanup the data structure.
	if (conn->data) {
		rq_data_free(conn->data, conn->rq->bufpool);
		free(conn->data);
		conn->data = NULL;
	}

	// remove the conn from the connlist.  It should actually be the highest one in the list, and then put it at the tail of the list.
	assert(conn->rq);
	assert(ll_count(&conn->rq->connlist) > 0);
	if (ll_count(&conn->rq->connlist) > 1) {
		ll_remove(&conn->rq->connlist, conn);
		ll_push_tail(&conn->rq->connlist, conn);
	}

	// clear the events
	if (conn->read_event) {
		event_free(conn->read_event);
		conn->read_event = NULL;
	}
	if (conn->write_event) {
		event_free(conn->write_event);
		conn->write_event = NULL;
	}
	assert(conn->connect_event == NULL);

	// timeout all the pending messages, if there are any.
	if (conn->in_msgs) {
		while ((msg = ll_pop_head(conn->in_msgs))) {
			assert(0);
		}
	}

	if (conn->out_msgs) {
		while ((msg = ll_pop_head(conn->out_msgs))) {
			assert(0);
		}
	}

	conn->active = 0;
	conn->closing = 0;

	// initiate a connect on the head of the list.
	assert(conn->rq);
	rq_connect(conn->rq);
}


// this function is used internally to send the data to the connected RQ
// controller.  If there is no data currently in the outbound queue, then we
// will try and send it straight away.   If we couldn't send any or all of it,
// then we add it to the out queue.  If there was already data in the out
// queue, then we add this data to the out queue and wait let the event system
// take care of it.
static void rq_senddata(rq_conn_t *conn, char *data, int length)
{
	int res = 0;
	
	assert(conn);
	assert(data);
	assert(length > 0);

	assert(conn->handle != INVALID_HANDLE);

	// if the out buffer is empty, then we will try and send what we've got straight away.
	if (conn->active > 0 && conn->outbuf == NULL) {
		res = send(conn->handle, data, length, 0);
		if (res > 0) {
			assert(res <= length);
		}
		else 	if (res == 0) {
			goto err;
		}
		else if (res == -1) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				goto err;
			}
		}
	}

	// if the connection is still valid, and we haven't sent everything yet,
	// then we need to add it to the outgoing queue, and then raise an event.
	assert(conn->handle != INVALID_HANDLE);
	if (res < length) {

		assert(res >= 0);

		if (conn->outbuf == NULL) {
			assert(conn->rq);
			assert(conn->rq->bufpool);
			conn->outbuf = expbuf_pool_new(conn->rq->bufpool, (length-res));
			assert(conn->outbuf);
		}
		
		expbuf_add(conn->outbuf, data+res, length-res);
		assert(conn->rq->evbase);
		assert(conn->read_event);
		assert(conn->write_event == NULL);
		conn->write_event = event_new(conn->rq->evbase, conn->handle, EV_WRITE | EV_PERSIST, rq_write_handler, conn);
		event_add(conn->write_event, NULL);
	}

	return;

err:
	assert(conn);
	rq_conn_closed(conn);
}


//-----------------------------------------------------------------------------
// Send a message to the controller that basically states that this client is
// closing its connection.  Since we are sending a single command we dont need
// to go through the expence of gettng a buffer from the bufpool.
static void rq_send_closing(rq_conn_t *conn)
{
	char buf[1];
	
	buf[0] = RQ_CMD_CLOSING;
	assert(conn);
	rq_senddata(conn, buf, 1);
}



//-----------------------------------------------------------------------------
// Used to tell the library that it needs to start shutting down connections so
// that the application can exit the loop.
void rq_shutdown(rq_t *rq)
{
	rq_conn_t *conn;
	int pending;
	
	assert(rq);

	// go thru the connect list, and tell each one that it is shutting down.
	ll_start(&rq->connlist);
	while ((conn = ll_next(&rq->connlist))) {

		// Because the list entries may move around while being processed, we may
		// need to restart the list from the head again.  Therefore we need to
		// check to see if we have already processed this connection.
		if (conn->shutdown == 0) {
			conn->shutdown ++;
	
			if (conn->handle != INVALID_HANDLE) {
	
				// if we have a handle, but not marked as active, then we must still be connecting.
				if (conn->active == 0) {
					assert(conn->closing == 0);
					
					// need to close the connection, and remove the connect event.
					assert(conn->connect_event);
					event_free(conn->connect_event);
					conn->connect_event = NULL;
					rq_conn_closed(conn);
					assert(conn->closing == 0);

					// closing the connection would have moved the conns around in the
					// list, so we need to reset the 'next'.  This means the loop will
					// restart again, but we wont process the ones that we have already
					// marked as 'shutdown'
					ll_finish(&rq->connlist);
					ll_start(&rq->connlist);
				}
				else {
					assert(conn->active > 0);
					assert(conn->connect_event == NULL);
					assert(conn->read_event);
					
					// send 'closing' message to each connected controller.  This should remove the node from any queues it is consuming.
					rq_send_closing(conn);
	
					assert(conn->closing == 0);
					conn->closing ++;
				
					// we need to wait if we are still processing some messages from a queue being consumed.
					pending = 0;
					if (conn->in_msgs) {
						pending = ll_count(conn->in_msgs);
						assert(0);
					}
			
					// we need to wait if there are replies we haven't heard from yet.
					if (conn->out_msgs) {
						pending += ll_count(conn->out_msgs);
						assert(0);
					}
		
					// close connections if there are no messages waiting to be processed on it.
					if (pending == 0) {
						rq_conn_closed(conn);
						assert(conn->closing == 0);
						
						// closing the connection would have moved the conns around in the
						// list, so we need to reset the 'next'.  This means the loop will
						// restart again, but we wont process the ones that we have already
						// marked as 'shutdown'
						ll_finish(&rq->connlist);
						ll_start(&rq->connlist);
					}
				}
			}
		}
	}
	ll_finish(&rq->connlist);
}


void rq_cleanup(rq_t *rq)
{
	rq_queue_t *q;
	rq_conn_t *conn;
	
	assert(rq != NULL);

	// cleanup the risp object.
	assert(rq->risp != NULL);
	risp_shutdown(rq->risp);
	free(rq->risp);
	rq->risp = NULL;

	// free the resources allocated for the connlist.
	while ((conn = ll_pop_head(&rq->connlist))) {
		assert(conn->handle == INVALID_HANDLE);
		assert(conn->active == 0);
// 		assert(conn->closing == 0);
		assert(conn->shutdown > 0);

		assert(conn->read_event == NULL);
		assert(conn->write_event == NULL);
		assert(conn->connect_event == NULL);

		conn->rq = NULL;
		conn->risp = NULL;
	
		assert(conn->hostname);
		free(conn->hostname);
		conn->hostname = NULL;

		assert(conn->inbuf == NULL);
		assert(conn->outbuf == NULL);
		assert(conn->readbuf == NULL);

		assert(conn->data == NULL);
	
		// linked-lists of the messages that are being processed.
		assert(conn->in_msgs == NULL);
		assert(conn->out_msgs == NULL);
	}
	ll_free(&rq->connlist);

	// cleanup all the queues that we have.
	while ((q = ll_pop_head(&rq->queues))) {
		rq_queue_free(q);
		free(q);
	}

	// cleanup the msgpool
	assert(rq->msgpool);
	mempool_free(rq->msgpool);
	free(rq->msgpool);
	rq->msgpool = NULL;

	assert(rq->bufpool);
	expbuf_pool_free(rq->bufpool);
	free(rq->bufpool);
	rq->bufpool = NULL;
}


void rq_setevbase(rq_t *rq, struct event_base *base)
{
	assert(rq);

	if (base) {
		assert(rq->evbase == NULL);
		rq->evbase = base;
	}
	else {
		assert(rq->evbase);
		rq->evbase = NULL;
	}
}






// this function is an internal one taht is used to read data from the socket.
// It is assumed that we are pretty sure that there is data to be read (or the
// socket has been closed).
static void rq_process_read(rq_conn_t *conn)
{
	int res, empty;
	
	assert(conn);
	assert(conn->rq);
	assert(conn->risp);
	assert(conn->readbuf);
	
	assert(BUF_LENGTH(conn->readbuf) == 0);
	assert(BUF_MAX(conn->readbuf) >= RQ_DEFAULT_BUFFSIZE);

	empty = 0;
	while (empty == 0) {
		assert(BUF_LENGTH(conn->readbuf) == 0);
		assert(conn->handle != INVALID_HANDLE && conn->handle > 0);
		assert(BUF_DATA(conn->readbuf) != NULL  && BUF_MAX(conn->readbuf) > 0);
		
		res = read(conn->handle, BUF_DATA(conn->readbuf), BUF_MAX(conn->readbuf));
		if (res > 0) {
			BUF_LENGTH(conn->readbuf) = res;
			assert(BUF_LENGTH(conn->readbuf) <= BUF_MAX(conn->readbuf));

			// if we pulled out the max we had avail in our buffer, that means we
			// can pull out more at a time, so we should increase our buffer size by
			// RQ_DEFAULT_BUFFSIZE amount.  This will increase the size of the
			// buffer in rather small chunks, which might not be optimal.
			if (res == BUF_MAX(conn->readbuf)) {
				expbuf_shrink(conn->readbuf, RQ_DEFAULT_BUFFSIZE);
				assert(empty == 0);
				fprintf(stderr, "Increased readbuf to: %d\n", BUF_MAX(conn->readbuf));
			}
			else { empty = 1; }
			
			// if there is no data in the in-buffer, then we will process the common buffer by itself.
			if (conn->inbuf == NULL) {
				res = risp_process(conn->risp, conn, BUF_LENGTH(conn->readbuf), (unsigned char *) BUF_DATA(conn->readbuf));
				assert(res <= BUF_LENGTH(conn->readbuf));
				assert(res >= 0);
				if (res > 0) { expbuf_purge(conn->readbuf, res); }

				// if there is data left over, then we need to add it to our in-buffer.
				if (BUF_LENGTH(conn->readbuf) > 0) {
					assert(conn->inbuf == NULL);
					conn->inbuf = expbuf_pool_new(conn->rq->bufpool, BUF_LENGTH(conn->readbuf));
					assert(conn->inbuf);
					
					expbuf_add(conn->inbuf, BUF_DATA(conn->readbuf), BUF_LENGTH(conn->readbuf));
					expbuf_clear(conn->readbuf);
				}
			}
			else {
				// we have data left in the in-buffer, so we add the content of the common buffer
				assert(BUF_LENGTH(conn->readbuf) > 0);
				assert(conn->inbuf);
				assert(BUF_LENGTH(conn->inbuf) > 0);
				expbuf_add(conn->inbuf, BUF_DATA(conn->readbuf), BUF_LENGTH(conn->readbuf));
				expbuf_clear(conn->readbuf);
				assert(BUF_LENGTH(conn->readbuf) == 0);
				assert(BUF_LENGTH(conn->inbuf) > 0 && BUF_DATA(conn->inbuf) != NULL);

				// and then process what is there.
				res = risp_process(conn->risp, conn, BUF_LENGTH(conn->inbuf), (unsigned char *) BUF_DATA(conn->inbuf));
				assert(res <= BUF_LENGTH(conn->inbuf));
				assert(res >= 0);
				if (res > 0) { expbuf_purge(conn->inbuf, res); }

				if (BUF_LENGTH(conn->inbuf) == 0) {
					// the in buffer is now empty, so we should return it to the pool.
					expbuf_pool_return(conn->rq->bufpool, conn->inbuf);
					conn->inbuf = NULL;
				}
			}
		}
		else {
			assert(empty == 0);
			empty = 1;
			
			if (res == 0) {
				rq_conn_closed(conn);
			}
			else {
				assert(res == -1);
				if (errno != EAGAIN && errno != EWOULDBLOCK) {
					rq_conn_closed(conn);
				}
			}
		}
	}

	assert(conn->readbuf);
	assert(BUF_LENGTH(conn->readbuf) == 0);
}




static void rq_write_handler(int fd, short int flags, void *arg)
{
	rq_conn_t *conn = (rq_conn_t *) arg;

	assert(fd >= 0);
	assert(flags != 0);
	assert(conn);
	assert(conn->rq);
	assert(conn->handle == fd);
	assert(conn->active > 0);
	assert(flags & EV_WRITE);
	assert(conn->write_event);
	
	// incomplete
	assert(0);

	// if we dont have any more to send, then we need to remove the WRITE event.
	assert(conn->outbuf);
	if (BUF_LENGTH(conn->outbuf) == 0) {
		event_free(conn->write_event);
		conn->write_event = NULL;
	}
}







//-----------------------------------------------------------------------------
// This internal function is used to actually send a queue consume request 
static void rq_send_consume(rq_conn_t *conn, rq_queue_t *queue)
{
	expbuf_t *buf;
	
	assert(conn);
	assert(queue);

	assert(queue->queue);
	assert(queue->max >= 0);
	assert(strlen(queue->queue) > 0 && strlen(queue->queue) < 256);

	// get a buffer from the bufpool.
	assert(conn->rq);
	assert(conn->rq->bufpool);
	buf = expbuf_pool_new(conn->rq->bufpool, 32);

	// send consume request to controller.
	addCmd(buf, RQ_CMD_CLEAR);
	if (queue->exclusive != 0)
		addCmd(buf, RQ_CMD_EXCLUSIVE);
	addCmdShortStr(buf, RQ_CMD_QUEUE, strlen(queue->queue), queue->queue);
	addCmdInt(buf, RQ_CMD_MAX, queue->max);
	addCmdShortInt(buf, RQ_CMD_PRIORITY, queue->priority);
	addCmd(buf, RQ_CMD_CONSUME);

	rq_senddata(conn, BUF_DATA(buf), BUF_LENGTH(buf));
	expbuf_clear(buf);
	
	// return the buffer to the bufpool.
	expbuf_pool_return(conn->rq->bufpool, buf);
}


static void rq_connect_handler(int fd, short int flags, void *arg)
{
	rq_conn_t *conn = (rq_conn_t *) arg;
	rq_queue_t *q;
	socklen_t foo;
	int error;

	assert(fd >= 0);
	assert(flags != 0);
	assert(conn);
	assert(conn->rq);
	assert(conn->handle == fd);
	assert(flags & EV_WRITE);
	assert(conn->rq->evbase != NULL);

	// remove the connect handler
	assert(conn->connect_event);
	event_free(conn->connect_event);
	conn->connect_event = NULL;

	foo = sizeof(error);
	getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &foo);
	if (error == ECONNREFUSED) {
		// connect failed...  we need to move this conn object from the list and put it at the tail.
	
		assert(conn->active == 0);
		assert(conn->closing == 0);
		assert(conn->connect_event == NULL);
		assert(conn->data == NULL);

		rq_conn_closed(conn);
	}
	else {

		assert(conn->active == 0);
		conn->active ++;

		// now that we have connected, we should get a buffer to handle read data.
		assert(conn->readbuf == NULL);
		assert(conn->rq);
		assert(conn->rq->bufpool);
		conn->readbuf = expbuf_pool_new(conn->rq->bufpool, RQ_DEFAULT_BUFFSIZE);
		assert(BUF_MAX(conn->readbuf) >= RQ_DEFAULT_BUFFSIZE);
		assert(conn->readbuf);
	
		// make sure our other buffers are empty, but keep in mind that our outbuf
		// may have something in it by now.
		assert(conn->inbuf == NULL);
		assert(conn->in_msgs == NULL);
		assert(conn->out_msgs == NULL);

		// initialise the data portion of the 'conn' object.
		assert(conn->rq);
		assert(conn->rq->bufpool);
		assert(conn->data == NULL);
		conn->data = (rq_data_t *) malloc(sizeof(rq_data_t));
		assert(conn->data);
		rq_data_init(conn->data, conn->rq->bufpool);

		// apply the regular read handler now.
		assert(conn->read_event == NULL);
		assert(conn->handle > 0);
		conn->read_event = event_new(conn->rq->evbase, conn->handle, EV_READ | EV_PERSIST, rq_read_handler, conn);
		event_add(conn->read_event, NULL);
	
		// if we have data in our out buffer, we need to create the WRITE event.
		if (conn->outbuf) {
			if (BUF_LENGTH(conn->outbuf) > 0) {
				assert(conn->handle != INVALID_HANDLE && conn->handle > 0);
				assert(conn->write_event == NULL);
				assert(conn->rq->evbase);
				conn->write_event = event_new(conn->rq->evbase, conn->handle, EV_WRITE | EV_PERSIST, rq_write_handler, conn);
				event_add(conn->write_event, NULL);
			}
		}
		
		// now that we have an active connection, we need to send our queue requests.
		ll_start(&conn->rq->queues);
		while ((q = ll_next(&conn->rq->queues))) {
			rq_send_consume(conn, q);
		}
		ll_finish(&conn->rq->queues);
	
		// just in case there is some data there already.
		rq_process_read(conn);
	}
}



static void rq_read_handler(int fd, short int flags, void *arg)
{
	rq_conn_t *conn = (rq_conn_t *) arg;

	assert(fd >= 0);
	assert(flags != 0);
	assert(conn);
	assert(conn->rq);
	assert(conn->active > 0);
	assert(flags & EV_READ);
	
	rq_process_read(conn);
}





//-----------------------------------------------------------------------------
// add a controller to the end of the connection list.   If this is the first
// controller, then we need to attempt to connect to it, and setup the socket
// on the event queue.
void rq_addcontroller(
	rq_t *rq,
	char *host,
	void (*connect_handler)(rq_service_t *service, void *arg),
	void (*dropped_handler)(rq_service_t *service, void *arg),
	void *arg)
{
	rq_conn_t *conn;
	
	assert(rq != NULL);
	assert(host != NULL);
	assert(host[0] != '\0');
	assert((arg != NULL && (connect_handler || dropped_handler)) || (arg == NULL));

	// dont currently have it coded to handle these, so we will fail for now
	// until we find a need to use them.
	assert(connect_handler == NULL);
	assert(dropped_handler == NULL);
	assert(arg == NULL);

	fprintf(stderr, "rq: addcontroller(\"%s\")\n", host);

	conn = (rq_conn_t *) malloc(sizeof(rq_conn_t));
	assert(conn);
	
	conn->hostname = strdup(host);

	conn->handle = INVALID_HANDLE;		// socket handle to the connected controller.
	conn->read_event = NULL;
	conn->write_event = NULL;
	conn->connect_event = NULL;

	conn->readbuf = NULL;	
	conn->inbuf = NULL;
	conn->outbuf = NULL;

	conn->in_msgs = NULL;
	conn->out_msgs = NULL;

	assert(rq->risp);
	conn->risp = rq->risp;

	conn->rq = rq;
	conn->active = 0;
	conn->shutdown = 0;
	conn->data = NULL;

	ll_push_tail(&rq->connlist, conn);

	// if this is the only controller we have so far, then we need to attempt the
	// connect (non-blocking)
	if (ll_count(&rq->connlist) == 1) {
		rq_connect(rq);
	}
}










//-----------------------------------------------------------------------------
// Send a request to the controller indicating a desire to consume a particular
// queue.  We will add queue information to our RQ structure.  If we are
// already connected to a controller, then the queue request will be sent
// straight away.  If not, then the request will be made as soon as a
// connection is made.  
void rq_consume(
	rq_t *rq,
	char *queue,
	int max,
	int priority,
	int exclusive,
	void (*handler)(rq_message_t *msg, void *arg),
	void (*accepted)(char *queue, queue_id_t qid, void *arg),
	void (*dropped)(char *queue, queue_id_t qid, void *arg),
	void *arg)
{
	int found;
	rq_queue_t *q;
	rq_conn_t *conn;
	
	assert(rq);
	assert(queue);
	assert(strlen(queue) < 256);
	assert(max >= 0);
	assert(priority == RQ_PRIORITY_NONE || priority == RQ_PRIORITY_LOW || priority == RQ_PRIORITY_NORMAL || priority == RQ_PRIORITY_HIGH);
	assert(handler);

	// We dont cater for the 'accepted' and 'dropped' handlers yet.
	assert(accepted == NULL && dropped == NULL);

	// check that we are connected to a controller.
	assert(ll_count(&rq->connlist) > 0);

	// check that we are not already consuming this queue.
	found = 0;
	ll_start(&rq->queues);
	q = ll_next(&rq->queues);
	while (q && found == 0) {
		if (strcmp(q->queue, queue) == 0) {
			// the queue is already in our list...
			found ++;
		}
		else {
			q = ll_next(&rq->queues);
		}
	}
	ll_finish(&rq->queues);

	if (found == 0) {
		q = (rq_queue_t *) malloc(sizeof(rq_queue_t));
		assert(q != NULL);

		rq_queue_init(q);
		q->queue = strdup(queue);
		q->handler = handler;
		q->arg = arg;
		q->exclusive = exclusive;
		q->max = max;
		q->priority = priority;

		ll_push_tail(&rq->queues, q);

		// check to see if the top connection is active.  If so, send the consume request.
		conn = ll_get_head(&rq->connlist);
		assert(conn);
		if (conn->active > 0 && conn->closing > 0) {
			rq_send_consume(conn, q);
		}
	}
}


// A request has been
static void processRequest(rq_conn_t *conn)
{
	msg_id_t msgid;
	queue_id_t qid = 0;
	char *qname = NULL;
	rq_queue_t *tmp, *queue;
	rq_message_t *msg;
	expbuf_t *buf;
	
	assert(conn);
	assert(conn->data);

	// strategy has changed, need to check that this is still valid.
	assert(0);

	if (BIT_TEST(conn->data->mask, RQ_DATA_MASK_ID) && BIT_TEST(conn->data->mask, RQ_DATA_MASK_PAYLOAD) && (BIT_TEST(conn->data->mask, RQ_DATA_MASK_QUEUEID) || BIT_TEST(conn->data->mask, RQ_DATA_MASK_QUEUE))) {

		// get message ID
		msgid = conn->data->id;
		assert(msgid > 0);

		// get queue Id or queue name.
		if (BIT_TEST(conn->data->mask, RQ_DATA_MASK_QUEUEID))
			qid = conn->data->qid;
		if (BIT_TEST(conn->data->mask, RQ_DATA_MASK_QUEUE))
			qname = expbuf_string(conn->data->queue);
		assert((qname == NULL && qid > 0) || (qname && qid == 0));

		// find the queue to handle this request.
		// TODO: use a function call to do this.
		queue = NULL;
		ll_start(&conn->rq->queues);
		tmp = ll_next(&conn->rq->queues);
		while (tmp) {
			assert(tmp->qid > 0);
			assert(tmp->queue);
			if (qid == tmp->qid || strcmp(qname, tmp->queue) == 0) {
				queue = tmp;
				tmp = NULL;
			}
			else {
				tmp = ll_next(&conn->rq->queues);
			}
		}
		ll_finish(&conn->rq->queues);

		if (queue == NULL) {
			// we dont seem to be consuming that queue...
			assert(conn->rq);
			assert(conn->rq->bufpool);
			buf = expbuf_pool_new(conn->rq->bufpool, 8);
			assert(buf);
			addCmd(buf, RQ_CMD_CLEAR);
			addCmdLargeInt(buf, RQ_CMD_ID, (short int)msgid);
			addCmd(buf, RQ_CMD_UNDELIVERED);
			rq_senddata(conn, BUF_DATA(buf), BUF_LENGTH(buf));
			expbuf_clear(buf);
			expbuf_pool_return(conn->rq->bufpool, buf);
		}
		else {
			// send a delivery message back to the controller.
			assert(conn->rq);
			assert(conn->rq->bufpool);
			buf = expbuf_pool_new(conn->rq->bufpool, 8);
			assert(buf);
			addCmd(buf, RQ_CMD_CLEAR);
			addCmdLargeInt(buf, RQ_CMD_ID, (short int)msgid);
			addCmd(buf, RQ_CMD_DELIVERED);
			rq_senddata(conn, BUF_DATA(buf), BUF_LENGTH(buf));
			expbuf_clear(buf);
			expbuf_pool_return(conn->rq->bufpool, buf);

			// get a new message object from the pool.
			msg = rq_msg_new(conn->rq, conn);
			assert(msg);
			assert(msg->id == 0);
			assert(msg->state == rq_msgstate_new);

			// fill out the message details, and add it to the head of the messages list.
			assert(conn->data);
			assert(queue && msgid > 0);
			msg->queue = queue;
			msg->id = msgid;
			if (BIT_TEST(conn->data->flags, RQ_DATA_FLAG_NOREPLY)) {
				msg->noreply = 1;
			}

			// move the payload buffer to the message.
			assert(msg->data);
			assert(conn->data);
			assert(conn->data->payload);
			msg->data = conn->data->payload;
			conn->data->payload = NULL;

			msg->state = rq_msgstate_delivering;
			queue->handler(msg, queue->arg);

			// if the message was NOREPLY, then we dont need to reply, and we can clear the message.
			if (msg->noreply == 1) {
				rq_msg_clear(msg);
				msg = NULL;
			}
			else if (msg->state == rq_msgstate_replied) {
				// we already have replied to this message.  Dont need to add it to
				// the out-process, as that would already have been done.  So all we
				// need to do is clear the message and return it to the pool.
				rq_msg_clear(msg);
				msg = NULL;
			}
			else {
				// we called the handled, but it hasn't replied yet.  We will need to
				// wait until it calls rq_reply, which can clean up this message
				// object.
				msg->state = rq_msgstate_delivered;
			}			
		}
	}
	else {
		// we dont have the required data to handle a request.
		// TODO: This should be handled better.
		assert(0);
	}
}


//-----------------------------------------------------------------------------
// By receiving the closing command from the controller, we should not get any
// more queue requests to consume.  Also as soon as there are no more messages
// waiting for replies, the controller will drop the connection.  Therefore,
// when this command is processed, we need to initiate a connection to an
// alternative controller that can receive requests.
static void processClosing(rq_conn_t *conn)
{
	assert(conn);

	// mark the connectiong as 'closing'.
	assert(conn->closing == 0);
	conn->closing ++;

	// initialise the connection to the alternate controller.
	assert(conn->rq);
	rq_connect(conn->rq);
}

static void processServerFull(rq_conn_t *conn)
{
	assert(conn);
	assert(0);
}

static void processDelivered(rq_conn_t *conn)
{
	assert(conn);
	assert(0);
}



static void storeQueueID(rq_conn_t *conn, char *queue, int qid)
{
	rq_queue_t *q;

	assert(conn);
	assert(queue);
	assert(qid > 0);
	assert(ll_count(&conn->rq->queues) > 0);

	ll_start(&conn->rq->queues);
	q = ll_next(&conn->rq->queues);
	while (q) {
		if (strcmp(q->queue, queue) == 0) {
			assert(q->qid == 0);
			q->qid = qid;
			q = NULL;
		}
		else {
			q = ll_next(&conn->rq->queues);
		}
	}
	ll_finish(&conn->rq->queues);
}




static void cmdClear(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);
	assert(conn->data);
	
	conn->data->mask = 0;
	conn->data->flags = 0;
	
	conn->data->id = 0;
	conn->data->qid = 0;
	conn->data->timeout = 0;
	conn->data->priority = 0;

	expbuf_clear(conn->data->queue);

	if (conn->data->payload) {
		expbuf_clear(conn->data->payload);
	}
}


static void cmdPing(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	char buf;
	
	assert(conn);

	// since we only need to send one char, its a bit silly to get an expanding buffer for it... so we'll do this one slightly different.
	buf = RQ_CMD_PONG;

	rq_senddata(conn, &buf, 1);
}


static void cmdPong(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);

	// not sure what to do with this yet.  Havent built in the code to actually handle things if we dont get the pong back.
	assert(0);
}





static void cmdConsuming(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);
	assert(conn->data);

	if (BIT_TEST(conn->data->mask, RQ_DATA_MASK_QUEUEID)  && BIT_TEST(conn->data->mask, RQ_DATA_MASK_QUEUE)) {
		storeQueueID(conn, expbuf_string(conn->data->queue), conn->data->qid);
	}
	else {
		fprintf(stderr, "Not enough data.\n");
		assert(0);
	}
}

static void cmdRequest(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);
	assert(conn->data);
	processRequest(conn);
}

static void cmdDelivered(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);
	assert(conn->data);
	processDelivered(conn);
}

static void cmdBroadcast(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);
	assert(conn->data);

	assert(0);
}
	
// set the noreply flag.
static void cmdNoreply(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;

	assert(conn);
	assert(conn->data);
	BIT_SET(conn->data->flags, RQ_DATA_FLAG_NOREPLY);	
}

static void cmdClosing(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;

	assert(conn);
	assert(conn->data);

	processClosing(conn);	
}
	
static void cmdServerFull(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;

	assert(conn);
	assert(conn->data);

	processServerFull(conn);
}
	
static void cmdID(void *ptr, risp_int_t value)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
	assert(conn);
	assert(value > 0 && value <= 0xffff);
	assert(conn->data);
	
	conn->data->id = value;
	BIT_SET(conn->data->mask, RQ_DATA_MASK_ID);
}
	
static void cmdQueueID(void *ptr, risp_int_t value)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
	assert(conn);
	assert(value > 0 && value <= 0xffff);
	assert(conn->data);
	
	conn->data->qid = value;
	BIT_SET(conn->data->mask, RQ_DATA_MASK_QUEUEID);

}
	
static void cmdTimeout(void *ptr, risp_int_t value)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
	assert(conn);
	assert(value > 0 && value <= 0xffff);
	assert(conn->data);
	
	conn->data->timeout = value;
	BIT_SET(conn->data->mask, RQ_DATA_MASK_TIMEOUT);
}
	
static void cmdPriority(void *ptr, risp_int_t value)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;

	assert(conn);
	assert(value > 0 && value <= 0xffff);
	assert(conn->data);
	
	conn->data->priority = value;
	BIT_SET(conn->data->mask, RQ_DATA_MASK_PRIORITY);
}
	
static void cmdPayload(void *ptr, risp_length_t length, risp_char_t *data)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
 	assert(conn);
 	assert(length > 0);
 	assert(data);

	assert(conn->data);
	if (conn->data->payload == NULL) {
		assert(conn->rq);
		assert(conn->rq->bufpool);
		conn->data->payload = expbuf_pool_new(conn->rq->bufpool, 0);
		assert(conn->data->payload);
	}
 	expbuf_set(conn->data->payload, data, length);
	BIT_SET(conn->data->mask, RQ_DATA_MASK_PAYLOAD);
}


static void cmdQueue(void *ptr, risp_length_t length, risp_char_t *data)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
 	assert(conn);
 	assert(length > 0);
 	assert(data);

 	assert(conn->data);
 	assert(conn->data->queue);
 	expbuf_set(conn->data->queue, data, length);
 	BIT_SET(conn->data->mask, RQ_DATA_MASK_QUEUE);
}

static void cmdInvalid(void *ptr, void *data, risp_length_t len)
{
	// this callback is called if we have an invalid command.  We shouldn't be receiving any invalid commands.
	unsigned char *cast;

	assert(ptr != NULL);
	assert(data != NULL);
	assert(len > 0);
	
	cast = (unsigned char *) data;
	fprintf(stderr, "Received invalid (%d)): [%d, %d, %d]\n", len, cast[0], cast[1], cast[2]);
	assert(0);
}



// Initialise an RQ structure.  
void rq_init(rq_t *rq)
{
	assert(rq);

	rq->evbase = NULL;

	// setup the risp processor.
	rq->risp = risp_init();
	risp_add_invalid(rq->risp, &cmdInvalid);
	risp_add_command(rq->risp, RQ_CMD_CLEAR,        &cmdClear);
	risp_add_command(rq->risp, RQ_CMD_PING,         &cmdPing);
	risp_add_command(rq->risp, RQ_CMD_PONG,         &cmdPong);
	risp_add_command(rq->risp, RQ_CMD_REQUEST,      &cmdRequest);
	risp_add_command(rq->risp, RQ_CMD_DELIVERED,    &cmdDelivered);
  risp_add_command(rq->risp, RQ_CMD_BROADCAST,    &cmdBroadcast);
	risp_add_command(rq->risp, RQ_CMD_NOREPLY,      &cmdNoreply);
	risp_add_command(rq->risp, RQ_CMD_CLOSING,      &cmdClosing);
	risp_add_command(rq->risp, RQ_CMD_CONSUMING,    &cmdConsuming);
	risp_add_command(rq->risp, RQ_CMD_SERVER_FULL,  &cmdServerFull);
	risp_add_command(rq->risp, RQ_CMD_ID,           &cmdID);
	risp_add_command(rq->risp, RQ_CMD_QUEUEID,      &cmdQueueID);
	risp_add_command(rq->risp, RQ_CMD_TIMEOUT,      &cmdTimeout);
	risp_add_command(rq->risp, RQ_CMD_PRIORITY,     &cmdPriority);
	risp_add_command(rq->risp, RQ_CMD_QUEUE,        &cmdQueue);
	risp_add_command(rq->risp, RQ_CMD_PAYLOAD,      &cmdPayload);

	ll_init(&rq->connlist);
	ll_init(&rq->queues);

	rq->msgpool = (mempool_t *) malloc(sizeof(mempool_t));
	mempool_init(rq->msgpool);

	rq->bufpool = (expbuf_pool_t *) malloc(sizeof(expbuf_pool_t));
	expbuf_pool_init(rq->bufpool, 0);		// TODO: should we have a max to avoid having large buffers that are not necessary?
}




//-----------------------------------------------------------------------------
// Return a new message struct.  Will get one from teh mempool if there are any
// available, otherwise it will create a new entry.  Cannot assume that
// anything left in the mempool has any valid data, so will initialise it as if
// it was a fresh allocation.
rq_message_t * rq_msg_new(rq_t *rq, rq_conn_t *conn)
{
	rq_message_t *msg;

	assert(rq);
	
	// need to get a message struct from the mempool.
	assert(rq);
	assert(rq->msgpool);
	msg = mempool_get(rq->msgpool, sizeof(rq_message_t));
	if (msg == NULL) {
		// there wasnt any messages available in the pool, so we need to create one, and add it.
		msg = (rq_message_t *) malloc(sizeof(rq_message_t));
		mempool_assign(rq->msgpool, msg, sizeof(rq_message_t));
	}

	assert(msg);
	msg->queue = NULL;
	msg->id = 0;
	msg->noreply = 0;
	msg->state = rq_msgstate_new;
	msg->conn = conn;
	msg->data = NULL;

	assert(msg);
	return(msg);
}

//-----------------------------------------------------------------------------
// clean up the resources used by the message so that it can be used again.  We
// will return the data buffer to the bufpool so that it can be used for
// payloads of future messages.  We will also return the message to the message
// pool.
void rq_msg_clear(rq_message_t *msg)
{
	assert(msg != NULL);

	msg->id = 0;
	msg->broadcast = 0;
	msg->noreply = 0;
	msg->queue = NULL;
	msg->state = rq_msgstate_new;

	// clear the buffer
	assert(msg->data);
	expbuf_clear(msg->data);

	// put the buffer back in the bufpool.
	assert(msg->conn);
	assert(msg->conn->rq);
	assert(msg->conn->rq->bufpool);
	expbuf_pool_return(msg->conn->rq->bufpool, msg->data);
	msg->data = NULL;

	// return the message to the msgpool.
	assert(msg->conn);
	assert(msg->conn->rq);
	assert(msg->conn->rq->msgpool);
	mempool_return(msg->conn->rq->msgpool, msg);
}


void rq_msg_setqueue(rq_message_t *msg, char *queue)
{
	assert(msg != NULL);
	assert(queue != NULL);
	assert(msg->queue == NULL);

	msg->queue = queue;
}


void rq_msg_setbroadcast(rq_message_t *msg)
{
	assert(msg != NULL);
	assert(msg->broadcast == 0);

	msg->broadcast = 1;
}

void rq_msg_setnoreply(rq_message_t *msg)
{
	assert(msg != NULL);
	assert(msg->noreply == 0);
	msg->noreply = 1;
}

// This function expects a pointer to memory that it now controls.  When it is finished 
void rq_msg_setdata(rq_message_t *msg, int length, char *data)
{
	assert(msg != NULL);
	assert(length > 0);
	assert(data != NULL);

	expbuf_set(msg->data, data, length);
}

// send a message to the controller.
void rq_send(rq_t *rq, rq_message_t *msg, void (*handler)(rq_message_t *reply, void *arg), void *arg)
{
	assert(rq);
	assert(msg);

	assert((handler == NULL && arg == NULL) || (handler != NULL));
	
	assert(BUF_LENGTH(msg->data) > 0);

	// if there is no data in the 'out' queue, then attempt to send the request to the socket.  Any that we are unable to send, should be added to teh out queue, and the WRITE event should be established.

	assert(0);
}


//-----------------------------------------------------------------------------
// This function is used to send a reply for a request.  The data being sent
// back should be placed in the data buffer.   Reply needs to be sent on the
// same connection that it arrived on.
void rq_reply(rq_message_t *msg)
{
	expbuf_t *buf;

	assert(msg);
	assert(msg->rq);
	assert(msg->conn);

	assert(msg->id > 0);
	assert(msg->broadcast == 0);
	assert(msg->noreply == 0);
	assert(msg->queue);
	assert(msg->state == rq_msgstate_delivering || msg->state == rq_msgstate_delivered);

	assert(msg->data);

	// get the send buffer from rq.
	assert(msg->conn);
	assert(msg->rq->bufpool);
	buf = expbuf_pool_new(msg->rq->bufpool, 0);
	addCmd(buf, RQ_CMD_CLEAR);
	addCmdLargeInt(buf, RQ_CMD_ID, (short int) msg->id);
	addCmdLargeStr(buf, RQ_CMD_PAYLOAD, BUF_LENGTH(msg->data), BUF_DATA(msg->data));
	addCmd(buf, RQ_CMD_REPLY);
	rq_senddata(msg->conn, BUF_DATA(buf), BUF_LENGTH(buf));
	expbuf_clear(buf);
	expbuf_pool_return(msg->rq->bufpool, buf);

	// if this reply is being sent after the message was delivered to the handler,
	if (msg->state == rq_msgstate_delivered) {
		// then we need to clean up the message, because there is nothing else that will.
		rq_msg_clear(msg);
	}
	else {
		// then we must be replying straight away and we can let the code that called the handler clean it up.
		msg->state = rq_msgstate_replied;
	}
}


///----------------------------------------------------------------------------
/// Service management.
///----------------------------------------------------------------------------


//-----------------------------------------------------------------------------
// Initialise a new rq_service_t object and return a pointer to it.  It should be cleaned up with rq_service_cleanup()
rq_service_t *rq_svc_new(void)
{
	rq_service_t *service;
	int i;

	service = (rq_service_t *) malloc(sizeof(rq_service_t));
	
	service->verbose = 0;

	service->rq = (rq_t *) malloc(sizeof(rq_t));
	rq_init(service->rq);
	assert(service->rq->evbase == NULL);

	for (i=0; i<RQ_MAX_HELPOPTIONS; i++) {
		service->help_options[i] = NULL;
	}

	service->svcname = NULL;
	rq_svc_setoption(service, 'c', "ip:port",  "Controller to connect to.");
	rq_svc_setoption(service, 'd', NULL,       "Run as a daemon");
	rq_svc_setoption(service, 'P', "file",     "save PID in <file>, only used with -d option");
	rq_svc_setoption(service, 'u', "username", "assume identity of <username> (only when run as root)");
	rq_svc_setoption(service, 'v', NULL,       "verbose (print errors/warnings to stdout)");
	rq_svc_setoption(service, 'h', NULL,       "print this help and exit");

	return(service);
}

//-----------------------------------------------------------------------------
// Cleanup the service object and free its resources and itself.
void rq_svc_cleanup(rq_service_t *service)
{
	rq_svc_helpoption_t *help;
	int i;
	
	assert(service);
	
	assert(service->rq);
	rq_cleanup(service->rq);
	free(service->rq);
	service->rq = NULL;

	// if we are in daemon mode, and a pidfile is provided, then we need to delete the pidfile
	assert(service->help_options['d']);
	assert(service->help_options['P']);
	if (service->help_options['d']->count && service->help_options['P']->value) {
		assert(service->help_options['P']->value[0] != 0);
		unlink(service->help_options['P']->value);
	}

	// free all the memory used by the help-options, command-line values.
	for (i=0; i<RQ_MAX_HELPOPTIONS; i++) {
		if (service->help_options[i]) {
			help = service->help_options[i];
			if (help->value)   free(help->value);
			free(help);
		}
	}

	assert(service);
	service->svcname = NULL;

	free(service);
}


void rq_svc_setname(rq_service_t *service, const char *name)
{
	assert(service);
	assert(name);

	assert(service->svcname == NULL);
	service->svcname = (char *) name;
}


// add a help option to the list.
void rq_svc_setoption(rq_service_t *service, char tag, const char *param, const char *details)
{
	rq_svc_helpoption_t *help;

	assert(service);
	assert(details);
	assert(tag > 0 && tag < RQ_MAX_HELPOPTIONS);
	assert(service->help_options[(int)tag] == NULL);
		
	help = (rq_svc_helpoption_t *) malloc(sizeof(rq_svc_helpoption_t));
	help->param = (char *) param;
	help->details = (char *) details;
	help->value = NULL;
	help->count = 0;

	service->help_options[(int)tag] = help;
}


static void rq_svc_usage(rq_service_t *service)
{
	int i;
	int largest;
	int len;
	rq_svc_helpoption_t *entry;
	
	assert(service);

	// go through the options list and determine the largest param field.
	largest = 0;
	for (i=0; i<RQ_MAX_HELPOPTIONS; i++) {
		entry = service->help_options[i];
		if (entry) {
			if (entry->param) {
				len = strlen(entry->param);
				if (len > largest) { largest = len; }
			}
		}
	}

	// if we have any options with parameters, then we need to account for the <> that we will be putting around them.
	if (largest > 0) {
		largest += 2;
	}

	// now go through the list and display the info.
	printf("Usage:\n");
	for (i=0; i<RQ_MAX_HELPOPTIONS; i++) {
		entry = service->help_options[i];
		if (entry) {
			assert(entry->details);
			if (largest == 0) {
				printf(" -%c %s\n", i, entry->details);
			}
			else {

			/// TODO: Need to use the 'largest' to space out the detail from the param.
			
				if (entry->param) {
					printf(" -%c <%s> %s\n", i, entry->param, entry->details);
				}
				else {
					printf(" -%c %s %s\n", i, "", entry->details);
				}
			}
		}
	}
}


#define MAX_OPTSTR ((RQ_MAX_HELPOPTIONS) * 4)
void rq_svc_process_args(rq_service_t *service, int argc, char **argv)
{
	int c;
	rq_svc_helpoption_t *entry;
	int i;
	char optstr[MAX_OPTSTR + 1];
	int len;

	assert(service);
	assert(argc > 0);
	assert(argv);

	// build the getopt string.
	len = 0;
	for (i=0; i<RQ_MAX_HELPOPTIONS; i++) {
		entry = service->help_options[i];
		if (entry) {
			assert(entry->value == NULL);
			optstr[len++] = i;
			if (entry->param != NULL) {
				optstr[len++] = ':';
			}
		}
		assert(len < MAX_OPTSTR);
	}
	optstr[len++] = '\0';
	
	while (-1 != (c = getopt(argc, argv, optstr))) {
		if ((entry = service->help_options[c])) {
			if (entry->param) {
				assert(entry->count == 0);
				assert(entry->value == NULL);
				entry->value = strdup(optarg);
			}
			else {
				entry->count ++;
			}
		}
		else {
			fprintf(stderr, "Illegal argument \"%c\"\n", c);
			exit(EXIT_FAILURE);
		}
	}

	// check for -h, display help
	assert(service->help_options['h']);
	if (service->help_options['h']->count > 0) {
		rq_svc_usage(service);
		exit(EXIT_SUCCESS);
	}

	// check for -v, for verbosity.
	assert(service->help_options['v']);
	assert(service->help_options['v']->value == NULL);
	assert(service->help_options['v']->param == NULL);
	service->verbose = service->help_options['v']->count;
	assert(service->verbose >= 0);
}
#undef MAX_OPTSTR


// Function is used to initialise a shutdown of the rq service.
void rq_svc_shutdown(rq_service_t *service)
{
	assert(service);

	assert(service->rq);
	rq_shutdown(service->rq);
}

//-----------------------------------------------------------------------------
// This function is used to initialise the service using the parameters that
// were supplied and already processed.   Therefore rq_svc_process_args()
// should be run before this function.  If the options state to be in daemon
// mode, it will fork and set the username it is running;
void rq_svc_initdaemon(rq_service_t *service)
{
	char *username;
	char *pidfile;
	int noclose;
	
	assert(service);
	assert(service->help_options['d']);

	if (service->help_options['d']->count > 0) {

		assert(service->help_options['u']);
		assert(service->help_options['P']);
		username = service->help_options['u']->value;
		pidfile = service->help_options['P']->value;
		noclose = service->verbose;
	
		// if we are supplied with a username, drop privs to it.  This will only
		// work if we are running as root, and is really only needed when running as 
		// a daemon.
		rq_daemon((const char *)username, (const char *)pidfile, noclose);
	}
}


void rq_svc_setevbase(rq_service_t *service, struct event_base *evbase)
{
	assert(service);
	assert(service->rq);

	rq_setevbase(service->rq, evbase);
}

//-----------------------------------------------------------------------------
// return the value of the stored option value.
char * rq_svc_getoption(rq_service_t *service, char tag)
{
	assert(service);
	assert(service->help_options[(int)tag]);
	return(service->help_options[(int)tag]->value);
}

//-----------------------------------------------------------------------------
// connect to the controllers that were specified in the controller parameter.
int rq_svc_connect(
	rq_service_t *service,
	void (*connect_handler)(rq_service_t *service, void *arg),
	void (*dropped_handler)(rq_service_t *service, void *arg),
	void *arg)
{
	char *str;
	char *copy;
	char *argument;
	char *next;
	
	assert(service);
	assert((arg != NULL && (connect_handler || dropped_handler)) || (arg == NULL));
	assert(service->rq);

	str = rq_svc_getoption(service, 'c');
	if (str == NULL) {
		return -1;
	}	

	// make a copy of the supplied string, because we will be splitting it into
	// its key/value pairs. We dont want to mangle the string that was supplied.
	assert(str);
	copy = strdup(str);
	assert(copy);

	next = copy;
	while (next != NULL && *next != '\0') {
		argument = strsep(&next, ",");
		if (argument) {
		
			// remove spaces from the begining of the key.
			while(*argument==' ' && *argument!='\0') { argument++; }
			
			if (strlen(argument) > 0) {
				rq_addcontroller(service->rq, argument, connect_handler, dropped_handler, arg);
			}
		}
	}
	
	free(copy);
	return 0;
}
