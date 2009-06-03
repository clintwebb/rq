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



static void rq_process_handler(int fd, short int flags, void *arg);



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




// based on some code that was used in memcached.
int rq_daemon(char *username, char *pidfile, int noclose)
{
	struct passwd *pw;
	int res;
	struct sigaction sa;
	int fd;
	FILE *fp;
	


	// if we are supplied with a username, drop privs to it.  This will only 
	// work if we are running as root, and is really only needed when running as 
	// a daemon.
	if (username != NULL) {

		assert(username[0] != '\0');
  
		if (getuid() == 0 || geteuid() == 0) {
			if (username == 0 || *username == '\0') {
				fprintf(stderr, "can't run as root without the -u switch\n");
				return 1;
			}
			pw = getpwnam((const char *)username);
			if (pw == NULL) {
				fprintf(stderr, "can't find the user %s to switch to\n", username);
				return 1;
			}
			if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
				fprintf(stderr, "failed to assume identity of user %s\n", username);
				return 1;
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
			return (-1);
		case 0:
			break;
		default:
			_exit(EXIT_SUCCESS);
	}

	if (setsid() == -1)
			return (-1);

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
			return -1;
		}
	
		fprintf(fp,"%ld\n", (long)getpid());
		if (fclose(fp) == -1) {
			fprintf(stderr, "Could not close the pid file %s.\n", pidfile);
			return -1;
		}
	}
    
	return (0);
}


void rq_data_init(rq_data_t *data)
{
	assert(data != NULL);
	
	data->flags = 0;
	data->mask = 0;
	
	data->id = 0;
	data->qid = 0;
	data->timeout = 0;
	data->priority = 0;
	expbuf_init(&data->payload, 0);
	expbuf_init(&data->queue, 0);
}

void rq_data_free(rq_data_t *data)
{
	assert(data != NULL);
	expbuf_free(&data->payload);
	expbuf_free(&data->queue);
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


// This function is called whenever we lose a connection to the controller.
void rq_conn_closed(rq_conn_t *conn)
{
	assert(conn);

	assert(conn->rq);
	assert(conn->rq->bufpool);

	// free all the buffers.
	assert(conn->readbuf);
	assert(BUF_LENGTH(conn->readbuf) == 0);
	expbuf_pool_return(conn->rq->bufpool, conn->readbuf);
	conn->readbuf = NULL;

	if (conn->in) {
		expbuf_clear(conn->in);
		expbuf_pool_return(conn->rq->bufpool, conn->in);
		conn->in = NULL;
	}
	
	if (conn->out) {
		expbuf_clear(conn->out);
		expbuf_pool_return(conn->rq->bufpool, conn->out);
		conn->out = NULL;
	}

	// cleanup the data structure.
	rq_data_free(&conn->data);

	// remove the conn from the connlist.
	assert(0);

	// clear the event.
// 	struct event event;
	assert(0);

	// timeout all the pending messages, if there are any.
//	list_t messages;
	assert(0);
	
	// clear the 'messages' linked list.
	assert(0);

	assert(conn->connect_event == NULL);

	// free the conn object.
	assert(0);
	
}


//-----------------------------------------------------------------------------
// Used to tell the library that it needs to start shutting down connections so that the application can exit the loop.
void rq_shutdown(rq_t *rq)
{
	assert(rq);

	assert(0);
}


void rq_cleanup(rq_t *rq)
{
	rq_queue_t *q;
	assert(rq != NULL);

	// cleanup the risp object.
	assert(rq->risp != NULL);
	risp_shutdown(rq->risp);
	free(rq->risp);
	rq->risp = NULL;

	// free the resources allocated for the connlist.
	assert(ll_count(&rq->connlist) == 0);
	ll_free(&rq->connlist);

	// cleanup the risp object.
	assert(rq->risp != NULL);
	risp_shutdown(rq->risp);
	rq->risp = NULL;

	// cleanup all the queues that we have.
	while (q = ll_pop_head(&rq->queues)) {
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
	assert(rq != NULL);
	assert(base != NULL);

	assert(rq->evbase == NULL);
	rq->evbase = base;
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
			}
			else { empty = 1; }
			
			// if there is no data in the in-buffer, then we will process the common buffer by itself.
			if (conn->in == NULL) {
				res = risp_process(conn->risp, conn, BUF_LENGTH(conn->readbuf), (unsigned char *) BUF_DATA(conn->readbuf));
				assert(res <= BUF_LENGTH(conn->readbuf));
				assert(res >= 0);
				if (res > 0) { expbuf_purge(conn->readbuf, res); }

				// if there is data left over, then we need to add it to our in-buffer.
				if (BUF_LENGTH(conn->readbuf) > 0) {
					if (conn->in == NULL) {
						conn->in = expbuf_pool_new(conn->rq->bufpool, BUF_LENGTH(conn->readbuf));
						assert(conn->in);
					}
					expbuf_add(conn->in, BUF_DATA(conn->readbuf), BUF_LENGTH(conn->readbuf));
					expbuf_clear(conn->readbuf);
				}
			}
			else {
				// we have data left in the in-buffer, so we add the content of the common buffer
				assert(BUF_LENGTH(conn->readbuf) > 0);
				assert(conn->in);
				assert(BUF_LENGTH(conn->in) > 0);
				expbuf_add(conn->in, BUF_DATA(conn->readbuf), BUF_LENGTH(conn->readbuf));
				expbuf_clear(conn->readbuf);
				assert(BUF_LENGTH(conn->readbuf) == 0);
				assert(BUF_LENGTH(conn->in) > 0 && BUF_DATA(conn->in) != NULL);

				// and then process what is there.
				res = risp_process(conn->risp, conn, BUF_LENGTH(conn->in), (unsigned char *) BUF_DATA(conn->in));
				assert(res <= BUF_LENGTH(conn->in));
				assert(res >= 0);
				if (res > 0) { expbuf_purge(conn->in, res); }

				if (BUF_LENGTH(conn->in) == 0) {
					// the in buffer is now empty, so we should return it to the pool.
					expbuf_pool_return(conn->rq->bufpool, conn->in);
					conn->in = NULL;
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
					close(conn->handle);
					rq_conn_closed(conn);
				}
			}
		}
	}
	
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
	assert(conn->status == active);
	assert(flags & EV_WRITE);
	assert(conn->write_event);
	
	// incomplete
	assert(0);

	// if we dont have any more to send, then we need to remove the WRITE event.
	if (BUF_LENGTH(conn->out) == 0) {
		event_free(conn->write_event);
		conn->write_event = NULL;
	}
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
	if (conn->status == active && conn->out == NULL) {
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
	if (res < length && conn->handle != INVALID_HANDLE) {
		// there is already data in the out queue...
		if (res < 0) { res = 0; }

		if (conn->out == NULL) {
			assert(conn->rq);
			assert(conn->rq->bufpool);
			conn->out = expbuf_pool_new(conn->rq->bufpool, (length-res));
			assert(conn->out);
		}
		
		expbuf_add(conn->out, data+res, length-res);
		assert(conn->rq->evbase);
		assert(conn->read_event);
		assert(conn->write_event == NULL);
		conn->write_event = event_new(conn->rq->evbase, conn->handle, EV_WRITE | EV_PERSIST, rq_write_handler, conn);
		event_add(conn->write_event, NULL);
	}

	return;

err:
	close(conn->handle);
	rq_conn_closed(conn);
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
	addCmd(buf, RQ_CMD_CONSUME);
	if (queue->exclusive != 0)
		addCmd(buf, RQ_CMD_EXCLUSIVE);
	addCmdShortStr(buf, RQ_CMD_QUEUE, strlen(queue->queue), queue->queue);
	addCmdInt(buf, RQ_CMD_MAX, queue->max);
	addCmdShortInt(buf, RQ_CMD_PRIORITY, queue->priority);
	addCmd(buf, RQ_CMD_EXECUTE);

	rq_senddata(conn, BUF_DATA(buf), BUF_LENGTH(buf));
	expbuf_clear(buf);
	
	// return the buffer to the bufpool.
	expbuf_pool_return(conn->rq->bufpool, buf);
}


static void rq_read_handler(int fd, short int flags, void *arg)
{
	rq_conn_t *conn = (rq_conn_t *) arg;
	rq_queue_t *q;
	void *next;

	assert(fd >= 0);
	assert(flags != 0);
	assert(conn);
	assert(conn->rq);
	assert(conn->status == active);
	assert(flags & EV_READ);
	
	rq_process_read(conn);
}



static void rq_connect_handler(int fd, short int flags, void *arg)
{
	rq_conn_t *conn = (rq_conn_t *) arg;
	rq_queue_t *q;
	void *next;

	assert(fd >= 0);
	assert(flags != 0);
	assert(conn);
	assert(conn->rq);
	assert(conn->handle == fd);

	assert(flags & EV_WRITE);

	assert(conn->status == connecting);
	conn->status = active;

	// remove the connect handler, and apply the regular read handler now.
	assert(conn->rq->evbase != NULL);
	assert(conn->read_event);
	
	event_del(conn->read_event);
	conn->read_event = event_new(conn->rq->evbase, conn->handle, EV_READ | EV_PERSIST, rq_read_handler, conn);
	event_add(conn->read_event, NULL);

	// if we have data in our out buffer, we need to create the WRITE event.
	if (BUF_LENGTH(conn->out) > 0) {
		assert(conn->handle != INVALID_HANDLE && conn->handle > 0);
		assert(conn->write_event == NULL);
		assert(conn->rq->evbase);
		conn->write_event = event_new(conn->rq->evbase, conn->handle, EV_WRITE | EV_PERSIST, rq_write_handler, conn);
		event_add(conn->write_event, NULL);
	}

	// now that we have an active connection, we need to send our queue requests.
	next = ll_start(&conn->rq->queues);
	while (q = ll_next(&conn->rq->queues, &next)) {
		rq_send_consume(conn, q);
	}

	// just in case there is some data there already.
	rq_process_read(conn);
}






//-----------------------------------------------------------------------------
// Assuming that the rq structure has been properlly filled out, this function
// will initiate the connection process to a specified IP address.   Since the
// application may be using the event loop for other things, and since we need
// to support primary and secondary controllers, we need to connect in
// non-blocking mode.
static void rq_connect(rq_t *rq)
{
	evutil_socket_t sock;
	rq_conn_t *conn, *first;
	int i, j;
	struct sockaddr saddr;
// 	struct sockaddr_in sin;
	unsigned long ulAddress;
	struct hostent *hp;
	int result;
	int len;

	assert(rq != NULL);

	assert(rq->evbase != NULL);
	assert(ll_count(&rq->connlist) > 0);

	conn = ll_get_head(&rq->connlist);
	first = conn;
	result = -1;
	while (conn && result < 0) {

		assert(conn->hostname != NULL);
		assert(conn->hostname[0] != '\0');
		assert(conn->read_event == NULL);
		assert(conn->write_event == NULL);

		len = sizeof(saddr);
		if (evutil_parse_sockaddr_port(conn->hostname, &saddr, &len) != 0) {
			// unable to parse the detail.  What do we need to do?
			assert(0);
		}
		else {
			sock = socket(AF_INET,SOCK_STREAM,0);
			assert(sock >= 0);
							
			// Before we attempt to connect, set the socket to non-blocking mode.
			evutil_make_socket_nonblocking(sock);
	
			conn->handle = sock;
			conn->status = connecting;
	
			result = connect(sock, &saddr, sizeof(saddr));
			assert(result < 0);
			assert(errno == EINPROGRESS);
	
			// connect process has been started.  Now we need to create an event so that we know when the connect has completed.
			assert(conn->connect_event == NULL);
			assert(conn->read_event == NULL);
			assert(conn->write_event == NULL);
			assert(conn->rq);
			assert(conn->rq->evbase);
			conn->connect_event = event_new(conn->rq->evbase, sock, EV_WRITE, rq_connect_handler, conn);
			event_add(conn->connect_event, NULL);
		}
	}
}



//-----------------------------------------------------------------------------
// add a controller to the end of the connection list.   If this is the first
// controller, then we need to attempt to connect to it, and setup the socket
// on the event queue.
void rq_addcontroller(rq_t *rq, char *host)
{
	rq_conn_t *conn;
	int j;
	
	assert(rq != NULL);
	assert(host != NULL);
	assert(host[0] != '\0');
	
	conn = (rq_conn_t *) malloc(sizeof(rq_conn_t));
	assert(conn);
	
	conn->hostname = strdup(host);

	conn->handle = INVALID_HANDLE;		// socket handle to the connected controller.
	conn->read_event = NULL;
	conn->write_event = NULL;
	conn->connect_event = NULL;

	assert(rq->bufpool);
	conn->readbuf = expbuf_pool_new(rq->bufpool, RQ_DEFAULT_BUFFSIZE);
	
	conn->in = NULL;
	conn->out = NULL;
	
	rq_data_init(&conn->data);

	assert(rq->risp);
	conn->risp = rq->risp;

	conn->rq = rq;
	conn->status = inactive;

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
void rq_consume(rq_t *rq, char *queue, int max, int priority, int exclusive, void (*handler)(rq_message_t *msg, void *arg), void *arg)
{
	int found;
	rq_queue_t *q;
	rq_conn_t *conn;
	void *next;
	
	assert(rq != NULL);
	assert(queue != NULL);
	assert(strlen(queue) < 256);
	assert(max >= 0);
	assert(priority == RQ_PRIORITY_NONE || priority == RQ_PRIORITY_LOW || priority == RQ_PRIORITY_NORMAL || priority == RQ_PRIORITY_HIGH);
	assert(handler != NULL);

	// check that we are connected to a controller.
	assert(ll_count(&rq->connlist) > 0);

	// check that we are not already consuming this queue.
	found = 0;
	next = ll_start(&rq->queues);
	q = ll_next(&rq->queues, &next);
	while (q && found == 0) {
		if (strcmp(q->queue, queue) == 0) {
			// the queue is already in our list...
			found ++;
		}
		else {
			q = ll_next(&rq->queues, &next);
		}
	}

	if (found == 0) {
		q = (rq_queue_t *) malloc(sizeof(rq_queue_t));
		assert(q != NULL);

		rq_queue_init(q);
		q->queue = queue;
		q->handler = handler;
		q->arg = arg;
		q->exclusive = exclusive;
		q->max = max;
		q->priority = priority;

		ll_push_tail(&rq->queues, q);

		// check to see if the top connection is active.  If so, send the consume request.
		conn = ll_get_head(&rq->connlist);
		assert(conn);
		if (conn->status == active) {
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
	void *next;
	rq_message_t *msg;
	expbuf_t *buf;
	
	assert(conn);

	// strategy has changed, need to check that this is still valid.
	assert(0);

	// get the required data out of the data structure, and make sure that we have all we need.
	assert(BIT_TEST(conn->data.flags, RQ_DATA_FLAG_REQUEST));
	assert(BIT_TEST(conn->data.flags, RQ_DATA_FLAG_BROADCAST) == 0);

	if (BIT_TEST(conn->data.mask, RQ_DATA_MASK_ID) && BIT_TEST(conn->data.mask, RQ_DATA_MASK_PAYLOAD) && (BIT_TEST(conn->data.mask, RQ_DATA_MASK_QUEUEID) || BIT_TEST(conn->data.mask, RQ_DATA_MASK_QUEUE))) {

		// get message ID
		msgid = conn->data.id;
		assert(msgid > 0);

		// get queue Id or queue name.
		if (BIT_TEST(conn->data.mask, RQ_DATA_MASK_QUEUEID))
			qid = conn->data.qid;
		if (BIT_TEST(conn->data.mask, RQ_DATA_MASK_QUEUE))
			qname = expbuf_string(&conn->data.queue);
		assert((qname == NULL && qid > 0) || (qname && qid == 0));

		// find the queue to handle this request.
		// TODO: use a function call to do this.
		queue = NULL;
		next = ll_start(&conn->rq->queues);
		tmp = ll_next(&conn->rq->queues, &next);
		while (tmp) {
			assert(tmp->qid > 0);
			assert(tmp->queue);
			if (qid == tmp->qid || strcmp(qname, tmp->queue) == 0) {
				queue = tmp;
				tmp = NULL;
			}
			else {
				tmp = ll_next(&conn->rq->queues, &next);
			}
		}

		if (queue == NULL) {
			// we dont seem to be consuming that queue...
			assert(conn->rq);
			assert(conn->rq->bufpool);
			buf = expbuf_pool_new(conn->rq->bufpool, 8);
			assert(buf);
			addCmd(buf, RQ_CMD_CLEAR);
			addCmd(buf, RQ_CMD_UNDELIVERED);
			addCmdLargeInt(buf, RQ_CMD_ID, (short int)msgid);
			addCmd(buf, RQ_CMD_EXECUTE);
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
			addCmd(buf, RQ_CMD_DELIVERED);
			addCmdLargeInt(buf, RQ_CMD_ID, (short int)msgid);
			addCmd(buf, RQ_CMD_EXECUTE);
			rq_senddata(conn, BUF_DATA(buf), BUF_LENGTH(buf));
			expbuf_clear(buf);
			expbuf_pool_return(conn->rq->bufpool, buf);

			assert(conn->rq->msgpool);
			msg = mempool_get(conn->rq->msgpool, sizeof(rq_message_t));
			if (msg == NULL) {
				// there is no message object in the list.  We need to create one.
				msg = (rq_message_t *) malloc(sizeof(rq_message_t));
				rq_message_init(msg);
				mempool_assign(conn->rq->msgpool, msg, sizeof(rq_message_t));
			}
			assert(msg);
			assert(msg->id == 0);

			// fill out the message details, and add it to the head of the messages list.
			assert(queue && msgid > 0);
			msg->queue = queue;
			msg->id = msgid;
			if (BIT_TEST(conn->data.flags, RQ_DATA_FLAG_NOREPLY))
				msg->noreply = 1;

			assert(msg->data);
			expbuf_set(msg->data, BUF_DATA(&conn->data.payload), BUF_LENGTH(&conn->data.payload));
		
			queue->handler(msg, queue->arg);

			// after the handler has processed.

			// if the message was NOREPLY, then we dont need to reply, and we can clear the message.
			if (msg->noreply == 1) {
				rq_msg_clear(msg);
				assert(msg->id == 0);
				mempool_return(conn->rq->msgpool, msg);
			}
			else {
				// If the message type is now REPLY, then it means that we already have
				// a reply to process.  Dont need to add it to the out-process, as that
				// would already have been done.
				assert(0);
			}
		}
	}
	else {
		// we dont have the required data to handle a request.
		// TODO: This should be handled better.
		assert(0);
	}
}
			
static void processClosing(rq_conn_t *conn)
{
	assert(conn);
	assert(0);
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

static void processReceived(rq_conn_t *conn)
{
	assert(conn);
	assert(0);
}



static void storeQueueID(rq_conn_t *conn, char *queue, int qid)
{
	rq_queue_t *q;
	void *next;

	assert(conn);
	assert(queue);
	assert(qid > 0);
	assert(ll_count(&conn->rq->queues) > 0);

	next = ll_start(&conn->rq->queues);
	q = ll_next(&conn->rq->queues, &next);
	while (q) {
		if (strcmp(q->queue, queue) == 0) {
			assert(q->qid == 0);
			q->qid = qid;
			q = NULL;
		}
		else {
			q = ll_next(&conn->rq->queues, &next);
		}
	}
}




static void cmdClear(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);

	conn->data.mask = 0;
	conn->data.flags = 0;
	
	conn->data.id = 0;
	conn->data.qid = 0;
	conn->data.timeout = 0;
	conn->data.priority = 0;

	expbuf_clear(&conn->data.queue);
	expbuf_clear(&conn->data.payload);
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





static void cmdExecute(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);

	if (BIT_TEST(conn->data.flags, RQ_DATA_FLAG_REQUEST))
		processRequest(conn);
	else if (BIT_TEST(conn->data.flags, RQ_DATA_FLAG_CLOSING))
		processClosing(conn);
	else if (BIT_TEST(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL))
		processServerFull(conn);
	else if (BIT_TEST(conn->data.flags, RQ_DATA_FLAG_RECEIVED))
		processReceived(conn);
	else if (BIT_TEST(conn->data.flags, RQ_DATA_FLAG_DELIVERED))
		processDelivered(conn);
	else if (BIT_TEST(conn->data.mask, RQ_DATA_MASK_QUEUEID))
		storeQueueID(conn, expbuf_string(&conn->data.queue), conn->data.qid);
	else {
		fprintf(stderr, "Unexpected command - (flags:%x, mask:%x)\n", conn->data.flags, conn->data.mask);
		assert(0);
	}
}

static void cmdRequest(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);

	// set the indicated flag.	
	BIT_SET(conn->data.flags, RQ_DATA_FLAG_REQUEST);

	// clear the incompatible flags.
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REQUEST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REPLY);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_RECEIVED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_DELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_BROADCAST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_UNDELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_CLOSING);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_NOREPLY);

	// clear the incompatible masks.
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PRIORITY);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUEID);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_TIMEOUT);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_ID);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUE);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PAYLOAD);
}

//-----------------------------------------------------------------------------
// This command is received from the controller when a request has been sent.
// It indicates that the controller received the message from the node, and is
// routing it to a consumer.
static void cmdReceived(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);

	// set the indicated flag.
	BIT_SET(conn->data.flags, RQ_DATA_FLAG_RECEIVED);

	// clear the incompatible flags.
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REQUEST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REPLY);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_RECEIVED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_DELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_BROADCAST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_UNDELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_CLOSING);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_NOREPLY);

	// clear the incompatible masks.
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PRIORITY);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUEID);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_TIMEOUT);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_ID);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUE);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PAYLOAD);

}

static void cmdDelivered(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);

	// set the indicated flag.
	BIT_SET(conn->data.flags, RQ_DATA_FLAG_DELIVERED);

	// clear the incompatible flags.
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REQUEST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REPLY);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_RECEIVED);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_DELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_BROADCAST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_UNDELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_CLOSING);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_NOREPLY);

	// clear the incompatible masks.
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PRIORITY);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUEID);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_TIMEOUT);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_ID);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUE);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PAYLOAD);
}

static void cmdBroadcast(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);

	// set the indicated flag.
	BIT_SET(conn->data.flags, RQ_DATA_FLAG_BROADCAST);

	// clear the incompatible flags.
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REQUEST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REPLY);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_RECEIVED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_DELIVERED);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_BROADCAST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_UNDELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_CLOSING);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_NOREPLY);

	// clear the incompatible masks.
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PRIORITY);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUEID);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_TIMEOUT);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_ID);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUE);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PAYLOAD);
	
}
	
static void cmdNoreply(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	assert(conn);

	// set the indicated flag.
	BIT_SET(conn->data.flags, RQ_DATA_FLAG_NOREPLY);

	// clear the incompatible flags.
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REQUEST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REPLY);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_RECEIVED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_DELIVERED);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_BROADCAST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_UNDELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_CLOSING);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_NOREPLY);

	// clear the incompatible masks.
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PRIORITY);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUEID);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_TIMEOUT);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_ID);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUE);
// 	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PAYLOAD);
	
}

static void cmdClosing(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;

	assert(conn);

	// set the indicated flag.
	BIT_SET(conn->data.flags, RQ_DATA_FLAG_CLOSING);

	// clear the incompatible flags.
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REQUEST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REPLY);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_RECEIVED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_DELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_BROADCAST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_UNDELIVERED);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_CLOSING);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_NOREPLY);

	// clear the incompatible masks.
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PRIORITY);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUEID);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_TIMEOUT);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_ID);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUE);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PAYLOAD);
	
}
	
static void cmdServerFull(void *ptr)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;

	assert(conn);

	// set the indicated flag.
	BIT_SET(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL);

	// clear the incompatible flags.
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REQUEST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_REPLY);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_RECEIVED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_DELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_BROADCAST);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_UNDELIVERED);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_CLOSING);
// 	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_SERVER_FULL);
	BIT_CLEAR(conn->data.flags, RQ_DATA_FLAG_NOREPLY);

	// clear the incompatible masks.
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PRIORITY);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUEID);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_TIMEOUT);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_ID);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_QUEUE);
	BIT_CLEAR(conn->data.mask, RQ_DATA_MASK_PAYLOAD);
	
}
	
static void cmdID(void *ptr, risp_int_t value)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
	assert(conn);
	assert(value > 0 && value <= 0xffff);
	
	conn->data.id = value;
	BIT_SET(conn->data.mask, RQ_DATA_MASK_ID);

}
	
static void cmdQueueID(void *ptr, risp_int_t value)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
	assert(conn);
	assert(value > 0 && value <= 0xffff);
	
	conn->data.qid = value;
	BIT_SET(conn->data.mask, RQ_DATA_MASK_QUEUEID);

}
	
static void cmdTimeout(void *ptr, risp_int_t value)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
	assert(conn);
	assert(value > 0 && value <= 0xffff);
	
	conn->data.timeout = value;
	BIT_SET(conn->data.mask, RQ_DATA_MASK_TIMEOUT);
}
	
static void cmdPriority(void *ptr, risp_int_t value)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;

	assert(conn);
	assert(value > 0 && value <= 0xffff);
	
	conn->data.priority = value;
	BIT_SET(conn->data.mask, RQ_DATA_MASK_PRIORITY);
}
	
static void cmdPayload(void *ptr, risp_length_t length, risp_char_t *data)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
	
 	assert(conn);
 	assert(length > 0);
 	assert(data);
 	
 	expbuf_set(&conn->data.payload, data, length);
	BIT_SET(conn->data.mask, RQ_DATA_MASK_PAYLOAD);
}


static void cmdQueue(void *ptr, risp_length_t length, risp_char_t *data)
{
	rq_conn_t *conn = (rq_conn_t *) ptr;
 	assert(conn != NULL);
 	assert(length > 0);
 	assert(data != NULL);
 	expbuf_set(&conn->data.queue, data, length);
 	BIT_SET(conn->data.mask, RQ_DATA_MASK_QUEUE);
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
	risp_add_command(rq->risp, RQ_CMD_EXECUTE,      &cmdExecute);
	risp_add_command(rq->risp, RQ_CMD_PING,         &cmdPing);
	risp_add_command(rq->risp, RQ_CMD_PONG,         &cmdPong);
	risp_add_command(rq->risp, RQ_CMD_REQUEST,      &cmdRequest);
	risp_add_command(rq->risp, RQ_CMD_RECEIVED,     &cmdReceived);
	risp_add_command(rq->risp, RQ_CMD_DELIVERED,    &cmdDelivered);
  risp_add_command(rq->risp, RQ_CMD_BROADCAST,    &cmdBroadcast);
	risp_add_command(rq->risp, RQ_CMD_NOREPLY,      &cmdNoreply);
	risp_add_command(rq->risp, RQ_CMD_CLOSING,      &cmdClosing);
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
// Initialise a message struct.  Assumes that the struct is currently invalid.
// Overwrites everything.
rq_message_t * rq_msg_new(rq_t *rq)
{
	rq_message_t *msg;
	
	assert(rq);
	assert(rq->msgpool);

	// need to get a message struct from the mempool.
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

	assert(rq->bufpool);
	msg->data = expbuf_pool_new(rq->bufpool, 0);
}

//-----------------------------------------------------------------------------
// clean up the resources used by the message so that it can be used again.  We
// will not yet clean up the data buffer, because it can stay until the message
// list is actually destroyed on shutdown.
void rq_msg_clear(rq_message_t *msg)
{
	assert(msg != NULL);

	msg->id = 0;
	msg->broadcast = 0;
	msg->noreply = 0;
	msg->queue = NULL;

	assert(msg->data);
	expbuf_clear(msg->data);
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


// This function is used to send a reply for a request.  The data being sent back should be placed in the data buffer.
void rq_reply(rq_message_t *msg)
{
	assert(msg);
	assert(msg->rq);

	assert(0);
}