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

	// free all the buffers.
	expbuf_free(&conn->readbuf);
	expbuf_free(&conn->in);
	expbuf_free(&conn->out);
	expbuf_free(&conn->build);

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

	// free the conn object.
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
}


void rq_setevbase(rq_t *rq, struct event_base *base)
{
	assert(rq != NULL);
	assert(base != NULL);

	assert(rq->evbase == NULL);
	rq->evbase = base;
}

// Handle the timeout event, and call the user's handler.
void rq_timeout_handler(int fd, short flags, void *ptr)
{
	rq_timeout_t *timeout;

	assert(fd == -1);
//	assert(flags == 0);
	assert(ptr != NULL);

	timeout = ptr;
	
	assert(timeout != NULL);
	assert(timeout->handler != NULL);
	assert(timeout->clockevent.ev_base != NULL);

	void (*func)(void *arg) = NULL;

	evtimer_del(&timeout->clockevent);
	func = timeout->handler;
	func(timeout->arg);

	free(timeout);	
}


//-----------------------------------------------------------------------------
// Sets a timeout events onto the event queue it will only fire one time, so if
// you want it to occur frequently then the event needs to be created each
// time.  Once the timeout is set, you cannot cancel it.  If you dont want
// something processed, you need to handle it yourself.
//
// We do not want to limit the number of timeouts that can occur.  So we need
// to create dynamic event structures.  We dont want to manage a list of those
// event structures as that becomes complicated.  So instead, when we create a
// timer, we build an event structure, and give it the pointer to the arg and
// handler that was specified, and then we pass a pointer to our new structure
// to the event loop.  When the timeout occurs, our internal handler picks it
// up, pulls out the real handler and arg, and then passes control to them.
void rq_settimeout(rq_t *rq, unsigned int msecs, void (*handler)(void *arg), void *arg)
{
	rq_timeout_t *tptr;
	struct timeval t = {.tv_sec = 1, .tv_usec = 0};

	assert(rq != NULL);
	assert(msecs > 0);
	assert(handler != NULL);

	// since 1 second timers are the most common that will be the default,
	// anything else and we will calculate it, which is slower.
	if (msecs != 1000) {
		if (msecs >= 1000) {
			t.tv_sec = msecs / 1000;
			t.tv_usec = (msecs % 1000) * 1000;
		}
		else {
			t.tv_sec = 0;
			t.tv_usec = msecs * 1000;
		}
	}

	// rq object needs to be added to the event system before a timeout can be set.
	assert(rq->evbase != NULL);

	tptr = (rq_timeout_t *) malloc(sizeof(rq_timeout_t));
	assert(tptr != NULL);
	tptr->handler = handler;
	tptr->arg = arg;

  evtimer_set(&tptr->clockevent, rq_timeout_handler, (void *)tptr);
  event_base_set(rq->evbase, &tptr->clockevent);
  evtimer_add(&tptr->clockevent, &t);
	assert(tptr->clockevent.ev_base == rq->evbase);
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
	
	assert(conn->readbuf.length == 0);
	assert(conn->readbuf.max >= RQ_DEFAULT_BUFFSIZE);

	empty = 0;
	while (empty == 0) {
		assert(conn->readbuf.length == 0);
		assert(conn->handle != INVALID_HANDLE && conn->handle > 0);
		assert(conn->readbuf.data != NULL  && conn->readbuf.max > 0);
		
		res = read(conn->handle, conn->readbuf.data, conn->readbuf.max);
		if (res > 0) {
			conn->readbuf.length = res;
			assert(conn->readbuf.length <= conn->readbuf.max);

			// if we pulled out the max we had avail in our buffer, that means we
			// can pull out more at a time, so we should increase our buffer size.
			if (res == conn->readbuf.max) {
				expbuf_shrink(&conn->readbuf, conn->readbuf.max + RQ_DEFAULT_BUFFSIZE);
				assert(empty == 0);
			}
			else { empty = 1; }
			
			// if there is no data in the in-buffer, then we will process the common buffer by itself.
			if (conn->in.length == 0) {
				res = risp_process(conn->risp, conn, conn->readbuf.length, (unsigned char *) conn->readbuf.data);
				assert(res <= conn->readbuf.length);
				assert(res >= 0);
				if (res > 0) { expbuf_purge(&conn->readbuf, res); }

				// if there is data left over, then we need to add it to our in-buffer.
				if (conn->readbuf.length > 0) {
					expbuf_add(&conn->in, conn->readbuf.data, conn->readbuf.length);
					expbuf_clear(&conn->readbuf);
				}
			}
			else {
				// we have data left in the in-buffer, so we add the content of the common buffer
				assert(conn->readbuf.length > 0);
				expbuf_add(&conn->in, conn->readbuf.data, conn->readbuf.length);
				expbuf_clear(&conn->readbuf);
				assert(conn->readbuf.length == 0);
				assert(conn->in.length > 0 && conn->in.data != NULL);

				// and then process what is there.
				res = risp_process(conn->risp, conn, conn->in.length, (unsigned char *) conn->in.data);
				assert(res <= conn->in.length);
				assert(res >= 0);
				if (res > 0) { expbuf_purge(&conn->in, res); }
			}
		}
		else {
			assert(empty == 0);
			empty = 1;
			
			if (res == 0) {
				printf("Node[%d] closed while reading.\n", conn->handle);
				rq_conn_closed(conn);
			}
			else {
				assert(res == -1);
				if (errno != EAGAIN && errno != EWOULDBLOCK) {
					printf("Node[%d] closed while reading- because of error: %d\n", conn->handle, errno);
					close(conn->handle);
					rq_conn_closed(conn);
				}
			}
		}
	}
	
	assert(conn->readbuf.length == 0);
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
	if (conn->out.length == 0) {
		if (event_del(conn->write_event) != -1) {
			event_free(conn->write_event);
			conn->write_event = NULL;
		}
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
	if (conn->status == active && conn->out.length == 0) {
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
		expbuf_add(&conn->out, data+res, length-res);
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
	assert(conn);
	assert(queue);

	assert(queue->queue);
	assert(queue->max >= 0);

	printf("Sending queue request for '%s' to %d\n", queue->queue, conn->handle);

	// send consume request to controller.
	addCmd(&conn->build, RQ_CMD_CLEAR);
	addCmd(&conn->build, RQ_CMD_CONSUME);
	if (queue->exclusive != 0)
		addCmd(&conn->build, RQ_CMD_EXCLUSIVE);
	addCmdShortStr(&conn->build, RQ_CMD_QUEUE, strlen(queue->queue), queue->queue);
	addCmdInt(&conn->build, RQ_CMD_MAX, queue->max);
	addCmdShortInt(&conn->build, RQ_CMD_PRIORITY, queue->priority);
	addCmd(&conn->build, RQ_CMD_EXECUTE);

	rq_senddata(conn, conn->build.data, conn->build.length);
	expbuf_clear(&conn->build);
	
	assert(conn->build.length == 0);
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

	printf("connected.\n");
		
	assert(conn->status == connecting);
	conn->status = active;

	// remove the connect handler, and apply the regular read handler now.
	assert(conn->rq->evbase != NULL);
	assert(conn->read_event);
	
	event_del(conn->read_event);
	conn->read_event = event_new(conn->rq->evbase, conn->handle, EV_READ | EV_PERSIST, rq_read_handler, conn);
	event_add(conn->read_event, NULL);

	// if we have data in our out buffer, we need to create the WRITE event.
	if (conn->out.length > 0) {
		assert(conn->handle != INVALID_HANDLE && conn->handle > 0);
		assert(conn->write_event == NULL);
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
	int sock;
	rq_conn_t *conn, *first;
	int i, j;
	struct sockaddr_in sin;
	unsigned long ulAddress;
	struct hostent *hp;
	int result;

	assert(rq != NULL);

	assert(rq->evbase != NULL);
	assert(ll_count(&rq->connlist) > 0);

	conn = ll_get_head(&rq->connlist);
	first = conn;
	result = -1;
	while (conn && result < 0) {

		assert(conn->hostname != NULL);
		assert(conn->hostname[0] != '\0');
		assert(conn->port > 0);
		assert(conn->read_event == NULL);
		assert(conn->write_event == NULL);

		// if the hostname hasn't been resolved, then resolve it.
		if (conn->resolved[0] == 0) {
			// Look up by standard notation (xxx.xxx.xxx.xxx) first.
			ulAddress = inet_addr(conn->hostname);
			if ( ulAddress != (unsigned long)(-1) )  {
				conn->resolved[0] = ulAddress;
			}
			else {
				// If that didn't work, try to resolve host name by DNS.
				hp = gethostbyname(conn->hostname);
				if( hp != NULL ) {
					for (j=0; j<5 && j< hp->h_length; j++) {
						conn->resolved[j] = hp->h_addr[j];
					}
					assert((j>=5) || (conn->resolved[j] == 0) );
				}
			}
		}

		// if the hostname was resolved, then create the socket, bind and connect.
		if (conn->resolved[0] != 0) {

			sin.sin_family = AF_INET;
			sin.sin_port = htons(conn->port);

			for (j=0; j<5 && result < 0 && conn->resolved[j] != 0; j++) {

				sin.sin_addr.s_addr = conn->resolved[j];

        sock = socket(AF_INET,SOCK_STREAM,0);
        if (sock >= 0) {
					int opts;

					// Before we attempt to connect, set the socket to non-blocking mode.
					opts = fcntl(sock, F_GETFL);
					if (opts >= 0) { fcntl(sock, F_SETFL, (opts | O_NONBLOCK)); }

					result = connect(sock, (struct sockaddr*)&sin, sizeof(struct sockaddr));
					if (result < 0 && errno == EINPROGRESS) {

						result = 0;
						conn->handle = sock;
						conn->status = connecting;

						printf("connect initiated.\n");

						// setup the event handler.
						assert(rq->evbase);
						assert(conn->handle != INVALID_HANDLE && conn->handle > 0);
						assert(conn->read_event == NULL);
						assert(conn->write_event == NULL);
						
						conn->read_event = event_new(rq->evbase, conn->handle, (EV_WRITE | EV_PERSIST), rq_connect_handler, conn);
						event_add(conn->read_event, NULL);
					}
					else {
						printf("-- connect failed. result=%d, errno=%d\n", result, errno);
						close(sock);
						sock = 0;
						assert(result == -1);
						
					}
				}
			}

			// if we dont have a connection...
			if (result < 0) {
				// connection failed.
				
				// Need to pop this connection from the top of the list and add it to the bottom.
				conn = ll_pop_head(&rq->connlist);
				ll_push_tail(&rq->connlist, conn);

				// and now get the next one from the list.
				conn = ll_get_head(&rq->connlist);
	
				// if we get a conn that is the same as the first one we tried, then we need to exit with a failure.
				if (conn == first)
					conn = NULL;
			}
		}
	}
}



//-----------------------------------------------------------------------------
// add a controller to the end of the connection list.   If this is the first
// controller, then we need to attempt to connect to it, and setup the socket
// on the event queue.
void rq_addcontroller(rq_t *rq, char *host, int port)
{
	rq_conn_t *conn;
	int j;
	
	assert(rq != NULL);
	assert(host != NULL && port > 0);
	assert(host[0] != '\0');
	
	conn = (rq_conn_t *) malloc(sizeof(rq_conn_t));
	assert(conn);
	
	conn->hostname = host;
	conn->port = port;
	for(j=0; j<5; j++) {
		conn->resolved[j] = 0;
	}

	conn->handle = INVALID_HANDLE;		// socket handle to the connected controller.
	conn->read_event = NULL;
	conn->write_event = NULL;

	expbuf_init(&conn->readbuf, RQ_DEFAULT_BUFFSIZE);
	expbuf_init(&conn->in, 0);
	expbuf_init(&conn->out, 0);
	expbuf_init(&conn->build, 0);

	rq_data_init(&conn->data);

	assert(rq->risp);
	conn->risp = rq->risp;

	conn->rq = rq;
	conn->status = inactive;

	ll_push_tail(&rq->connlist, conn);

	// if this is the only controller we have so far, then we need to attempt the
	// connect (non-blocking)
	if (ll_count(&rq->connlist) == 1) {
		printf("calling rq_connect()\n");
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
	
	assert(conn);

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
			addCmd(&conn->build, RQ_CMD_CLEAR);
			addCmd(&conn->build, RQ_CMD_UNDELIVERED);
			addCmdLargeInt(&conn->build, RQ_CMD_ID, (short int)msgid);
			addCmd(&conn->build, RQ_CMD_EXECUTE);
			rq_senddata(conn, conn->build.data, conn->build.length);
			expbuf_clear(&conn->build);
		}
		else {
			// send a delivery message back to the controller.
			addCmd(&conn->build, RQ_CMD_CLEAR);
			addCmd(&conn->build, RQ_CMD_DELIVERED);
			addCmdLargeInt(&conn->build, RQ_CMD_ID, (short int)msgid);
			addCmd(&conn->build, RQ_CMD_EXECUTE);
			rq_senddata(conn, conn->build.data, conn->build.length);
			expbuf_clear(&conn->build);

			// create a message structure.
			msg = ll_get_tail(&conn->messages);
			if (msg == NULL) {
				// there is no message object in the list.  We need to create one.
				msg = (rq_message_t *) malloc(sizeof(rq_message_t));
				rq_message_init(msg);
			}
			else if (msg->id > 0) {
				// the message at the tail, is in use, so we need to create one.
				msg = (rq_message_t *) malloc(sizeof(rq_message_t));
				rq_message_init(msg);
			}
			else {
				// the message is ready to be used, so we need to pop it off.
				msg = ll_pop_tail(&conn->messages);
			}
			assert(msg);
			assert(msg->id == 0);

			// fill out the message details, and add it to the head of the messages list.
			assert(queue && msgid > 0);
			msg->queue = queue;
			msg->type = RQ_TYPE_REQUEST;
			msg->id = msgid;
			if (BIT_TEST(conn->data.flags, RQ_DATA_FLAG_NOREPLY))
				msg->noreply = 1;

			expbuf_set(&msg->data, conn->data.payload.data, conn->data.payload.length);
		
			queue->handler(msg, queue->arg);

			// after the handler has processed.

			// if the message was NOREPLY, then we dont need to reply, and we can clear the message.
			if (msg->noreply == 1) {
				rq_message_clear(msg);
				assert(msg->id == 0);
				ll_push_tail(&conn->messages, msg);
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
		printf("Unexpected command - (flags:%x, mask:%x)\n", conn->data.flags, conn->data.mask);
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
	printf("Received invalid (%d)): [%d, %d, %d]\n", len, cast[0], cast[1], cast[2]);
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
}




//-----------------------------------------------------------------------------
// Initialise a message struct.  Assumes that the struct is currently invalid.
// Overwrites everything.
void rq_message_init(rq_message_t *msg)
{
	assert(msg != NULL);
	
	msg->queue = NULL;
	msg->type = 0;
	msg->id = 0;
	msg->noreply = 0;

	expbuf_init(&msg->data, 0);
}

//-----------------------------------------------------------------------------
// clean up the resources used by the message so that it can be used again.  We will not yet clean up the data buffer, because it can stay until the message list is actually destroyed on shutdown.
void rq_message_clear(rq_message_t *msg)
{
	assert(msg != NULL);

	msg->id = 0;
	msg->type = 0;
	msg->noreply = 0;
	msg->queue = NULL;
}


void rq_message_setqueue(rq_message_t *msg, char *queue)
{
	assert(msg != NULL);
	assert(queue != NULL);
	assert(msg->queue == NULL);

	msg->queue = queue;
}


void rq_message_setbroadcast(rq_message_t *msg)
{
	assert(msg != NULL);
	assert(msg->type == 0);

	msg->type = RQ_TYPE_BROADCAST;
}

void rq_message_setnoreply(rq_message_t *msg)
{
	assert(msg != NULL);
	assert(msg->noreply == 0);
	msg->noreply = 1;
}

// This function expects a pointer to memory that it now controls.  When it is finished 
void rq_message_setdata(rq_message_t *msg, int length, char *data)
{
	assert(msg != NULL);
	assert(length > 0);
	assert(data != NULL);

	expbuf_set(&msg->data, data, length);
}

// send a message to the controller.
void rq_send(rq_t *rq, rq_message_t *msg)
{
	assert(rq != NULL);
	assert(msg != NULL);

	if (msg->type == 0);
		msg->type = RQ_TYPE_REQUEST;

	assert(msg->data.length > 0);

	// if there is no data in the 'out' queue, then attempt to send the request to the socket.  Any that we are unable to send, should be added to teh out queue, and the WRITE event should be established.

	assert(0);
}


