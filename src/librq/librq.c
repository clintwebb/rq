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
	data->op = RQ_CMD_NOP;
	data->broadcast = 0;
	data->noreply = 0;
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
}

// Initialise an RQ structure.  
void rq_init(rq_t *rq)
{
	assert(rq != NULL);

	rq->handle = INVALID_HANDLE;		// socket handle to the connected controller.
	memset(&rq->event, 0, sizeof(rq->event));
	rq->evbase = NULL;
	rq->risp = NULL;

	rq->connlist = NULL;
	rq->conns = 0;
	rq->connactive = -1;

	rq->queuelist = NULL;
	int queues = 0;
	
// 	rq->arg = NULL;
// 	rq->handler = NULL;

	expbuf_init(&rq->readbuf, RQ_DEFAULT_BUFFSIZE);
	expbuf_init(&rq->in, 0);
	expbuf_init(&rq->out, 0);
	expbuf_init(&rq->build, 0);

	rq_data_init(&rq->data);
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



void rq_cleanup(rq_t *rq)
{
	assert(rq != NULL);


	assert((rq->connlist == NULL && rq->conns == 0) || (rq->connlist != NULL && rq->conns > 0));
	assert(rq->risp != NULL);
	risp_shutdown(rq->risp);
	rq->risp = NULL;

	assert(rq->handle == INVALID_HANDLE);
	if (rq->connlist != NULL) {
		free(rq->connlist);
		rq->connlist = NULL;
		rq->conns = 0;
		rq->connactive = -1;
	}

	expbuf_free(&rq->readbuf);
	expbuf_free(&rq->in);
	expbuf_free(&rq->out);
	expbuf_free(&rq->build);

	rq_data_free(&rq->data);

	assert((rq->queues == 0 && rq->queuelist == NULL) || (rq->queues > 0 && rq->queuelist != NULL));
	while (rq->queues > 0) {
		rq->queues--;
		if (rq->queuelist[rq->queues] != NULL) {
			rq_queue_free(rq->queuelist[rq->queues]);
			free(rq->queuelist[rq->queues]);
			rq->queuelist[rq->queues] = NULL;
		}
	}
	if (rq->queuelist != NULL) {
		free(rq->queuelist);
		rq->queuelist = NULL;
	}

	assert(rq->queues == 0 && rq->queuelist == NULL);
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


void rq_addcontroller(rq_t *rq, char *host, int port)
{
	rq_conn_t *conn;
	int j;
	
	assert(rq != NULL);
	assert(host != NULL && port > 0);
	assert(host[0] != '\0');
	
	if (rq->conns == 0) {
		rq->connlist = (rq_conn_t *) malloc(sizeof(rq_conn_t));
		conn = rq->connlist;
		rq->conns ++;
	}
	else {
		rq->connlist = (rq_conn_t *) realloc(rq->connlist, sizeof(rq_conn_t) * (rq->conns + 1));
		conn = &rq->connlist[rq->conns];
		rq->conns ++;
	}
	
	conn->hostname = host;
	conn->port = port;
	for(j=0; j<5; j++) {
		conn->resolved[j] = 0;
	}
}



//-----------------------------------------------------------------------------
// Assuming that the rq structure has been properlly filled out, this function
// will initiate the connection process to a specified IP address.   Since the
// application cant really do anything until it has connected to the
// controller, there is very little point in connecting in non-blocking mode.
// So we will connect in blocking mode and will return when we have either
// connected, or there was an error. If we are connected, then we will return 0.
// Otherwise we will return some other number (probably -1).
// We will try to connect to each controller.  if all fail, then this function
// will exit.
int rq_connect(rq_t *rq)
{
	int result = -1;
	int sock;
	rq_conn_t *conn;
	int i, j;
	struct sockaddr_in sin;
	unsigned long ulAddress;
	struct hostent *hp;

	assert(rq != NULL);

	assert(rq->handle == INVALID_HANDLE);
	assert(rq->evbase != NULL);
	assert(rq->conns > 0);
	assert(rq->connlist != NULL);

	for (i=0; i<rq->conns && result < 0; i++) {

		conn = &rq->connlist[i];

		assert(conn->hostname != NULL);
		assert(conn->hostname[0] != '\0');
		assert(conn->port > 0);

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
					if (connect(sock, (struct sockaddr*)&sin, sizeof(struct sockaddr)) >= 0) {
						int opts;

						// we are connected.  Set the socket to non-blocking mode.
						opts = fcntl(sock, F_GETFL);
						if (opts >= 0) { fcntl(sock, F_SETFL, (opts | O_NONBLOCK)); }

						result = 0;
						rq->handle = sock;
						rq->connactive = i;
						assert(rq->connactive < rq->conns);
					}
					else {
						close(sock);
						sock = 0;
						assert(result == -1);
					}
				}
			}
		}	
	}

	assert((result < 0 && rq->handle == INVALID_HANDLE) || (result == 0 && rq->handle > 0));

	return (result);
}


// this function is an internal one taht is used to read data from the socket.
// It is assumed that we are pretty sure that there is data to be read (or the
// socket has been closed).
static void rq_process_read(rq_t *rq)
{
	int res, empty;
	
	assert(rq != NULL);

	assert(rq != NULL);
	assert(rq->readbuf.length == 0);
	assert(rq->readbuf.max >= RQ_DEFAULT_BUFFSIZE);

	empty = 0;
	while (empty == 0) {
		assert(rq->readbuf.length == 0);
		assert(rq->risp != NULL);
		assert(rq->handle != INVALID_HANDLE && rq->handle > 0);
		assert(rq->readbuf.data != NULL);
		assert(rq->readbuf.max > 0);
		
printf("rq(%d) - reading data\n", rq->handle);
		
		res = read(rq->handle, rq->readbuf.data, rq->readbuf.max);
		if (res > 0) {
			rq->readbuf.length = res;
			assert(rq->readbuf.length <= rq->readbuf.max);

printf("rq(%d) - data received (%d)\n", rq->handle, res);

			// if we pulled out the max we had avail in our buffer, that means we can pull out more at a time.
			if (res == rq->readbuf.max) {
				expbuf_shrink(&rq->readbuf, rq->readbuf.max + RQ_DEFAULT_BUFFSIZE);
				assert(empty == 0);
			}
			else { empty = 1; }
			
			// if there is no data in the in-buffer, then we will process the common buffer by itself.
			if (rq->in.length == 0) {
				res = risp_process(rq->risp, rq, rq->readbuf.length, (unsigned char *) rq->readbuf.data);
				assert(res <= rq->readbuf.length);
				assert(res >= 0);
				if (res > 0) { expbuf_purge(&rq->readbuf, res); }

				// if there is data left over, then we need to add it to our in-buffer.
				if (rq->readbuf.length > 0) {
					expbuf_add(&rq->in, rq->readbuf.data, rq->readbuf.length);
					expbuf_clear(&rq->readbuf);
				}
			}
			else {
				// we have data left in the in-buffer, so we add the content of the common buffer
				assert(rq->readbuf.length > 0);
				expbuf_add(&rq->in, rq->readbuf.data, rq->readbuf.length);
				expbuf_clear(&rq->readbuf);
				assert(rq->readbuf.length == 0);
				assert(rq->in.length > 0 && rq->in.data != NULL);

				res = risp_process(rq->risp, rq, rq->in.length, (unsigned char *) rq->in.data);
				assert(res <= rq->in.length);
				assert(res >= 0);
				if (res > 0) { expbuf_purge(&rq->in, res); }
			}
		}
		else {
			assert(empty == 0);
			empty = 1;
			
			if (res == 0) {
				printf("Node[%d] closed while reading.\n", rq->handle);
				rq->handle = INVALID_HANDLE;
				rq_cleanup(rq);
			}
			else {
				assert(res == -1);
				if (errno != EAGAIN && errno != EWOULDBLOCK) {
					printf("Node[%d] closed while reading- because of error: %d\n", rq->handle, errno);
					close(rq->handle);
					rq->handle = INVALID_HANDLE;
					rq_cleanup(rq);
				}
			}
		}
	}
	
	assert(rq->readbuf.length == 0);
}

static void rq_process_write(rq_t *rq)
{
	assert(rq != NULL);
	assert(0);
}

static void rq_process_handler(int fd, short int flags, void *arg)
{
	rq_t *rq;

	assert(fd >= 0);
	assert(flags != 0);

	rq = (rq_t *) arg;

	assert(rq != NULL);

	printf("Processing data: rqh=%d, fd=%d, flags=%d\n", rq->handle, fd, flags);
	
	assert(rq->handle == fd);

	if (flags & EV_READ) {
		printf("rq_process_handler(%d, EV_READ))\n", fd);
		rq_process_read(rq);
	}
	
	if (flags & EV_WRITE) {
		printf("rq_process_handler(%d, EV_WRITE))\n", fd);
		rq_process_write(rq);
	}
}


// this function is used internally to send the data to the connected RQ
// controller.  If there is no data currently in the outbound queue, then we
// will try and send it straight away.   If we couldn't send any or all of it,
// then we add it to the out queue.  If there was already data in the out
// queue, then we add this data to the out queue and wait let the event system
// take care of it.
static void rq_senddata(rq_t *rq, char *data, int length)
{
	int res = 0;
	
	assert(rq != NULL);
	assert(data != NULL);
	assert(length > 0);

	assert(rq->handle != INVALID_HANDLE);

	// if the out buffer is empty, then we will try and send what we've got straight away.
	if (rq->out.length == 0) {
		res = send(rq->handle, data, length, 0);
		if (res > 0) {
			assert(res <= length);
		}
		else 	if (res == 0) {
			// socket was closed.   how best to handle that?
			event_del(&rq->event);
			rq->handle = INVALID_HANDLE;

			assert(0);
		}
		else if (res == -1) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				// we've had a failure.
				close(rq->handle);
				event_del(&rq->event);
				rq->handle = INVALID_HANDLE;

				assert(0);
			}
		}
	}

	// if the connection is still valid, and we haven't sent everything yet,
	// then we need to add it to the outgoing queue, and then raise an event.
	if (res < length && rq->handle != INVALID_HANDLE) {
		// there is already data in the out queue...
		if (res < 0) { res = 0; }
		expbuf_add(&rq->out, data+res, length-res);
		if (event_del(&rq->event) != -1) {
			assert(rq->handle != INVALID_HANDLE && rq->handle > 0);
			event_set(&rq->event, rq->handle, EV_READ | EV_WRITE | EV_PERSIST, rq_process_handler, (void *)rq);
			event_base_set(rq->evbase, &rq->event);
			event_add(&rq->event, 0);
		}
	}	
}







//-----------------------------------------------------------------------------
// Send a request to the controller indicating a desire to consume a particular
// queue.  We will add queue information to our RQ structure.
void rq_consume(rq_t *rq, char *queue, int max, int priority, void (*handler)(rq_message_t *msg, void *arg), void *arg)
{
	int i;
	int found;
	int avail=-1;
	
	assert(rq != NULL);
	assert(queue != NULL);
	assert(max >= 0);
	assert(priority == RQ_PRIORITY_NONE || priority == RQ_PRIORITY_LOW || priority == RQ_PRIORITY_NORMAL || RQ_PRIORITY_HIGH);
	assert(handler != NULL);

	// check that we are connected to a controller.
	assert(rq->handle != INVALID_HANDLE && rq->connactive >= 0);

	assert(rq->build.length == 0);

	// check that we are not already consuming this queue.
	assert((rq->queues == 0 && rq->queuelist == NULL) || (rq->queues > 0 && rq->queuelist != NULL));
	found = 0;
	for (i=0; i<rq->queues && found == 0; i++) {
		if (rq->queuelist[i] != NULL) {
			if (strcmp(rq->queuelist[i]->queue, queue) == 0) {
				// the queue is already in our list...
	
				// we send a message to controller cancelling the queue.
				addCmd(&rq->build, RQ_CMD_CLEAR);
				addCmd(&rq->build, RQ_CMD_CANCEL_QUEUE);
				addCmdShortStr(&rq->build, RQ_CMD_QUEUE, strlen(queue), queue);
				addCmd(&rq->build, RQ_CMD_EXECUTE);
				
				// we over-write what is already in there...
				rq->queuelist[i]->handler = handler;
				rq->queuelist[i]->arg = arg;
				
				found ++;
			}
		}
		else if (avail < 0) {
			avail = i;
		}
	}

	if (found == 0) {
		if (avail < 0) {
			// this is a new queue, create queue entry in rq.
		
			rq->queuelist = (rq_queue_t **) realloc(rq->queuelist, sizeof(rq_queue_t *) * (rq->queues + 1));
			rq->queuelist[rq->queues] = NULL;
			avail = rq->queues;
			rq->queues ++;
		}

		assert(avail >= 0);
		assert(rq->queuelist[avail] == NULL);
		rq->queuelist[avail] = (rq_queue_t *) malloc(sizeof(rq_queue_t));
		assert(rq->queuelist[avail] != NULL);
		rq_queue_init(rq->queuelist[avail]);
		rq->queuelist[avail]->queue = queue;
		rq->queuelist[avail]->handler = handler;
		rq->queuelist[avail]->arg = arg;
	}
	
	// send consume request to controller.
	addCmd(&rq->build, RQ_CMD_CLEAR);
	addCmd(&rq->build, RQ_CMD_CONSUME);
	addCmdShortStr(&rq->build, RQ_CMD_QUEUE, strlen(queue), queue);
	addCmdInt(&rq->build, RQ_CMD_MAX, (short int)max);
	addCmdShortInt(&rq->build, RQ_CMD_PRIORITY, (unsigned char)priority);
	addCmd(&rq->build, RQ_CMD_EXECUTE);

	rq_senddata(rq, rq->build.data, rq->build.length);
	expbuf_clear(&rq->build);

	assert(rq->build.length == 0);
}




static void processRequest(rq_t *rq)
{
	assert(rq != NULL);
	assert(0);
}
			
static void processClosing(rq_t *rq)
{
	assert(rq != NULL);
	assert(0);
}

static void processServerFull(rq_t *rq)
{
	assert(rq != NULL);
	assert(0);
}

static void processDelivered(rq_t *rq)
{
	assert(rq != NULL);
	assert(0);
}

static void processReceived(rq_t *rq)
{
	assert(rq != NULL);
	assert(0);
}



static void storeQueueID(rq_t *rq, char *queue, int qid)
{
	int i;
	
	assert(rq != NULL);
	assert(queue != NULL);
	assert(qid > 0);
	assert(rq->queues > 0);

	for (i=0; i<rq->queues; i++) {
		if (rq->queuelist[i] != NULL) {
			assert(rq->queuelist[i]->queue != NULL);
			if (strcmp(rq->queuelist[i]->queue, queue) == 0) {
				assert(rq->queuelist[i]->qid == 0);
				rq->queuelist[i]->qid = qid;
				break;
			}
		}
	}
}




static void cmdClear(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	rq->data.op = RQ_CMD_NOP;
	rq->data.broadcast = 0;
	rq->data.noreply = 0;
	rq->data.id = 0;
	rq->data.qid = 0;
	rq->data.timeout = 0;
	rq->data.priority = 0;

	printf("CLEAR\n");
}

static void cmdExecute(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	printf("EXECUTE (%d)\n", rq->data.op);
	
	switch (rq->data.op) {

		case RQ_CMD_REQUEST:
			processRequest(rq);
			break;

		case RQ_CMD_CLOSING:
			processClosing(rq);
			break;

		case RQ_CMD_SERVER_FULL:
			processServerFull(rq);
			break;

		case RQ_CMD_RECEIVED:
			processReceived(rq);
			break;
	
		case RQ_CMD_DELIVERED:
			processDelivered(rq);
			break;
	
		default:
			// what do we do if we dont have a valid operation?
			if (rq->data.qid > 0 && rq->data.queue.length > 0) {
				// the queue has been linked, so we need to mark the qid.
				storeQueueID(rq, expbuf_string(&rq->data.queue), rq->data.qid);
			}
			else {
				printf("Unexpected command - %d\n", rq->data.op);
				assert(0);
			}
			break;
	}
}

static void cmdRequest(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	rq->data.op = RQ_CMD_REQUEST;
	printf("REQUEST\n");
}

static void cmdReceived(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	rq->data.op = RQ_CMD_RECEIVED;
	printf("RECEIVED\n");
}

static void cmdDelivered(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	rq->data.op = RQ_CMD_DELIVERED;
	printf("DELIVERED\n");
}

static void cmdBroadcast(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	rq->data.broadcast = 1;
	printf("BROADCAST\n");
}
	
static void cmdNoreply(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	rq->data.noreply = 1;
	printf("NOREPLY\n");
}

static void cmdClosing(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	rq->data.op = RQ_CMD_CLOSING;
	printf("CLOSING\n");
}
	
static void cmdServerFull(void *ptr)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	rq->data.op = RQ_CMD_SERVER_FULL;
	printf("SERVERFULL\n");
}
	
static void cmdID(void *ptr, risp_int_t value)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	assert(value > 0 && value <= 0xffff);
	rq->data.id = value;
	printf("ID (%d)", rq->data.id);
}
	
static void cmdQueueID(void *ptr, risp_int_t value)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	assert(value > 0 && value <= 0xffff);
	rq->data.qid = value;
	printf("QUEUEID (%d)\n", value);
}
	
static void cmdTimeout(void *ptr, risp_int_t value)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	assert(value > 0 && value <= 0xffff);
	rq->data.timeout = value;
	printf("TIMEOUT (%d)\n", value);

}
	
static void cmdPriority(void *ptr, risp_int_t value)
{
	rq_t *rq;

	assert(ptr != NULL);
	rq = (rq_t *) ptr;
	assert(rq != NULL);

	assert(value > 0 && value <= 0xffff);
	rq->data.priority = value;
	printf("PRIORITY (%d)\n", value);
}
	
static void cmdPayload(void *ptr, risp_length_t length, risp_char_t *data)
{
	rq_t *rq = (rq_t *) ptr;
 	assert(rq != NULL);
 	assert(length > 0);
 	assert(data != NULL);
 	expbuf_set(&rq->data.payload, data, length);
// 	printf("node:%d PAYLOAD (len:%d)\n", ptr->handle, length);
	printf("PAYLOAD (len:%d)\n", length);

}


static void cmdQueue(void *ptr, risp_length_t length, risp_char_t *data)
{
	rq_t *rq = (rq_t *) ptr;
 	assert(rq != NULL);
 	assert(length > 0);
 	assert(data != NULL);
 	expbuf_set(&rq->data.queue, data, length);
	printf("QUEUE (%s)\n", expbuf_string(&rq->data.queue));

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



//-----------------------------------------------------------------------------
// This function is the main event loop.  Once control is passed here, it will
// remain until a signal is received to terminate.  Therefore, everything
// needs to be setup and initialised before this point.  The Process loop will
// basically do a little checking, and will then call the libevent main loop.
void rq_process(rq_t *rq)
{
	assert(rq != NULL);

	// setup the risp processor.
	assert(rq->risp == NULL);
	rq->risp = risp_init();
	assert(rq->risp != NULL);
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

	// setup the event handler.
	assert(rq->evbase != NULL);
	assert(rq->handle != INVALID_HANDLE && rq->handle > 0);
	event_set(&rq->event, rq->handle, (EV_READ | EV_PERSIST), rq_process_handler, (void *)rq);
  event_base_set(rq->evbase, &rq->event);
  event_add(&rq->event, NULL);
	
	// start the event main loop.
	event_base_loop(rq->evbase, 0);

	// if we have broken out of this loop, we need to start shutting things down.  We want the loop to continue until all of the nodes have successfully closed.  We can probably manually process everything at this point.

	assert(0);
	
	// ** send message to all nodes telling them that we are shutting down (RQ_CMD_CLOSING).
	// ** Update the timeout of every pending request.

	// ** keep looping in a semi-controlled manner (manually process each node every second).  In other words, we stimulate a loop, but dont actually call event_base_loop().

	assert(rq->risp != NULL);
	risp_shutdown(rq->risp);
	free(rq->risp);
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
	msg->broadcast = 0;
	msg->noreply = 0;

	msg->request.data = NULL;
	msg->request.length = 0;
	msg->reply.data = NULL;
	msg->reply.length = 0;
	
	msg->arg = NULL;
}

//-----------------------------------------------------------------------------
// 
void rq_message_clear(rq_message_t *msg)
{
	assert(msg != NULL);
// 	assert(msg->request.data == NULL && msg->request.length == 0);
// 	assert(msg->reply.data == NULL && msg->reply.length == 0);


	// not sure what is the best here.  will have to wait until I write the code to implement message sending to see what is appropriate.
	assert(0);
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
	assert(msg->broadcast == 0);

	msg->broadcast = 1;
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

	assert(msg->request.length == 0);
	assert(msg->request.data == NULL);
	assert(msg->reply.length == 0);
	assert(msg->reply.data == NULL);
	
	msg->request.length = length;
	msg->request.data = data;
}

// send a message to the controller.
void rq_send(rq_t *rq, rq_message_t *msg)
{
	assert(rq != NULL);
	assert(msg != NULL);

	assert(msg->type == 0);
	msg->type = RQ_TYPE_REQUEST;

	assert(msg->request.length > 0);
	assert(msg->request.data != NULL);

	assert(msg->reply.length == 0);
	assert(msg->reply.data == NULL);

	// if there is no data in the 'out' queue, then attempt to send the request to the socket.  Any that we are unable to send, should be added to teh out queue, and the WRITE event should be established.

	assert(0);
}


