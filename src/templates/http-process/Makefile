## make file for rq-http-static.

all: http-process

#DEBUG_LIBS=-lefence -lpthread
DEBUG_LIBS=


ARGS=-Wall -O2 -g
RQ_LIBS=-lrispbuf -lrisp -levent_core -lexpbufpool -lmempool -lexpbuf -lrq -llinklist
LIBS=$(RQ_LIBS) -lrq-http $(DEBUG_LIBS)
OBJS=http-process.o


H_rq=/usr/include/rq.h
H_rq_http=/usr/include/rq-http.h
H_linklist=/usr/include/linklist.h
H_expbuf=/usr/include/expbuf.h

http-process: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)


http-process.o: http-process.c $(H_rq) $(H_rq_http) $(H_linklist)
	gcc -c -o $@ http-process.c $(ARGS)


install: http-process
	@cp http-process /usr/bin

clean:
	@-rm http-process
	@-rm $(OBJS)
