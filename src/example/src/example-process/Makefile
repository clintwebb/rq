## make file for rq-http-static.

all: example-process

#DEBUG_LIBS=-lefence -lpthread
DEBUG_LIBS=


ARGS=-Wall -O2 -g
RQ_LIBS=-lrispbuf -lrisp -levent_core -lexpbufpool -lmempool -lexpbuf -lrq -llinklist
LIBS=$(RQ_LIBS) -lrq-http -lsqlite3 $(DEBUG_LIBS)
OBJS=example-process.o


H_rq=/usr/include/rq.h
H_rq_http=/usr/include/rq-http.h
H_linklist=/usr/include/linklist.h
H_expbuf=/usr/include/expbuf.h

example-process: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)


example-process.o: example-process.c $(H_rq) $(H_rq_http) $(H_linklist)
	gcc -c -o $@ example-process.c $(ARGS)


install: example-process
	@cp example-process /usr/bin

clean:
	@-rm example-process
	@-rm $(OBJS)
