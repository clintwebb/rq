## make file for rq-http-static.

all: rq-http-static

#DEBUG_LIBS=-lefence -lpthread
DEBUG_LIBS=


ARGS=-Wall -O2 -g
RQ_LIBS=-lrispbuf -lrisp -levent_core -lexpbufpool -lmempool -lexpbuf -lrq -llinklist
LIBS=$(RQ_LIBS) -lrq-http -lsqlite3 $(DEBUG_LIBS)
OBJS=rq-http-static.o


H_rq=/usr/include/rq.h
H_rq_http_config=/usr/include/rq-http.h
H_linklist=/usr/include/linklist.h
H_expbuf=/usr/include/expbuf.h

rq-http-static: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)


rq-http-static.o: rq-http-static.c $(H_rq) $(H_rq_http) $(H_linklist)
	gcc -c -o $@ rq-http-static.c $(ARGS)


install: rq-http-static
	@cp rq-http-static /usr/bin

clean:
	@-rm rq-http-static
	@-rm $(OBJS)
