## make file for rq-http-config.

all: rq-http-config

#DEBUG_LIBS=-lefence -lpthread
DEBUG_LIBS=


ARGS=-Wall -O2 -g
LIBS=-lrispbuf -lrisp -levent_core -lexpbufpool -lmempool -lexpbuf -lrq -llinklist -lrq-http-config -lsqlite3 $(DEBUG_LIBS)
OBJS=rq-http-config.o


H_rq=/usr/include/rq.h
H_rq_http_config=/usr/include/rq-http-config.h
H_linklist=/usr/include/linklist.h
H_expbuf=/usr/include/expbuf.h

rq-http-config: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)


rq-http-config.o: rq-http-config.c $(H_rq) $(H_rq_http_config) $(H_linklist)
	gcc -c -o $@ rq-http-config.c $(ARGS)


install: rq-http-config
	@cp rq-http-config /usr/bin

clean:
	@-rm rq-http-config
	@-rm $(OBJS)
