## make file for rq-blacklist.

all: rq-blacklist

#DEBUG_LIBS=-lefence -lpthread
DEBUG_LIBS=


ARGS=-Wall -O2 -g
LIBS=-lrispbuf -lrisp -levent_core -lexpbufpool -lmempool -lexpbuf -lrq -llinklist -lrq-blacklist -lcsv $(DEBUG_LIBS)
OBJS=rq-blacklist.o


H_rq=/usr/include/rq.h
H_rq_blacklist=/usr/include/rq-blacklist.h
H_linklist=/usr/include/linklist.h
H_expbuf=/usr/include/expbuf.h

rq-blacklist: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)


rq-blacklist.o: rq-blacklist.c $(H_rq) $(H_rq_blacklist) $(H_linklist)
	gcc -c -o $@ rq-blacklist.c $(ARGS)


install: rq-blacklist
	@cp rq-blacklist /usr/bin

clean:
	@-rm rq-blacklist
	@-rm $(OBJS)
