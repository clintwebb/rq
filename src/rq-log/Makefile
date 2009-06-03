## make file for rq-log.

all: rq-log 

ARGS=-Wall -O2
LIBS=-levlogging -lrispbuf -lrisp -levent -lexpbufpool -lmempool -lexpbuf -lrq -llinklist

# Need to be able to make 'man-pages' as well.  Not sure where to get the source for those... 


rq-log: rq-log.c /usr/include/rq-log.h /usr/lib/librq-log.so.1.0.1
	gcc -o $@ rq-log.c $(LIBS) $(ARGS)

log-test: log-test.c /usr/include/rq-log.h rq-log /usr/include/rq.h
	gcc -o $@ log-test.c -lrq-log -lrispbuf -lrisp -lexpbuf -lrq -llinklist -levent $(ARGS)

install: rq-log
	@cp rq-log /usr/bin

clean:
	@-rm rq-log
	@-rm log-test
	@-rm *.o
