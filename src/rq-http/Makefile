## make file for rq-http.

all: rq-http

ARGS=-Wall -O2
LIBS=-lrispbuf -lrisp -levent_core -lexpbufpool -lmempool -lexpbuf -lrq -llinklist -lrq-http -lsqlite3
OBJS=rq-http.o


H_rq=/usr/include/rq.h
H_rq_http=/usr/include/rq-http.h
H_linklist=/usr/include/linklist.h


rq-http: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)


rq-http.o: rq-http.c $(H_rq) $(H_rq_http) 
	gcc -c -o $@ rq-http.c $(ARGS)



install: rq-http
	@cp rq-http /usr/bin

clean:
	@-rm rq-http
	@-rm $(OBJS)

