## make file for rq-http.

all: rq-http

ARGS=-Wall -O2
OBJS=rq-http.o server.o settings.o
LIBS=-lrispbuf -lrisp -levent -lexpbuf -lrq -llinklist -lrq-http

H_rq=/usr/include/rq-http.h

H_settings=settings.h
H_server=server.h $(H_rq) $(H_settings)


rq-http: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)


rq-http.o: rq-http.c $(H_rq) $(H_server) $(H_settings)
	gcc -c -o $@ rq-http.c $(ARGS)

server.o: server.c $(H_server) 
	gcc -c -o $@ server.c $(ARGS)

settings.o: settings.c $(H_settings) $(H_rq)
	gcc -c -o $@ settings.c $(ARGS)




clean:
	@-rm rq-http
	@-rm $(OBJS)
