## make file for rq-http.

all: rq-http

ARGS=-Wall -O2
LIBS=-lrispbuf -lrisp -levent -lexpbufpool -lmempool -lexpbuf -lrq -llinklist -lrq-http -lsqlite3
OBJS=rq-http.o \
      config.o \
      server.o settings.o signals.o


H_rq=/usr/include/rq.h
H_rq_http=/usr/include/rq-http.h
H_linklist=/usr/include/linklist.h

H_settings=settings.h $(H_linklist)
H_server=server.h $(H_rq) $(H_settings)
H_config=config.h
H_master_data=master-data.h $(H_settings) $(H_config)
H_signals=signals.h

rq-http: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)


rq-http.o: rq-http.c $(H_rq) $(H_rq_http) $(H_server) $(H_settings) $(H_master_data) $(H_signals) $(H_config)
	gcc -c -o $@ rq-http.c $(ARGS)

config.o: config.c $(H_config)
	gcc -c -o $@ config.c $(ARGS)

server.o: server.c $(H_server)
	gcc -c -o $@ server.c $(ARGS)

settings.o: settings.c $(H_settings) $(H_rq)
	gcc -c -o $@ settings.c $(ARGS)

signals.o: signals.c $(H_signals) $(H_rq) $(H_master_data)
	gcc -c -o $@ signals.c $(ARGS)



install: rq-http
	@cp rq-http /usr/bin

clean:
	@-rm rq-http
	@-rm $(OBJS)
