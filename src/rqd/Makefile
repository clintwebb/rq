all: rqd

ARGS=-g -Wall -O2


H_settings=settings.h
H_stats=stats.h
H_data=data.h
H_system_data=system_data.h /usr/include/rq.h
H_message=message.h $(H_system_data)
H_node=node.h $(H_message) $(H_data) $(H_system_data)
H_queue=queue.h $(H_node) $(H_message) $(H_system_data)
H_server=server.h $(H_node) $(H_stats) $(H_queue) $(H_system_data)
H_commands=commands.h
H_process=process.h
H_send=send.h $(H_node) $(H_message)
H_actions=actions.h 



OBJS=rqd.o daemon.o settings.o stats.o data.o server.o node.o queue.o commands.o process.o message.o send.o actions.o

# DEBUG_LIBS=-lefence
DEBUG_LIBS=

LIBS=-lrisp -levent -lexpbuf -lexpbufpool -lrispbuf -lrq -levactions -lmempool $(DEBUG_LIBS)

rqd: $(OBJS)
	gcc -o $@ $(OBJS) $(LIBS) $(ARGS)

actions.o: actions.c $(H_actions) $(H_server)
	gcc -c -o $@ actions.c $(ARGS)

commands.o: commands.c $(H_commands) $(H_node) $(H_process)
	gcc -c -o $@ commands.c $(ARGS)

data.o: data.c $(H_data)
	gcc -c -o $@ data.c $(ARGS)

daemon.o: daemon.c daemon.h
	gcc -c -o $@ daemon.c $(ARGS)

message.o: message.c $(H_message) $(H_queue) $(H_actions)
	gcc -c -o $@ message.c $(ARGS)

node.o: node.c $(H_node) $(H_data) $(H_stats) $(H_queue) $(H_actions)
	gcc -c -o $@ node.c $(ARGS)

process.o: process.c $(H_process) $(H_send) $(H_queue) $(H_message) $(H_actions)
	gcc -c -o $@ process.c $(ARGS)

queue.o: queue.c $(H_queue) $(H_server) $(H_actions) $(H_send)
	gcc -c -o $@ queue.c $(ARGS)

rqd.o: rqd.c daemon.h $(H_actions) $(H_commands) $(H_server) $(H_settings) $(H_system_data)
	gcc -c -o $@ rqd.c $(ARGS)

send.o: send.c $(H_send) $(H_queue)
	gcc -c -o $@ send.c $(ARGS)

server.o: server.c $(H_server) $(H_commands) $(H_queue) $(H_settings)
	gcc -c -o $@ server.c $(ARGS)

settings.o: settings.c $(H_settings)
	gcc -c -o $@ settings.c $(ARGS)

stats.o: stats.c $(H_stats)
	gcc -c -o $@ stats.c $(ARGS)



clean:
	@-rm rqd
	@-rm *.o

