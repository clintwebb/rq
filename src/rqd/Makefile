all: rqd

ARGS=-g -Wall -O2
# -fprofile-arcs -ftest-coverage 
# -pg


HH_rq=/usr/include/rq.h
HH_linklist=/usr/include/linklist.h

H_controllers=controllers.h $(HH_linklist)
H_settings=settings.h $(HH_linklist)
H_logging=logging.h
H_system_data=system_data.h $(HH_rq) $(H_logging)
H_stats=stats.h $(H_system_data)
H_data=data.h
H_message=message.h $(H_system_data)
H_node=node.h $(H_data) $(H_system_data) $(H_message)
H_queue=queue.h $(H_node) $(H_message) $(H_system_data)
H_server=server.h $(H_system_data)
H_commands=commands.h
H_process=process.h
H_send=send.h $(H_node) $(H_message)
H_signals=signals.h


OBJS=rqd.o daemon.o settings.o \
     stats.o data.o server.o   \
     node.o queue.o commands.o \
     process.o message.o send.o \
     signals.o controllers.o \
     logging.o

DEBUG_LIBS=-lefence -lpthread
#DEBUG_LIBS=

LIBS=-lrisp -levent -lexpbuf -lexpbufpool -lrispbuf -lrq -lmempool -llinklist $(DEBUG_LIBS)

rqd: $(OBJS)
	gcc -o $@ $(ARGS) $(OBJS) $(LIBS) 

actions.o: actions.c $(H_actions) $(H_controllers) $(H_server) $(H_stats) $(H_queue)
	gcc -c -o $@ $(ARGS) actions.c 

commands.o: commands.c $(H_commands) $(H_node) $(H_process)  $(H_send)
	gcc -c -o $@ $(ARGS) commands.c

controllers.o: controllers.c $(H_controllers) $(H_node) $(H_queue) $(H_send) $(H_server) $(H_system_data) $(HH_rq)
	gcc -c -o $@ $(ARGS) controllers.c

data.o: data.c $(H_data)
	gcc -c -o $@ $(ARGS) data.c

daemon.o: daemon.c daemon.h
	gcc -c -o $@ $(ARGS) daemon.c

logging.o: logging.c $(H_logging)
	gcc -c -o $@ $(ARGS) logging.c

message.o: message.c $(H_message) $(H_queue)
	gcc -c -o $@ $(ARGS) message.c

node.o: node.c $(H_node) $(H_data) $(H_stats) $(H_queue) $(H_server) $(H_send)
	gcc -c -o $@ $(ARGS) node.c

process.o: process.c $(H_process) $(H_send) $(H_queue) $(H_message) $(H_stats)
	gcc -c -o $@ $(ARGS) process.c

queue.o: queue.c $(H_queue) $(H_server) $(H_send)
	gcc -c -o $@ $(ARGS) queue.c

rqd.o: rqd.c daemon.h $(H_commands) $(H_server) $(H_settings) $(H_stats) $(H_system_data) $(H_signals) $(H_queue) $(H_controllers)
	gcc -c -o $@ $(ARGS) rqd.c

send.o: send.c $(H_send) $(H_queue)
	gcc -c -o $@ $(ARGS) send.c

server.o: server.c $(H_server) $(H_commands) $(H_queue) $(H_settings)
	gcc -c -o $@ $(ARGS) server.c

settings.o: settings.c $(H_settings) $(HH_rq)
	gcc -c -o $@ $(ARGS) settings.c

signals.o: signals.c $(H_signals) $(H_system_data) $(H_server) $(H_stats) $(H_queue)
	gcc -c -o $@ $(ARGS) signals.c

stats.o: stats.c $(H_stats) $(H_server) $(H_queue)
	gcc -c -o $@ $(ARGS) stats.c



clean:
	@-rm rqd
	@-rm *.o
	@-rm *.gcno
	@-rm gmon.out

