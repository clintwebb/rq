## make file for librq.

all: librq.so.1.0.1

ARGS=
OBJS=librq.o

librq.o: librq.c rq.h 
	gcc -c -fPIC librq.c  -o $@ $(ARGS)


librq.a: $(OBJS)
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

librq.so.1.0.1: $(OBJS)
	gcc -shared -Wl,-soname,librq.so.1 -o librq.so.1.0.1 $(OBJS)
	

install: librq.so.1.0.1 rq.h
	@-test -e /usr/include/rq.h && rm /usr/include/rq.h
	cp rq.h /usr/include/
	cp librq.so.1.0.1 /usr/lib/
	@-test -e /usr/lib/librq.so && rm /usr/lib/librq.so
	ln -s /usr/lib/librq.so.1.0.1 /usr/lib/librq.so
	ldconfig
	@echo "Install complete."

clean:
	@-[ -e librq.o ] && rm librq.o
	@-[ -e librq.so* ] && rm librq.so*
