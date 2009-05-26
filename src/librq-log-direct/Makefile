## make file for librq-log-direct.

all: librq-log-direct.so.1.0.1

ARGS=-Wall -O2

# Need to be able to make 'man-pages' as well.  Not sure where to get the source for those... 

librq-log-direct.o: librq-log-direct.c rq-log-direct.h /usr/include/rq.h
	gcc -c -fPIC librq-log-direct.c -o $@ $(ARGS)

librq-log-direct.a: librq-log-direct.o
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

librq-log-direct.so.1.0.1: librq-log-direct.o
	gcc -shared -Wl,-soname,librq-log-direct.so.1 -o librq-log-direct.so.1.0.1 librq-log-direct.o
	

install: librq-log-direct.so.1.0.1 rq-log-direct.h
	@-test -e /usr/include/rq-log-direct.h && rm /usr/include/rq-log-direct.h
	cp rq-log-direct.h /usr/include/
	cp librq-log-direct.so.1.0.1 /usr/lib/
	@-test -e /usr/lib/librq-log-direct.so && rm /usr/lib/librq-log-direct.so
	ln -s /usr/lib/librq-log-direct.so.1.0.1 /usr/lib/librq-log-direct.so
	ldconfig
	@echo "Install complete."




clean:
	@-[ -e librq-log-direct.o ] && rm librq-log-direct.o
	@-[ -e librq-log-direct.so* ] && rm librq-log-direct.so*
	@-rm *.o

