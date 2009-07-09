## make file for librq-http-config.

all: librq-http-config.so.1.0.1

ARGS=-Wall -O2 -g

# Need to be able to make 'man-pages' as well.  Not sure where to get the source for those... 

librq-http-config.o: librq-http-config.c rq-http-config.h /usr/include/rq.h
	gcc -c -fPIC librq-http-config.c -o $@ $(ARGS)

librq-http-config.a: librq-http-config.o
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

librq-http-config.so.1.0.1: librq-http-config.o
	gcc -shared -Wl,-soname,librq-http-config.so.1 -o librq-http-config.so.1.0.1 librq-http-config.o
	

install: librq-http-config.so.1.0.1 rq-http-config.h
	@-test -e /usr/include/rq-http-config.h && rm /usr/include/rq-http-config.h
	cp rq-http-config.h /usr/include/
	cp librq-http-config.so.1.0.1 /usr/lib/
	@-test -e /usr/lib/librq-http-config.so && rm /usr/lib/librq-http-config.so
	ln -s /usr/lib/librq-http-config.so.1.0.1 /usr/lib/librq-http-config.so
	ldconfig
	@echo "Install complete."




clean:
	@-[ -e librq-http-config.o ] && rm librq-http-config.o
	@-[ -e librq-http-config.so* ] && rm librq-http-config.so*
	@-rm *.o

