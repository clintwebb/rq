## make file for librq-http.

all: librq-http.so.1.0.1

ARGS=-Wall -O2

# Need to be able to make 'man-pages' as well.  Not sure where to get the source for those... 

librq-http.o: librq-http.c rq-http.h /usr/include/rq.h
	gcc -c -fPIC librq-http.c -o $@ $(ARGS)

librq-http.a: librq-http.o
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

librq-http.so.1.0.1: librq-http.o
	gcc -shared -Wl,-soname,librq-http.so.1 -o librq-http.so.1.0.1 librq-http.o
	

install: librq-http.so.1.0.1 rq-http.h
	@-test -e /usr/include/rq-http.h && rm /usr/include/rq-http.h
	cp rq-http.h /usr/include/
	cp librq-http.so.1.0.1 /usr/lib/
	@-test -e /usr/lib/librq-http.so && rm /usr/lib/librq-http.so
	ln -s /usr/lib/librq-http.so.1.0.1 /usr/lib/librq-http.so
	ldconfig
	@echo "Install complete."



clean:
	@-[ -e librq-http.o ] && rm librq-http.o
	@-[ -e librq-http.so* ] && rm librq-http.so*
	@-rm *.o
