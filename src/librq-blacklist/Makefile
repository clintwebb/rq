## make file for librq-blacklist.

all: librq-blacklist.so.1.0.1

ARGS=-Wall -O2

# Need to be able to make 'man-pages' as well.  Not sure where to get the source for those... 

librq-blacklist.o: librq-blacklist.c rq-blacklist.h /usr/include/rq.h
	gcc -c -fPIC librq-blacklist.c -o $@ $(ARGS)

librq-blacklist.a: librq-blacklist.o
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

librq-blacklist.so.1.0.1: librq-blacklist.o
	gcc -shared -Wl,-soname,librq-blacklist.so.1 -o librq-blacklist.so.1.0.1 librq-blacklist.o
	

install: librq-blacklist.so.1.0.1 rq-blacklist.h
	@-test -e /usr/include/rq-blacklist.h && rm /usr/include/rq-blacklist.h
	cp rq-blacklist.h /usr/include/
	cp librq-blacklist.so.1.0.1 /usr/lib/
	@-test -e /usr/lib/librq-blacklist.so && rm /usr/lib/librq-blacklist.so
	ln -s /usr/lib/librq-blacklist.so.1.0.1 /usr/lib/librq-blacklist.so
	ldconfig
	@echo "Install complete."




clean:
	@-[ -e librq-blacklist.o ] && rm librq-blacklist.o
	@-[ -e librq-blacklist.so* ] && rm librq-blacklist.so*
	@-rm *.o

