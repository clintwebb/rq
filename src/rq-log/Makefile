## make file for librq-log.

all: librq-log.so.1.0.1 rq-log 

ARGS=-Wall -O2

# Need to be able to make 'man-pages' as well.  Not sure where to get the source for those... 

librq-log.o: librq-log.c rq-log.h
	gcc -c -fPIC librq-log.c -o $@ $(ARGS)

librq-log.a: librq-log.o
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

librq-log.so.1.0.1: librq-log.o
	gcc -shared -Wl,-soname,librq-log.so.1 -o librq-log.so.1.0.1 librq-log.o
	

install: librq-log.so.1.0.1 rq-log.h
	@-test -e /usr/include/rq-log.h && rm /usr/include/rq-log.h
	cp rq-log.h /usr/include/
	cp librq-log.so.1.0.1 /usr/lib/
	@-test -e /usr/lib/librq-log.so && rm /usr/lib/librq-log.so
	ln -s /usr/lib/librq-log.so.1.0.1 /usr/lib/librq-log.so
	ldconfig
	@echo "Install complete."



rq-log: rq-log.c 
	gcc -o $@ rq-log.c -lrispbuf -lrisp -levent -lexpbuf -lrq $(ARGS)

log-test: log-test.c 
	gcc -o $@ log-test.c -lrq-log -lrispbuf -lrisp -lexpbuf -lrq -levent $(ARGS)



clean:
	@-[ -e librq-log.o ] && rm librq-log.o
	@-[ -e librq-log.so* ] && rm librq-log.so*
	@-rm rq-log
	@-rm log-test
	@-rm *.o
