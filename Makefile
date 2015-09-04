all: *.c *.h
	gcc -std=c99 -O3  -Wall -pthread -D_GNU_SOURCE  *.c -o loader -lm -levent

clean:
	rm loader
