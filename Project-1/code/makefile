CC = g++ -g -O0 -Wno-deprecated

tag = -i

ifdef linux
	tag = -n
endif

# main.out:	Schema.o Catalog.o main.o TwoWayList.o
# 	$(CC) -o main.out main.o Catalog.o Schema.o TwoWayList.o -lsqlite3
	
# main.o:	main.cc
# 	$(CC) -c main.cc

main.out:	Schema.o Catalog.o main-2.o TwoWayList.o
	$(CC) -o main.out main-2.o Catalog.o Schema.o TwoWayList.o -lsqlite3
	
main.o:	main-2.cc
	$(CC) -c main-2.cc

Catalog.o: Catalog.h Catalog.cc	TwoWayList.h TwoWayList.cc
	$(CC) -c Catalog.cc
	
Schema.o: Config.h Schema.h Schema.cc	
	$(CC) -c Schema.cc
TwoWayList.o: Swap.h Swapify.h Swapify.cc Keyify.h Keyify.cc TwoWayList.cc TwoWayList.h
	$(CC) -c TwoWayList.cc
clean: 
	rm -f *.o
	rm -f *.out
