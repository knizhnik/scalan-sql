all: tpch csv2rdd
CPP=g++ -pthread -O3 -Wall

tpch.o: tpch.cpp rdd.h cluster.h tpch.h sync.h sockio.h
	$(CPP) -c tpch.cpp

cluster.o: cluster.cpp cluster.h sync.h sockio.h
	$(CPP) -c cluster.cpp

sockio.o: sockio.cpp sockio.h
	$(CPP) -c sockio.cpp

tpch: tpch.o cluster.o sockio.o
	$(CPP) -o tpch tpch.o cluster.o sockio.o

csv2rdd: csv2rdd.cpp tpch.h
	$(CPP) -o csv2rdd csv2rdd.cpp

clean:
	rm *.o 

