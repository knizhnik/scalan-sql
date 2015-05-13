all: tpch csv2rdd

SIMDCOMP_HOME=~/simdcomp

CPP=g++ -pthread -O3 -g -Wall -I $(SIMDCOMP_HOME)/include -DUSE_ZLIB
#CPP=g++ -pthread -O3 -g -Wall -I $(SIMDCOMP_HOME)/include
#CPP=g++ -pthread -O0 -g -Wall -I $(SIMDCOMP_HOME)/include -DUSE_ZLIB


tpch.o: tpch.cpp rdd.h cluster.h tpch.h sync.h sockio.h
	$(CPP) -c tpch.cpp

cluster.o: cluster.cpp cluster.h sync.h sockio.h
	$(CPP) -c cluster.cpp

sockio.o: sockio.cpp sockio.h
	$(CPP) -c sockio.cpp

tpch: tpch.o cluster.o sockio.o
	$(CPP) -o tpch tpch.o cluster.o sockio.o -L $(SIMDCOMP_HOME) -lsimdcomp -lz

csv2rdd: csv2rdd.cpp tpch.h
	$(CPP) -o csv2rdd csv2rdd.cpp

clean:
	rm -f *.o tpch csv2rdd

tgz: clean 
	cd .. ; tar cvzf kraps.tgz kraps
