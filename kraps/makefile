all: tpch csv2rdd

SIMDCOMP_HOME=~/simdcomp

#CPP=g++ -pthread -fPIC -O3 -g -Wall
CPP=g++ -pthread -fPIC -O3 -g -Wall -DUSE_ZLIB
#CPP=g++ -pthread -fPIC -O3 -g -Wall -DUSE_LZH
#CPP=g++ -pthread -fPIC -O0 -g -Wall
#CPP=g++ -pthread -fPIC -O3 -g -Wall -DUSE_SIMDCOMP -I $(SIMDCOMP_HOME)/include

JNI_INCLUDES = -I/usr/lib/jvm/java/include/ -I/usr/lib/jvm/java/include/linux -I/usr/java/default/include -I/usr/java/default/include/linux -I/usr/java/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I/usr/lib/jvm/java-7-openjdk-amd64/include 

tpch.o: tpch.cpp rdd.h cluster.h tpch.h sync.h sockio.h
	$(CPP) -c tpch.cpp

cluster.o: cluster.cpp cluster.h sync.h sockio.h
	$(CPP) -c cluster.cpp

sockio.o: sockio.cpp sockio.h
	$(CPP) -c sockio.cpp

compress.o: compress.cpp compress.h
	$(CPP) -c compress.cpp

tpch: tpch.o cluster.o sockio.o compress.o
#	$(CPP) -o tpch tpch.o cluster.o sockio.o compress.o
	$(CPP) -o tpch tpch.o cluster.o sockio.o compress.o -lz
#	$(CPP) -o tpch tpch.o cluster.o sockio.o compress.o -L $(SIMDCOMP_HOME) -lsimdcomp 


libq1rdd.so: Q1_RDD.cpp cluster.o sockio.o compress.o
	$(CPP) -shared $(JNI_INCLUDES) -o libq1rdd.so Q1_RDD.cpp cluster.o sockio.o compress.o


csv2rdd: csv2rdd.cpp tpch.h
	$(CPP) -o csv2rdd csv2rdd.cpp

clean:
	rm -f *.o tpch csv2rdd

tgz: clean 
	cd .. ; tar cvzf kraps.tgz kraps
