all: tpch csv2rdd
rdd: q1rdd.jar libq1rdd.so

SIMDCOMP_HOME=~/simdcomp
PARQUET_CPP_HOME=~/parquet++
HADOOP_HOME=~/hadoop-2.7.0
SPARK_HOME=/srv/remote/all-common/spark-1.3.1-bin-hadoop2.6
SPARK_HOME=~/spark

# java home on Linux
#JAVA_HOME=/usr/lib/jvm/java

# java home on Mac OSX
JAVA_HOME?=/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home

#CPP=g++ -pthread -fPIC -O3 -g -Wall -DSHOW_HASH_STATISTIC #-DUSE_MESSAGE_HANDLER -DSHOW_HASH_STATISTIC
#CPP=g++ -pthread -fPIC -O3 -g -Wall
#CPP=g++ -pthread -fPIC -O3 -g -Wall -DUSE_ZLIB -I $(HOME)/.nix-profile/include/
#CPP=g++ -pthread -fPIC -O3 -g -Wall -DUSE_LZH
#CPP=g++ -pthread -fPIC -O0 -g -Wall
#CPP=g++ -pthread -fPIC -O3 -g -Wall -DUSE_SIMDCOMP -I $(SIMDCOMP_HOME)/include
#CPP=g++ -pthread -fPIC -O3 -g -Wall -DUSE_ZLIB
CPP=g++ -std=c++11 -pthread -fPIC -O0 -g -Wall -Wno-unused-value -Wno-unused-variable -Wno-sign-compare -DUSE_ZLIB -DUSE_PARQUET -I $(PARQUET_CPP_HOME)/src -I $(PARQUET_CPP_HOME)/generated -I $(HADOOP_HOME)/include 

JNI_INCLUDES = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/include/darwin -I/usr/java/default/include -I/usr/java/default/include/linux -I/usr/java/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I/usr/lib/jvm/oracle-jdk-bin-1.8/include -I/usr/lib/jvm/jdk1.8.0_60/include -I/usr/lib/jvm/jdk1.8.0_60/include/linux

tpch.o: tpch.cpp rdd.h cluster.h tpch.h sync.h sockio.h
	$(CPP) -c tpch.cpp

cluster.o: cluster.cpp cluster.h sync.h sockio.h
	$(CPP) -c cluster.cpp

sockio.o: sockio.cpp sockio.h
	$(CPP) -c sockio.cpp

compress.o: compress.cpp compress.h
	$(CPP) -c compress.cpp

parquet.o: parquet.cpp rdd.h cluster.h
	$(CPP) -c parquet.cpp

tpch: tpch.o cluster.o sockio.o compress.o parquet.o 
#	$(CPP) -static -o tpch tpch.o cluster.o sockio.o compress.o -L$(HOME)/.nix-profile/lib -lz
#	$(CPP) -o tpch tpch.o cluster.o sockio.o compress.o -lz
#	$(CPP) -o tpch tpch.o cluster.o sockio.o compress.o -L $(SIMDCOMP_HOME) -lsimdcomp 
	$(CPP) -o tpch tpch.o cluster.o sockio.o compress.o  parquet.o $(PARQUET_CPP_HOME)/build/libParquet.a $(PARQUET_CPP_HOME)/build/libParquetCompression.a $(PARQUET_CPP_HOME)/build/libThriftParquet.a $(PARQUET_CPP_HOME)/thirdparty/installed/lib/libsnappy.a  $(PARQUET_CPP_HOME)/thirdparty/installed/lib/liblz4.a /usr/local/lib/libthrift.a $(HADOOP_HOME)/lib/native/libhdfs.a -L/usr/lib/jvm/oracle-jdk-bin-1.8/jre/lib/amd64/server -lz -ljvm

kraps: libkraps.so kraps.jar libkrapsrdd.so
q1: libq1rdd.so q1rdd.jar

libq1rdd.so: Q1_RDD.cpp cluster.o sockio.o compress.o 
	$(CPP) -shared $(JNI_INCLUDES) -o libq1rdd.so Q1_RDD.cpp cluster.o sockio.o compress.o -lz

q1rdd.jar: Q1_RDD.scala
	rm -f *.class
	scalac -classpath $(SPARK_HOME)/lib/spark-assembly-1.3.1-hadoop2.6.0.jar:$(SPARK_HOME)/lib/spark-1.3.1-yarn-shuffle.jar Q1_RDD.scala
	jar cvf q1rdd.jar *.class

libkraps.so: cluster.o sockio.o compress.o 
	$(CPP) -shared -o libkraps.so cluster.o sockio.o compress.o -L$(HOME)/.nix-profile/lib -lz

libkraps.dylib: cluster.o sockio.o compress.o 
	$(CPP) -shared -o libkraps.dylib cluster.o sockio.o compress.o -lz

libkrapsrdd.so: KrapsRDD.cpp
	$(CPP) -shared $(JNI_INCLUDES) -o libkrapsrdd.so KrapsRDD.cpp  -L. -lkraps

libkrapsrdd.dylib: KrapsRDD.cpp
	$(CPP) -shared $(JNI_INCLUDES) -o libkrapsrdd.dylib KrapsRDD.cpp  -L. -lkraps

kraps.jar: KrapsRDD.scala
	rm -f *.class
	scalac -classpath $(SPARK_HOME)/lib/spark-assembly-1.3.1-hadoop2.6.0.jar:$(SPARK_HOME)/lib/spark-1.3.1-yarn-shuffle.jar KrapsRDD.scala
	jar cvf kraps.jar *.class


csv2rdd: csv2rdd.cpp tpch.h
	$(CPP) -o csv2rdd csv2rdd.cpp

clean:
	rm -f *.o tpch csv2rdd

tgz: clean 
	cd .. ; tar cvzf kraps.tgz kraps


doc:
	doxygen doxygen.cfg
