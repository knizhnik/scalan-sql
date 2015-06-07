all: libnativerdd.so NativeTest.jar

libnativerdd.so: nativerdd.cpp
	g++ -shared -fPIC -g -O0 -I/usr/lib/jvm/java/include/ -I/usr/lib/jvm/java/include/linux -I/usr/java/default/include -I/usr/java/default/include/linux -I/usr/java/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -o libnativerdd.so nativerdd.cpp

NativeTest.jar: NativeTest.scala
	scalac -classpath ../spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3.0-hadoop2.4.0.jar:../spark-1.3.0-bin-hadoop2.4/lib/spark-1.3.0-yarn-shuffle.jar NativeTest.scala
	jar cvf NativeTest.jar *.class

run:
	../spark-1.3.0-bin-hadoop2.4/bin/spark-submit --master local[16] --driver-memory 16G --executor-cores 16 --num-executors 1  --executor-memory 64H --class NativeTest  NativeTest.jar 2> spark.log


