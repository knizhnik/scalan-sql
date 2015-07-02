SPARK_HOME=/srv/remote/all-common/spark-1.3.1-bin-hadoop2.6

all: libnativerdd.so NativeTest.jar CustomRDDTest.jar

libnativerdd.so: nativerdd.cpp
	g++ -shared -fPIC -g -O0 -I/usr/lib/jvm/java/include/ -I/usr/lib/jvm/java/include/linux -I/usr/java/default/include -I/usr/java/default/include/linux -I/usr/java/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -o libnativerdd.so nativerdd.cpp

NativeTest.jar: NativeTest.scala
	rm -f *.class
	scalac -classpath $(SPARK_HOME)/lib/spark-assembly-1.3.1-hadoop2.6.0.jar:$(SPARK_HOME)/lib/spark-1.3.1-yarn-shuffle.jar NativeTest.scala
	jar cvf NativeTest.jar *.class


CustomRDDTest.jar: CustomRDDTest.scala
	rm -f *.class
	scalac -classpath $(SPARK_HOME)/lib/spark-assembly-1.3.1-hadoop2.6.0.jar:$(SPARK_HOME)/lib/spark-1.3.1-yarn-shuffle.jar CustomRDDTest.scala
	jar cvf CustomRDDTest.jar *.class

run:
	$(SPARK_HOME)/bin/spark-submit --master local[16] --driver-memory 16G --executor-cores 16 --num-executors 1  --executor-memory 64H --class NativeTest  NativeTest.jar 2> spark.log


