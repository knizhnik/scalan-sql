all: libnativerdd.so NativeTest.jar

engine: JavaEngine.jar libcppengine.so

libnativerdd.so: nativerdd.cpp
	g++ -shared -fPIC -g -O3 -I/usr/lib/jvm/java/include/ -I/usr/lib/jvm/java/include/linux -I/usr/java/default/include -I/usr/java/default/include/linux -I/usr/java/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -o libnativerdd.so nativerdd.cpp

NativeTest.jar: NativeTest.scala
	scalac -classpath ../spark-1.4.0-bin-hadoop2.6/lib/spark-assembly-1.4.0-hadoop2.6.0.jar:../spark-1.4.0-bin-hadoop2.6/lib/spark-1.4.0-yarn-shuffle.jar NativeTest.scala
	jar cvf NativeTest.jar *.class

run:
	../spark-1.3.0-bin-hadoop2.4/bin/spark-submit --master local[8] --executor-cores 8 --class NativeTest  NativeTest.jar


JavaEngine.jar: JavaEngine.java
	rm -f *.class
	javac JavaEngine.java
	jar cvf JavaEngine.jar *.class

libcppengine.so: CppEngine.cpp
	g++ -shared -fPIC -g -O3 -I/usr/lib/jvm/java/include/ -I/usr/lib/jvm/java/include/linux -I/usr/java/default/include -I/usr/java/default/include/linux -I/usr/java/include -I ~/.nix-profile/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -o libcppengine.so CppEngine.cpp

