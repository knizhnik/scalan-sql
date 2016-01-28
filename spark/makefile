all: libnativerdd.so NativeTest.jar

testjni: libnative.so TestJNI.jar

libnativerdd.so: nativerdd.cpp
	g++ -shared -fPIC -g -O3 -I/usr/lib/jvm/java/include/ -I/usr/lib/jvm/java/include/linux -I/usr/java/default/include -I/usr/java/default/include/linux -I/usr/java/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -o libnativerdd.so nativerdd.cpp

NativeTest.jar: NativeTest.scala
	scalac -classpath ../spark-1.4.0-bin-hadoop2.6/lib/spark-assembly-1.4.0-hadoop2.6.0.jar:../spark-1.4.0-bin-hadoop2.6/lib/spark-1.4.0-yarn-shuffle.jar NativeTest.scala
	jar cvf NativeTest.jar *.class

run:
	../spark-1.3.0-bin-hadoop2.4/bin/spark-submit --master local[8] --executor-cores 8 --class NativeTest  NativeTest.jar

clean:
	rm -f *.jar *.o *.so *.class

libnative.so: native.cpp
	g++ -shared -fPIC -g -O0 -I/usr/lib/jvm/java/include/ -I/usr/lib/jvm/java/include/linux -I/usr/java/default/include -I ~/.nix-profile/include -I/usr/java/default/include/linux -I/usr/java/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -o libnative.so native.cpp

TestJNI.jar: TestJNI.java
	rm -f *.class
	javac TestJNI.java
	jar cvf TestJNI.jar *.class


