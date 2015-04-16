all: tpch csv2rdd

tpch: tpch.cpp rdd.h tpch.h
	g++ -O3 -o tpch tpch.cpp

csv2rdd: csv2rdd.cpp tpch.h
	g++ -O3 -o csv2rdd csv2rdd.cpp
