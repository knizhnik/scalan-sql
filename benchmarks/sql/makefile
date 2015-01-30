all: sqliteBench sqlParBenchmark sqlSeqBenchmark sqlMapReduceBenchmark sqlAggJoinBenchmark MonetDbBench MonetDbJoinBench TPCH_Q1

sqliteBench: 
	gcc -O2 -o sqlite_bench sqlite_bench.c -lsqlite3
	./sqlite_bench

sqlParBenchmark: 
	scala -J-Xmx12G -J-Xms12G -optimize  sqlParBenchmark.scala

sqlColumnarStoreBenchmark: 
	scala -J-Xmx14G -J-Xms14G -optimize  sqlColumnarStoreBenchmark.scala

sqlSeqBenchmark: 
	scala -J-Xmx12G -J-Xms12G -optimize  sqlSeqBenchmark.scala

sqlMapReduceBenchmark: 
	scala -J-Xmx10G -J-Xms10G -optimize  sqlMapReduceBenchmark.scala

sqlAggJoinBenchmark: 
	scala -J-Xhprof -J-Xmx12G -J-Xms12G -optimize  sqlAggJoinBenchmark.scala

JoinBenchMonetDB:
	javac JoinBenchJDBC.java
	java -cp /usr/local/share/monetdb/lib/jdbcclient.jar:. JoinBenchJDBC monetdb

JoinBenchPostgres:
	javac JoinBenchJDBC.java
	java -cp /usr/share/java/postgresql-9.3-1100.jdbc41.jar:. JoinBenchJDBC postgres

JoinBenchSQLite:
	javac JoinBenchJDBC.java
	java -cp /usr/share/java/sqlite-jdbc-3.8.7.jar:. JoinBenchJDBC sqlite

MapReduceBenchMonetDB:
	javac MapReduceBenchJDBC.java
	java -cp /usr/local/share/monetdb/lib/jdbcclient.jar:. MapReduceBenchJDBC monetdb

MapReduceBenchPostgres:
	javac MapReduceBenchJDBC.java
	java -cp /usr/share/java/postgresql-9.3-1100.jdbc41.jar:. MapReduceBenchJDBC postgres

SeqSearchBenchMonetDB:
	javac SeqSearchBenchJDBC.java
	java -cp /usr/local/share/monetdb/lib/jdbcclient.jar:. SeqSearchBenchJDBC monetdb

SeqSearchBenchPostgres:
	javac SeqSearchBenchJDBC.java
	java -cp /usr/share/java/postgresql-9.3-1100.jdbc41.jar:. SeqSearchBenchJDBC postgres

TPCH_Q1: 
	scala -J-Xmx12G -J-Xms12G -optimize TPCH_Q1.scala

TPCH_Q1_hor_seq: 
	scala -J-Xmx12G -J-Xms12G -optimize TPCH_Q1_hor_seq.scala

TPCH_Q1_hor_par: 
	scala -J-Xmx12G -J-Xms12G -optimize TPCH_Q1_hor_par.scala

TPCH_Q1_ver_seq: 
	scala -J-Xmx12G -J-Xms12G -optimize TPCH_Q1_ver_seq.scala

TPCH_Q1_ver_par: 
	scala -J-Xmx12G -J-Xms12G -optimize TPCH_Q1_ver_par.scala
