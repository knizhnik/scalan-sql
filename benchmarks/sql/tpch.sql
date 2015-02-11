-- ./bin/spark-shell --master local[4]

create table lineitem(
   l_orderkey Int,
   l_partkey Int,
   l_suppkey Int,
   l_linenumber Int,
   l_quantity Double,
   l_extendedprice Double,
   l_discount Double,
   l_tax Double,
   l_returnflag Byte,
   l_linestatus Byte,
   l_shipdate Timestamp,
   l_commitdate Timestamp,
   l_receiptdate Timestamp,
   l_shipinstruct String,
   l_shipmode String,
   l_comment String);

----------------------------------
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val lineitems = sc.textFile("/home/knizhnik/tpch-data/sf1/lineitem.tbl")
val schema = StructType(Array(
        StructField("l_orderkey", IntegerType, true), 
        StructField("l_partkey", IntegerType, true),
        StructField("l_suppkey", IntegerType, true),
        StructField("l_linenumber", IntegerType, true),
        StructField("l_quantity", DoubleType, true),
        StructField("l_extendedprice", DoubleType, true),
        StructField("l_discount", DoubleType, true),
        StructField("l_tax", DoubleType, true),
        StructField("l_returnflag", ByteType, true),
        StructField("l_linestatus", ByteType, true),
        StructField("l_shipdate", IntegerType, true),
        StructField("l_commitdate", IntegerType, true),
        StructField("l_receiptdate", IntegerType, true),
        StructField("l_shipinstruct", StringType, true),
        StructField("l_shipmode", StringType, true),
        StructField("l_comment", StringType)))
def parseDate(s: String): Int = (s.substring(0,4) + s.substring(5,7) + s.substring(8,10)).toInt
val lineitemRDD = lineitem.map(_.split("\\|")).map(p => Row(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toInt, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8)(0).toByte, p(9)(0).toByte, parseDate(p(10)), parseDate(p(11)), parseDate(p(12)), p(13), p(14), p(15)))
val lineitemSchemaRDD = sqlContext.applySchema(lineitemRDD, schema)
lineitemSchemaRDD.registerTempTable("lineitemRDD")
def now: Long = java.lang.System.currentTimeMillis()
def exec(sql :String) = {
  val start = now
  val results = sqlContext.sql(sql)
  results.collect().foreach(println)
  println("Elapsed time: " + (now - start))
}
exec("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitemRDD where l_shipdate <= 19981201 group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus")

lineitemSchemaRDD.saveAsParquetFile("lineitem.parquet")
val parquetFile = sqlContext.parquetFile("lineitem.parquet")
parquetFile.registerTempTable("parquetLineitem")

exec("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from parquetLineitem where l_shipdate <= 19981201 group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus")





create table lineitem(
   l_orderkey integer,
   l_partkey integer,
   l_suppkey integer,
   l_linenumber integer,
   l_quantity real,
   l_extendedprice real,
   l_discount real,
   l_tax real,
   l_returnflag char,
   l_linestatus char,
   l_shipdate date,
   l_commitdate date,
   l_receiptdate date,
   l_shipinstruct char(25),
   l_shipmode char(10),
   l_comment char(44));


PostgreSQL:

create table lineitem(
   l_orderkey integer,
   l_partkey integer,
   l_suppkey integer,
   l_linenumber integer,
   l_quantity real,
   l_extendedprice real,
   l_discount real,
   l_tax real,
   l_returnflag char,
   l_linestatus char,
   l_shipdate date,
   l_commitdate date,
   l_receiptdate date,
   l_shipinstruct char(25),
   l_shipmode char(10),
   l_comment char(44),
   l_dummy char(1));

create table orders(
    o_orderkey integer,
    o_custkey integer,
    o_orderstatus char,
    o_totalprice real,
    o_orderdate date,
    o_orderpriority varchar,
    o_clerk varchar,
    o_shippriority integer,
    o_comment varchar,
    o_dummy char(1));

create table customer(
    c_custkey integer,
    c_name varchar,
    c_address varchar,
    c_nationkey integer,
    c_phone varchar,
    c_acctbal real,
    c_mktsegment varchar,
    c_comment varchar,
    c_dummy char(1));

create table nation(
    n_nationkey integer,
    n_name varchar,
    n_regionkey integer,
    n_comment varchar,
    c_dummy char(1));


create index lineitem_order_fk on lineitem(l_orderkey);
create index customer_pk on customer(c_custkey);
create index orders_pk on orders(o_orderkey);
create index orders_cust_fk on orders(o_custkey);
create index nation_pk on nation(n_nationkey);

copy lineitem from '/home/postgres/lineitem.tbl' delimiter '|' csv;
copy customer from '/home/postgres/customer.tbl' delimiter '|' csv;
copy orders from '/home/postgres/orders.tbl' delimiter '|' csv;
copy nation from '/home/postgres/nation.tbl' delimiter '|' csv;

select
    l_orderkey,
    sum(l_extendedprice*(1-l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer join orders on c_custkey = o_custkey
    join lineitem on l_orderkey = o_orderkey
where
    c_mktsegment = 'HOUSEHOLD'
    and o_orderdate < cast('1995-03-04' as date)
    and l_shipdate > cast('1995-03-04' as date)
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate;

select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    orders join customer on c_custkey = o_custkey
    join lineitem on l_orderkey = o_orderkey
    join nation on c_nationkey = n_nationkey
where
    o_orderdate >= cast('1994-11-01' as date) and o_orderdate < cast('1995-02-01' as date)
    and l_returnflag = 'R'
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc;


eXtremeDB:
insert into lineitem select * from foreign table (path='/home/knizhnik/tpch-data/sf1/lineitem.tbl', delimiter='|') as lineitem;

MySQL:
load data infile '/home/knizhnik/tpch-data/sf1/lineitem.tbl' into table lineitem  fields terminated by '|' lines terminated by '\n'

select 
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= cast('1998-12-01' as date)
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
