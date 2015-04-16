import org.apache.spark.sql._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val data_dir = "hdfs://strong:9000/"
//val data_dir = "/mnt/tpch/"
sqlContext.sql("set spark.sql.codegen=true")

sqlContext.parquetFile(data_dir + "lineitem.parquet").registerTempTable("lineitem")
sqlContext.parquetFile(data_dir + "orders.parquet").registerTempTable("orders")
sqlContext.parquetFile(data_dir + "customer.parquet").registerTempTable("customer")
sqlContext.parquetFile(data_dir + "supplier.parquet").registerTempTable("supplier")
sqlContext.parquetFile(data_dir + "partsupp.parquet").registerTempTable("partsupp")
sqlContext.parquetFile(data_dir + "region.parquet").registerTempTable("region")
sqlContext.parquetFile(data_dir + "nation.parquet").registerTempTable("nation")
sqlContext.parquetFile(data_dir + "part.parquet").registerTempTable("part")

def now: Long = java.lang.System.currentTimeMillis()

def exec(query: String, df: DataFrame) = {
  df.explain()
  val start = now
  df.show()
  println(s"Elapsed time for ${query}: ${now - start}")
}

val lineitem = sqlContext.parquetFile(data_dir + "lineitem.parquet")
val orders = sqlContext.parquetFile(data_dir + "orders.parquet")
val customer = sqlContext.parquetFile(data_dir + "customer.parquet")
val supplier = sqlContext.parquetFile(data_dir + "supplier.parquet")
val partsupp = sqlContext.parquetFile(data_dir + "partsupp.parquet")
val region = sqlContext.parquetFile(data_dir + "region.parquet")
val nation = sqlContext.parquetFile(data_dir + "nation.parquet")
val part = sqlContext.parquetFile(data_dir + "part.parquet")

val q1 = lineitem.filter(lineitem("l_shipdate") <= 19981201).groupBy("l_returnflag", "l_linestatus").agg(
    $"l_returnflag",
    $"l_linestatus",
    sum("l_quantity"),
    sum("l_extendedprice"),
    sum($"l_extendedprice" * (lit(1) - $"l_discount")),
    sum($"l_extendedprice" * (lit(1) - $"l_discount") * (lit(1) + $"l_tax")),
    avg("l_quantity"),
    avg("l_extendedprice"),
    avg("l_discount"),
    count("*")).orderBy("l_returnflag","l_linestatus")


exec("Q1", q1)

val q5 = orders.filter(orders("o_orderdate") >= 19960101 and orders("o_orderdate") < 19970101).
    join(lineitem, lineitem("l_orderkey") === orders("o_orderkey")).
    join(supplier, lineitem("l_suppkey") === supplier("s_suppkey")).
    join(customer, customer("c_custkey") === orders("o_custkey") and customer("c_nationkey") === supplier("s_nationkey")).
    join(nation, customer("c_nationkey") === nation("n_nationkey")).
    join(region, nation("n_regionkey") === region("r_regionkey")).
    filter(region("r_name") === lit("ASIA")).
    groupBy("n_name").
    agg(sum(lineitem("l_extendedprice") * (lit(1)-lineitem("l_discount"))) as "revenue").
    orderBy($"revenue".desc)

exec("Q5", q5)



sqlContext.sql("set spark.sql.codegen=true")
//sqlContext.sql("set spark.sql.shuffle.partitions=64")

def now: Long = java.lang.System.currentTimeMillis()

def exec(test: String, sql: String) = {
  val start = now
  val results = sqlContext.sql(sql)
  results.collect().foreach(println)
  println(s"Elapsed time for ${test}: ${now - start}")
}

exec("Q1", """
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
    l_shipdate <= 19981201
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus""")

exec("Q3", """
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
    and o_orderdate < 19950304
    and l_shipdate > 19950304
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    o_orderdate""")

exec("Q4", """
select
    o_orderpriority,
    count(*) as order_count
from
    orders join lineitem on l_orderkey = o_orderkey
where
    o_orderdate >= 19930801
    and o_orderdate < 19931101
    and l_commitdate < l_receiptdate
group by
    o_orderpriority
order by
    o_orderpriority""")


exec("Q5", """
select
    n_name,
    sum(l_extendedprice * (1-l_discount)) as revenue
from
    customer join orders on c_custkey = o_custkey
    join lineitem on l_orderkey = o_orderkey
    join supplier on l_suppkey = s_suppkey
    join nation on c_nationkey = n_nationkey
    join region on n_regionkey = r_regionkey
where
    c_nationkey = s_nationkey
    and r_name = 'ASIA'
    and o_orderdate >= 19960101
    and o_orderdate < 19970101
group by
    n_name
order by
    revenue desc""")


exec("Q6", """
select
    sum(l_extendedprice*l_discount) as revenue
from
    lineitem
where
    l_shipdate between 19960101 and 19970101
    and l_discount between 0.08 and 0.1
    and l_quantity < 24""")


exec("Q7", """
select
    supp_nation,
    cust_nation,
    l_year, sum(volume) as revenue
from (
    select
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        l_shipdate/10000 as l_year,
        l_extendedprice * (1-l_discount) as volume
    from
        supplier join lineitem on s_suppkey = l_suppkey
        join orders on o_orderkey = l_orderkey
        join customer on c_custkey = o_custkey
        join nation as n1 on s_nationkey = n1.n_nationkey
        join nation as n2 on c_nationkey = n2.n_nationkey
    where
        ((n1.n_name = 'UNITED STATES' and n2.n_name = 'INDONESIA')
         or (n1.n_name = 'INDONESIA' and n2.n_name = 'UNITED STATES'))
         and l_shipdate between 19950101 and 19961231) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year""")

exec("Q8", """
select
    o_year,
    nation_volume / total_volume as mkt_share
from
    (select
        o_year,
        sum(case
            when nation = 'INDONESIA'
            then volume
            else 0 end) as nation_volume,
        sum(volume) as total_volume
    from (
        select
            o_orderdate/10000 as o_year,
            l_extendedprice * (1-l_discount) as volume,
            n2.n_name as nation
        from
            part join lineitem on p_partkey = l_partkey
            join supplier on s_suppkey = l_suppkey
            join orders on l_orderkey = o_orderkey
            join customer on o_custkey = c_custkey
            join nation n1 on c_nationkey = n1.n_nationkey
            join nation n2 on s_nationkey = n2.n_nationkey
            join region on n1.n_regionkey = r_regionkey
        where
            r_name = 'ASIA'
            and o_orderdate between 19950101 and 19961231
            and p_type = 'MEDIUM ANODIZED NICKEL'
        ) as all_nations
    group by
        o_year) as mkt
order by
    o_year""")

exec("Q9", """
select
    nation,
    o_year,
    sum(amount) as sum_profit
from (
    select
        n_name as nation,
        o_orderdate/10000 as o_year,
        l_extendedprice*(1-l_discount)-ps_supplycost * l_quantity as amount
    from
        lineitem join supplier on s_suppkey = l_suppkey
        join part on p_partkey = l_partkey
        join partsupp on ps_partkey = l_partkey and ps_suppkey = l_suppkey
        join orders on o_orderkey = l_orderkey
        join nation on s_nationkey = n_nationkey
    where
        p_name like '%ghost%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc""")

exec("Q10", """
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
    o_orderdate >= 19941101 and o_orderdate < 19950201
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
    revenue desc""")

exec("Q12", """
select
    l_shipmode,
    sum(case
        when o_orderpriority ='1-URGENT'
        or o_orderpriority ='2-HIGH'
        then 1
        else 0
        end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
        and o_orderpriority <> '2-HIGH'
        then 1
        else 0
        end) as low_line_count
from
    orders join lineitem on o_orderkey = l_orderkey
where
    l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= 19940101
    and l_receiptdate < 19950101
group by
    l_shipmode
order by
    l_shipmode""")


exec("Q13", """
select
    c_count,
    count(*) as custdist
from (
    select
        c_custkey,
        count(o_orderkey) as c_count
    from
        customer left outer join orders on c_custkey = o_custkey
    where
        o_comment not like '%unusual%packages%'
    group by
        c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc""")


exec("Q14", """
select
    100.00 * sum(case
              when p_type like 'PROMO%'
              then l_extendedprice*(1-l_discount)
              else 0 end)
        / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem join part on l_partkey = p_partkey
    where
        l_shipdate >= 19940301
        and l_shipdate < 19940401""")


exec("Q19", """
select
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    lineitem join part on p_partkey = l_partkey
where
    (p_brand = 'Brand#31'
    and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and l_quantity >= 26 and l_quantity <= 36
    and p_size between 1 and 5
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON')
or
    (p_brand = 'Brand#43'
    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and l_quantity >= 15 and l_quantity <= 25
    and p_size between 1 and 10
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON')
or
    (p_brand = 'Brand#43'
    and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and l_quantity >= 4 and l_quantity <= 14
    and p_size between 1 and 15
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON')""")

