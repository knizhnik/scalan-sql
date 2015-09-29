
import org.apache.spark.sql.hive._
val hivesc = new HiveContext(sc)
hivesc.setConf("spark.sql.nuwa.cbo.enable","true")
hivesc.setConf("spark.sql.hive.convertMetastoreParquet", "true")
hivesc.setConf("spark.sql.parquet.binaryAsString", "true")
hivesc.setConf("spark.sql.parquet.filterPushdown", "false")
hivesc.hql("use tpch100g_date")


val q1 = "SELECT l_returnflag, l_linestatus, sum(l_quantity) sum_qty, sum(l_extendedprice) sum_base_price, sum(l_extendedprice * (1 - l_discount)) sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) sum_charge, avg(l_quantity) avg_qty, avg(l_extendedprice) avg_price, avg(l_discount) avg_disc, count(* ) count_order FROM lineitem_parquet_string WHERE l_shipdate <=  '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
val start = System.currentTimeMillis
hivesc.hql(q1).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q2 = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part_parquet_string join supplier_parquet_string join partsupp_parquet_string join nation_parquet_string join region_parquet_string join (SELECT ps_partkey xxx, min(ps_supplycost) min_ps_supplycost FROM partsupp_parquet_string join supplier_parquet_string join nation_parquet_string join region_parquet_string WHERE s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' group by ps_partkey) subquery WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND p_partkey = subquery.xxx AND ps_supplycost = subquery.min_ps_supplycost ORDER BY s_acctbal DESC, n_name, s_name, p_partkey"
val start = System.currentTimeMillis
hivesc.hql(q2).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q3 = "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) revenue, o_orderdate, o_shippriority FROM customer_parquet_string join orders_parquet_string join lineitem_parquet_string WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate <  '1995-03-15' AND l_shipdate >  '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate"
val start = System.currentTimeMillis
hivesc.hql(q3).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q4 = "SELECT o_orderpriority, count(*) order_count FROM orders_parquet_string join (select DISTINCT l_orderkey from lineitem_parquet_string where l_commitdate < l_receiptdate) t WHERE l_orderkey = o_orderkey AND o_orderdate >=  '1993-07-01' AND o_orderdate <  '1993-10-01' GROUP BY o_orderpriority ORDER BY o_orderpriority"
val start = System.currentTimeMillis
hivesc.hql(q4).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q5 = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) revenue FROM customer_parquet_string join orders_parquet_string join lineitem_parquet_string join supplier_parquet_string join nation_parquet_string join region_parquet_string WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >=  '1994-01-01' AND o_orderdate <  '1995-01-01' GROUP BY n_name ORDER BY revenue DESC"
val start = System.currentTimeMillis
hivesc.hql(q5).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))



val q6 = "SELECT sum(l_extendedprice * l_discount) revenue FROM lineitem_parquet_string WHERE l_shipdate >=  '1994-01-01' AND l_shipdate <  '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
val start = System.currentTimeMillis
hivesc.hql(q6).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q7 = "select supp_nation, cust_nation, l_year, sum(volume) revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, substring(l_shipdate,1,4) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier_parquet_string join lineitem_parquet_string join orders_parquet_string join customer_parquet_string join nation_parquet_string n1 join nation_parquet_string n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between  '1995-01-01' and  '1996-12-31' ) shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year"
val start = System.currentTimeMillis
hivesc.hql(q7).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q8 = "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select substring(o_orderdate,1,4) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from nation_parquet_string n1 join nation_parquet_string n2 join region_parquet_string join supplier_parquet_string join part_parquet_string join customer_parquet_string join orders_parquet_string join lineitem_parquet_string where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between  '1995-01-01' and  '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' ) all_nations group by o_year order by o_year"
val start = System.currentTimeMillis
hivesc.hql(q8).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q9 = "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, substring(o_orderdate,1,4) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from nation_parquet_string join supplier_parquet_string join part_parquet_string join partsupp_parquet_string join orders_parquet_string join lineitem_parquet_string where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%' ) profit group by nation, o_year order by nation, o_year desc"
val start = System.currentTimeMillis
hivesc.hql(q9).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q10 = "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM nation_parquet_string join customer_parquet_string join orders_parquet_string join lineitem_parquet_string WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >=  '1993-10-01' AND o_orderdate <  '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC"
val start = System.currentTimeMillis
hivesc.hql(q10).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q11 = "SELECT t0.ps_partkey, t0.part_value FROM  (SELECT sum(ps_supplycost * ps_availqty) AS total_value FROM nation_parquet_string n JOIN supplier_parquet_string s ON s.s_nationkey = n.n_nationkey AND n.n_name = 'GERMANY' JOIN partsupp_parquet_string ps ON ps.ps_suppkey = s.s_suppkey) t1 JOIN (SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS part_value FROM nation_parquet_string n JOIN supplier_parquet_string s ON s.s_nationkey = n.n_nationkey AND n.n_name = 'GERMANY' JOIN partsupp_parquet_string ps ON ps.ps_suppkey = s.s_suppkey GROUP BY ps_partkey) t0 WHERE t0.part_value > t1.total_value * 0.000001 ORDER BY t0.part_value DESC"
val start = System.currentTimeMillis
hivesc.hql(q11).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q12 = "select l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders_parquet_string join lineitem_parquet_string where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >=  '1994-01-01' and l_receiptdate <  '1995-01-01' group by l_shipmode order by l_shipmode"
val start = System.currentTimeMillis
hivesc.hql(q12).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q13 = "select c_count, count(*) as custdist from (select c_custkey, count(o_orderkey) c_count from customer_parquet_string left outer join orders_parquet_string on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey ) c_orders group by c_count order by custdist desc, c_count desc"
val start = System.currentTimeMillis
hivesc.hql(q13).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))



val q14 = "select 100.0 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0.0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from part_parquet_string join lineitem_parquet_string where l_partkey = p_partkey and l_shipdate >=  '1995-09-01' and l_shipdate <  '1995-10-01'"
val start = System.currentTimeMillis
hivesc.hql(q14).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q15 = "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier_parquet_string join (SELECT l_suppkey supplier_no, sum(l_extendedprice * (1 - l_discount)) total_revenue FROM lineitem_parquet_string WHERE l_shipdate >=  '1996-01-01' AND l_shipdate <  '1996-04-01' GROUP BY l_suppkey) revenue join (select max(total_revenue2) max_total_revenue from (SELECT l_suppkey supplier_no2, sum(l_extendedprice * (1 - l_discount)) total_revenue2 FROM lineitem_parquet_string WHERE l_shipdate >=  '1996-01-01' AND l_shipdate <  '1996-04-01' GROUP BY l_suppkey) revenue2) revenue3 WHERE s_suppkey = supplier_no AND total_revenue = max_total_revenue ORDER BY s_suppkey"
val start = System.currentTimeMillis
hivesc.hql(q15).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q16 = "SELECT p_brand, p_type, p_size, count(ps_suppkey) AS supplier_cnt FROM part_parquet_string join partsupp_parquet_string on p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) left outer join (SELECT s_suppkey FROM supplier_parquet_string WHERE s_comment LIKE '%Customer%Complaints%') tmp on ps_suppkey = s_suppkey WHERE s_suppkey IS NULL GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"
val start = System.currentTimeMillis
hivesc.hql(q16).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q17 = "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM part_parquet_string join lineitem_parquet_string join (SELECT l_partkey tmp_partkey, 0.2 * avg(l_quantity) tmp_quantity FROM lineitem_parquet_string group by l_partkey) tmp WHERE p_partkey = l_partkey AND p_partkey = tmp_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < tmp_quantity"
val start = System.currentTimeMillis
hivesc.hql(q17).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q18 = """SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(sum_quantity) 
FROM customer_parquet_string 
join orders_parquet_string on c_custkey = o_custkey 
join (SELECT l_orderkey tmp_orderkey, sum(l_quantity) sum_quantity FROM lineitem_parquet_string GROUP BY l_orderkey HAVING sum(l_quantity) > 300) tmp on o_orderkey = tmp_orderkey 
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate"""
val start = System.currentTimeMillis
hivesc.hql(q18).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))



val q19 = "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM part_parquet_string join lineitem_parquet_string WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"
val start = System.currentTimeMillis
hivesc.hql(q19).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q20 = "select s.s_name, s.s_address from supplier_parquet_string s join nation_parquet_string n on s.s_nationkey = n.n_nationkey and n.n_name = 'CANADA' join (select t3.ps_suppkey from (select ps.ps_suppkey, ps.ps_availqty, t2.sum_quantity from partsupp_parquet_string ps join (select p_partkey from part_parquet_string where p_name like 'forest%') t1 on ps.ps_partkey = t1.p_partkey join (select l_partkey, l_suppkey, 0.5 * sum(l_quantity) sum_quantity from lineitem_parquet_string where l_shipdate >=  '1994-01-01' and l_shipdate <  '1995-01-01' group by l_partkey, l_suppkey) t2 on ps.ps_partkey = t2.l_partkey and ps.ps_suppkey = t2.l_suppkey) t3 where t3.ps_availqty > t3.sum_quantity group by t3.ps_suppkey) t4 on s.s_suppkey = t4.ps_suppkey order by s.s_name"
val start = System.currentTimeMillis
hivesc.hql(q20).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))



val q21 = "SELECT s_name, count(*) AS numwait FROM nation_parquet_string join supplier_parquet_string on s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' join  (select l_orderkey, l_suppkey from lineitem_parquet_string left semi join (select * from (select l_orderkey from lineitem_parquet_string group by l_orderkey having count(l_suppkey) > 1) tmp1 left semi join (select l_orderkey from lineitem_parquet_string where l_receiptdate > l_commitdate group by l_orderkey having count(l_suppkey) = 1) tmp2 on tmp1.l_orderkey = tmp2.l_orderkey) tmp3 on lineitem_parquet_string.l_orderkey = tmp3.l_orderkey where l_receiptdate > l_commitdate) tmp4 on s_suppkey = tmp4.l_suppkey join orders_parquet_string on o_orderkey = tmp4.l_orderkey AND o_orderstatus = 'F' GROUP BY s_name ORDER BY numwait DESC, s_name"
val start = System.currentTimeMillis
hivesc.hql(q21).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))


val q22 = "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone,1,2) as cntrycode, c_acctbal, c_custkey from customer_parquet_string where substring(c_phone,1,2) in ('13', '31', '23', '29', '30', '18', '17') ) custsale join ( select avg(c_acctbal) tmp_avg_c_acctbal from customer_parquet_string where c_acctbal > 0.00 and substring(c_phone,1,2) in ('13', '31', '23', '29', '30', '18', '17') ) tmp1 on custsale.c_acctbal>tmp_avg_c_acctbal left join orders_parquet_string on c_custkey=o_custkey where o_orderkey is null group by cntrycode order by cntrycode"
val start = System.currentTimeMillis
hivesc.hql(q22).collect
val end = System.currentTimeMillis
println("elapsed time: %f seconds\n".format((end - start)/1000.0))

