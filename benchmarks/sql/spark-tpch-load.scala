import org.apache.spark.sql._
import org.apache.spark.sql.types._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//val data_dir = "hdfs://strong:9000/"
val data_dir = "/srv/remote/all-common/tpch/data/"

val lineitems = sc.textFile(data_dir + "lineitem.tbl")
val orders = sc.textFile(data_dir + "orders.tbl")
val customer = sc.textFile(data_dir + "customer.tbl")
val supplier = sc.textFile(data_dir + "supplier.tbl")
val partsupp = sc.textFile(data_dir + "partsupp.tbl")
val region = sc.textFile(data_dir + "region.tbl")
val nation = sc.textFile(data_dir + "nation.tbl")
val part = sc.textFile(data_dir + "part.tbl")

val ordersSchema = StructType(Array(
       StructField("o_orderkey", LongType, false),
       StructField("o_custkey", IntegerType, false),
       StructField("o_orderstatus", ByteType, false),
       StructField("o_totalprice", DoubleType, false),
       StructField("o_orderdate", IntegerType, false),
       StructField("o_orderpriority", StringType, false),
       StructField("o_clerk", StringType, false),
       StructField("o_shippriority", IntegerType, false),
       StructField("o_comment", StringType, false)));

val customerSchema = StructType(Array(
       StructField("c_custkey", IntegerType, false),
       StructField("c_name", StringType, false),
       StructField("c_address", StringType, false),
       StructField("c_nationkey", IntegerType, false),
       StructField("c_phone", StringType, false),
       StructField("c_acctbal", DoubleType, false),
       StructField("c_mktsegment", StringType, false),
       StructField("c_comment", StringType, false)));

val supplierSchema = StructType(Array(
       StructField("s_suppkey", IntegerType, false),
       StructField("s_name", StringType, false),
       StructField("s_address", StringType, false),
       StructField("s_nationkey", IntegerType, false),
       StructField("s_phone", StringType, false),
       StructField("s_acctbal", DoubleType, false),
       StructField("s_comment", StringType, false)));

val partsuppSchema = StructType(Array(
       StructField("ps_partkey", IntegerType, false),
       StructField("ps_suppkey", IntegerType, false),
       StructField("ps_availqty", IntegerType, false),
       StructField("ps_supplycost", DoubleType, false),
       StructField("ps_comment", StringType, false)));


val regionSchema = StructType(Array(
       StructField("r_regionkey", IntegerType, false),
       StructField("r_name", StringType, false),
       StructField("r_comment", StringType, false)));

val nationSchema = StructType(Array(
       StructField("n_nationkey", IntegerType, false),
       StructField("n_name", StringType, false),
       StructField("n_regionkey", IntegerType, false),
       StructField("n_comment", StringType, false)));

val partSchema = StructType(Array(
       StructField("p_partkey", IntegerType, false),
       StructField("p_name", StringType, false),
       StructField("p_mfgr", StringType, false),
       StructField("p_brand", StringType, false),
       StructField("p_type", StringType, false),
       StructField("p_size", IntegerType, false),
       StructField("p_container", StringType, false),
       StructField("p_retailprice", DoubleType, false),
       StructField("p_comment", StringType, false)));

val lineitemSchema = StructType(Array(
        StructField("l_orderkey", LongType, false), 
        StructField("l_partkey", IntegerType, false),
        StructField("l_suppkey", IntegerType, false),
        StructField("l_linenumber", IntegerType, false),
        StructField("l_quantity", DoubleType, false),
        StructField("l_extendedprice", DoubleType, false),
        StructField("l_discount", DoubleType, false),
        StructField("l_tax", DoubleType, false),
        StructField("l_returnflag", ByteType, false),
        StructField("l_linestatus", ByteType, false),
        StructField("l_shipdate", IntegerType, false),
        StructField("l_commitdate", IntegerType, false),
        StructField("l_receiptdate", IntegerType, false),
        StructField("l_shipinstruct", StringType, false),
        StructField("l_shipmode", StringType, false),
        StructField("l_comment", StringType)))

def parseDate(s: String): Int = (s.substring(0,4) + s.substring(5,7) + s.substring(8,10)).toInt

val lineitemRDD = lineitems.map(_.split("\\|")).map(p => Row(p(0).toLong, p(1).toInt, p(2).toInt, p(3).toInt, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8)(0).toByte, p(9)(0).toByte, parseDate(p(10)), parseDate(p(11)), parseDate(p(12)), p(13), p(14), p(15)))
val ordersRDD = orders.map(_.split("\\|")).map(p => Row(p(0).toLong, p(1).toInt, p(2)(0).toByte, p(3).toDouble, parseDate(p(4)), p(5), p(6), p(7).toInt, p(8)))
val customerRDD = customer.map(_.split("\\|")).map(p => Row(p(0).toInt, p(1), p(2), p(3).toInt, p(4), p(5).toDouble, p(6), p(7)))
val supplierRDD = customer.map(_.split("\\|")).map(p => Row(p(0).toInt, p(1), p(2), p(3).toInt, p(4), p(5).toDouble, p(6)))
val partsuppRDD = partsupp.map(_.split("\\|")).map(p => Row(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toDouble, p(4)))
val regionRDD = region.map(_.split("\\|")).map(p => Row(p(0).toInt, p(1), p(2)))
val nationRDD = nation.map(_.split("\\|")).map(p => Row(p(0).toInt, p(1), p(2).toInt, p(3)))
val partRDD = part.map(_.split("\\|")).map(p => Row(p(0).toInt, p(1), p(2), p(3), p(4), p(5).toInt, p(6), p(7).toDouble, p(8)))


val lineitemSchemaRDD = sqlContext.applySchema(lineitemRDD, lineitemSchema)
val ordersSchemaRDD = sqlContext.applySchema(ordersRDD, ordersSchema)
val customerSchemaRDD = sqlContext.applySchema(customerRDD, customerSchema)
val supplierSchemaRDD = sqlContext.applySchema(supplierRDD, supplierSchema)
val partsuppSchemaRDD = sqlContext.applySchema(partsuppRDD, partsuppSchema)
val regionSchemaRDD = sqlContext.applySchema(regionRDD, regionSchema)
val nationSchemaRDD = sqlContext.applySchema(nationRDD, nationSchema)
val partSchemaRDD = sqlContext.applySchema(partRDD, partSchema)

val data_dir = "hdfs://strong:9121/"
lineitemSchemaRDD.saveAsParquetFile(data_dir + "Lineitem.parquet")
ordersSchemaRDD.saveAsParquetFile(data_dir + "Orders.parquet")
customerSchemaRDD.saveAsParquetFile(data_dir + "Customer.parquet")
supplierSchemaRDD.saveAsParquetFile(data_dir + "Supplier.parquet")
partsuppSchemaRDD.saveAsParquetFile(data_dir + "Partsupp.parquet")
regionSchemaRDD.saveAsParquetFile(data_dir + "Region.parquet")
nationSchemaRDD.saveAsParquetFile(data_dir + "Nation.parquet")
partSchemaRDD.saveAsParquetFile(data_dir + "Part.parquet")
