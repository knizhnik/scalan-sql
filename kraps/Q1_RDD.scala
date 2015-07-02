import org.apache.spark._ 
import org.apache.spark.rdd._ 
import org.apache.spark.sql._ 
import org.apache.spark.sql.catalyst.expressions.Row

class RowDecoder
{
  @native def getInt(row: Long, offs: Int): Int
  @native def getLong(row: Long, offs: Int): Long
  @native def getByte(row: Long, offs: Int): Byte
  @native def getDouble(row: Long, offs: Int): Double
}

class Q1(input: RDD[Row], nNodes: Int) extends RDD[Row](input) {
  @transient var iterator:Long = 0
  @transient val decoder = new RowDecoder()
  @transient var query:Long = 0

  class Q1Iterator(input:Long) extends Iterator[Row] {
    var row:Long = 0

    def hasNext = {
      if (row == 0) row = nextRow(input) 
      if (row == 0) { 
        freeQuery(input)        
        false
      } else { 
        true
      }
    }

    def next:Row = {
      if (row == 0) throw new java.lang.IllegalStateException()
      val result = Row(decoder.getDouble(row, 0),  
          decoder.getDouble(row, 8),  
          decoder.getDouble(row, 16),  
          decoder.getDouble(row, 24),  
          decoder.getDouble(row, 32),  
          decoder.getDouble(row, 40),  
          decoder.getDouble(row, 48),  
          decoder.getLong(row, 56),  
          decoder.getByte(row, 64),  
          decoder.getByte(row, 65))
      freeRow(row)
      row = 0
      result
    }
  }

  def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    System.load("/srv/remote/all-common/tpch/data/libq1rdd.so")
    new Q1Iterator(prepareQuery(input.compute(split, context), nNodes))
  }      
 
  protected def getPartitions: Array[Partition] = input.partitions

  @native def prepareQuery(iterator: Iterator[Row], nNodes: Int): Long
  @native def nextRow(rdd:Long): Long
  @native def freeRow(row:Long)
  @native def freeQuery(query:Long)
}

object Q1
{
  def now: Long = java.lang.System.currentTimeMillis()

  def exec(rdd: RDD[Row]) = {
    val start = now
    rdd.collect().foreach(println)
    println(s"Elapsed time ${now - start} seconds")
  }
 
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Spark intergration with native code")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val nExecutors = 4
    val data_dir = "hdfs://strong:9121/"
    val lineitem = sqlContext.parquetFile(data_dir + "Lineitem.parquet").rdd.coalesce(nExecutors)

    exec(new Q1(lineitem, nExecutors))
  }
}
