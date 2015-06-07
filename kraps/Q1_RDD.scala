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

class Q1(sc: SparkContext, input: RDD[Row], push: Boolean) extends RDD[Row](sc, input.dependencies) {
  var iterator:Long = 0
  val decoder = new RowDecoder()

  class Q1Iterator(input:Long) extends Iterator[Row] {
    var row:Long = 0

    def hasNext = {
      if (row == 0) row = nextRow(input)
      row != 0
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
    new Q1Iterator(begin(input.compute(split, context)))
  }      

  protected def getPartitions: Array[Partition] = input.partitions

  @native def rdd(iterator: Iterator[Row]): Long
  @native def nextRow(rdd:Long): Long
}

object NativeTest
{
  def now: Long = java.lang.System.currentTimeMillis()

  def exec(rdd: ExternalRDD) = {
    val start = now
    val result = rdd.fold(0)((x:Int,y:Int) => x+y)
    println(s"Result ${result} produced in ${now - start} second")
  }
 
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Spark intergration with native code")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data_dir = "/mnt/tpch/"
    val lineitem = sqlContext.parquetFile(data_dir + "lineitem.parquet").rdd

    System.loadLibrary("q1rdd")

    exec(new ExternalRDD(sc, lineitem, true))
    exec(new ExternalRDD(sc, lineitem, false))
    exec(new ExternalRDD(sc, lineitem, true))
    exec(new ExternalRDD(sc, lineitem, false))
    exec(new ExternalRDD(sc, lineitem, true))
    exec(new ExternalRDD(sc, lineitem, false)) 
  }
}
