import org.apache.spark._ 
import org.apache.spark.rdd._ 
import org.apache.spark.sql._ 
import org.apache.spark.sql.catalyst.expressions.Row

class CustomRDD(@transient sc: SparkContext, input: RDD[Row]) extends RDD[Int](sc, input.dependencies) {
    def compute(split: Partition, ctx: TaskContext): Iterator[Int] = {
      /*
      val nodes = context.getExecutorMemoryStatus.size
      val executor = context.getConf.get("spark.executor.id")
      val driver = context.getConf.get("spark.driver.host")
      println(s"executor=${executor} driver=${driver} nodes=${nodes}")
      */
      val i = input.compute(split, ctx)
      var sum = 0
      while (i.hasNext) sum = sum + i.next.getInt(0)
      Seq(sum).iterator
    }      

    protected def getPartitions: Array[Partition] = input.partitions
}

object CustomRDDTest
{
  def now: Long = java.lang.System.currentTimeMillis()

  def exec(rdd: CustomRDD) = {
    val start = now
    val result = rdd.fold(0)((x:Int,y:Int) => x+y)
    println(s"Result ${result} produced in ${now - start} second")
  }
 
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Spark intergration with native code")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data_dir = "hdfs://strong:9121/"
    val nExecutors =  4
    val lineitem = sqlContext.parquetFile(data_dir + "Supplier.parquet").rdd.coalesce(nExecutors)

    exec(new CustomRDD(sc, lineitem))
  }
}
