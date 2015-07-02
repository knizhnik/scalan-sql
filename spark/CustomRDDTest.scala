import org.apache.spark._ 
import org.apache.spark.rdd._ 
import org.apache.spark.sql._ 
import org.apache.spark.sql.catalyst.expressions.Row

class CustomRDD(sc: SparkContext, input: RDD[Row]) extends RDD[Int](sc, input.dependencies) {
    def compute(split: Partition, context: TaskContext): Iterator[Int] = {
      val nodes = sc.getExecutorMemoryStatus.size
      val executor = sc.getConf.get("spark.executor.id")
      val driver = sc.getConf.get("spark.driver.host")
      println(s"executor=${executor} driver=${driver} nodes=${nodes}")
      val i = input.compute(split, context)
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
    val nExecutors = sc.getExecutorMemoryStatus.size - 1
    println(s"Executors=${nExecutors}"); 
    val lineitem = sqlContext.parquetFile(data_dir + "Supplier.parquet").rdd.coalesce(nExecutors)

    exec(new CustomRDD(sc, lineitem))
  }
}
