import org.apache.spark._ 
import org.apache.spark.rdd._ 
import org.apache.spark.sql._ 
import org.apache.spark.sql.catalyst.expressions.Row

class ExternalRDD(sc: SparkContext, input: RDD[Row], push: Boolean) extends RDD[Int](sc, input.dependencies) {
    def compute(split: Partition, context: TaskContext): Iterator[Int] = {
      val i = input.compute(split, context)
      val sum = if (push) nativeSumPush(i.toArray) else nativeSumPull(i)
      Seq(sum).iterator
    }      

    protected def getPartitions: Array[Partition] = input.partitions

    @native def nativeSumPush(arr: Array[Row]):Int
    @native def nativeSumPull(iter: Iterator[Row]):Int
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

    System.loadLibrary("nativerdd")

    exec(new ExternalRDD(sc, lineitem, true))
    exec(new ExternalRDD(sc, lineitem, false))
    exec(new ExternalRDD(sc, lineitem, true))
    exec(new ExternalRDD(sc, lineitem, false))
    exec(new ExternalRDD(sc, lineitem, true))
    exec(new ExternalRDD(sc, lineitem, false)) 
  }
}
