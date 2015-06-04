import org.apache.spark.rdd
import org.apache.spark.sql.catalyst.expressions.Row

class ExternalRDD(input: SchemaRDD) extends RDD[Int]
{
    System.loadLibrary("nativerdd")

    def compute(split: Partition, context: TaskContext): Iterator[Int] = {
      val i = input.compute(split, context)
      val sum = native_sum(i.toArray)
      Seq(sum).iterator
      /*
      var jobResult = 0
      val sumPartition = (iter: Iterator[Row]) => native_sum(i.toArray)
      val mergeResults = (index: Int, taskResult: Int) => jobResult = jobResult + taskResult
      sc.runJob(this, sumPartition, mergeResults)
      */
    }
      

    protected def getPartitions: Array[Partition] = input.getPartitions
    protected def getDependencies: Seq[Dependency[_]] = input.getDependencies

    @native def native_sum(arr: Array[Row])
}