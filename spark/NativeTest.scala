import org.apache.spark._ 
import org.apache.spark.rdd._ 
import org.apache.spark.sql._ 
import org.apache.spark.sql.catalyst.expressions.Row
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.unsafe.memory.TaskMemoryManager
import org.apache.spark.util.{TaskCompletionListener, TaskCompletionListenerException}

class ExternalRDD(sc: SparkContext, input: RDD[Row], push: Boolean) extends RDD[Int](sc, input.dependencies) {
    def compute(split: Partition, context: TaskContext): Iterator[Int] = {
      val i = input.compute(split, context)
      System.err.println(s"context.partitionId=${context.partitionId}")
      val sum = scalaSum(i)
    //  val sum = nativeSumPull(i)
      Seq(sum).iterator
    }      

    def scalaSum(i: Iterator[Row]):Int = {
      var sum = 0
      while (i.hasNext) {
        val row = i.next
        for (col <- 0 until row.size) {
           sum += row(col).hashCode()
        }
      }
      sum
    }

    protected def getPartitions: Array[Partition] = input.partitions

    @native def nativeSumPush(arr: Array[Row]):Int
    @native def nativeSumPushArr(arr: Array[Int]):Int
    @native def nativeSumPull(iter: Iterator[Row]):Int
}
    class CombineRDD[T: ClassTag](prev: RDD[T], maxPartitions: Int) extends RDD[T](prev)
    {
      val inputPartitions = prev.partitions
      class CombineIterator(partitions: Array[Partition], index: Int, context: TaskContext) extends Iterator[T]
      {
        var iter : Iterator[T] = null
        var i = index
        def hasNext() : Boolean = {        
          while ((iter == null || !iter.hasNext) && i < partitions.length) { 
            val ctx = new CombineTaskContext(context.stageId, context.partitionId, context.taskAttemptId, context.attemptNumber, null/*context.taskMemoryManager*/, context.isRunningLocally, context.taskMetrics)     
            iter = firstParent[T].compute(partitions(i), ctx)
            //ctx.complete()
            partitions(i) = null
            i = i + maxPartitions
          }
          iter != null && iter.hasNext
        }
      
        def next() = { iter.next }
     }

     class CombineTaskContext(val stageId: Int,
       val partitionId: Int,
       override val taskAttemptId: Long,
       override val attemptNumber: Int,
       override val taskMemoryManager: TaskMemoryManager,
       val runningLocally: Boolean = true,
       val taskMetrics: TaskMetrics = null) extends TaskContext 
     {
       @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

       override def attemptId(): Long = taskAttemptId

       override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
         onCompleteCallbacks += listener
         this
       }
       def complete(): Unit = {
         // Process complete callbacks in the reverse order of registration
         onCompleteCallbacks.reverse.foreach { listener =>
           listener.onTaskCompletion(this)
         }
       }
       override def addTaskCompletionListener(f: TaskContext => Unit): this.type = {
         onCompleteCallbacks += new TaskCompletionListener {
           override def onTaskCompletion(context: TaskContext): Unit = f(context)
         }
         this
       }
       override def addOnCompleteCallback(f: () => Unit) {
         onCompleteCallbacks += new TaskCompletionListener {
           override def onTaskCompletion(context: TaskContext): Unit = f()
         }
       }
       override def isCompleted(): Boolean = false

       override def isRunningLocally(): Boolean = true

       override def isInterrupted(): Boolean = false
     } 

       
     case class CombinePartition(index : Int) extends Partition

     protected def getPartitions: Array[Partition] = Array.tabulate(maxPartitions){i => CombinePartition(i)}
 
     override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
        new CombineIterator(inputPartitions, partition.index, context)
     }
   }
       


object NativeTest
{
  def now: Long = java.lang.System.currentTimeMillis()

  def exec(rdd: ExternalRDD) = {
    val start = now
    val result = rdd.fold(0)((x:Int,y:Int) => x+y)
    System.err.println(s"Result ${result} produced in ${now - start} second")
  }
 
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Spark intergration with native code")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data_dir = "/mnt/tpch/"
    val lineitem = new CombineRDD[Row](sqlContext.parquetFile(data_dir + "lineitem.parquet").rdd, 16)

    //System.loadLibrary("nativerdd")

    exec(new ExternalRDD(sc, lineitem, false))
  }
}
