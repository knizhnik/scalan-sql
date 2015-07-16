import org.apache.spark._ 
import org.apache.spark.rdd._ 
import org.apache.spark.sql._ 
import org.apache.spark.sql.catalyst.expressions.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.executor.TaskMetrics
// import org.apache.spark.unsafe.memory.TaskMemoryManager
import org.apache.spark.util.TaskCompletionListener

class RowDecoder extends Serializable
{
  @native def getInt(row: Long, offs: Int): Int
  @native def getLong(row: Long, offs: Int): Long
  @native def getByte(row: Long, offs: Int): Byte
  @native def getDouble(row: Long, offs: Int): Double
}

class CombineIterator(input: RDD[Row], partitions: Array[Partition], index: Int, nNodes: Int, context: TaskContext) extends Iterator[Row]
{
  var iter : Iterator[Row] = null
  var i = index
  def hasNext() : Boolean = {        
    while ((iter == null || !iter.hasNext) && i < partitions.length) { 
      val ctx = new CombineTaskContext(context.stageId, context.partitionId, context.taskAttemptId, context.attemptNumber/*, null context.taskMemoryManager*/, context.isRunningLocally, context.taskMetrics)     
      iter = input.compute(partitions(i), ctx)
      //ctx.complete()
      partitions(i) = null
      i = i + nNodes
    }
    iter != null && iter.hasNext
  }

  def next() = { iter.next }
}

class CombineTaskContext(
  val stageId: Int,
  val partitionId: Int,
  override val taskAttemptId: Long,
  override val attemptNumber: Int,
 // override val taskMemoryManager: TaskMemoryManager,
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

class Q1(@transient input: RDD[Row], nNodes: Int) extends RDD[Row](input) {
  val inputPartitions = input.partitions
  @transient var iterator:Long = 0
  @transient var query:Long = 0
  val decoder = new RowDecoder()

  class Q1Iterator(input:Long) extends Iterator[Row] {
    var row:Long = 0
    var eof:Boolean = false

    def hasNext = {
      if (eof) { 
         false
      } else {  
         if (row == 0) row = nextRow(input) 
         if (row == 0) { 
           eof = true
           false
         } else { 
           true
         }
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

  protected def getPartitions: Array[Partition] = Array.tabulate(nNodes){i => CombinePartition(i)}
 
  def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    println("Execute computer for parition " + split.index)
    System.load("/srv/remote/all-common/tpch/data/libq1rdd.so")
    println("Create Q1 iterator")
    new Q1Iterator(runQuery(new CombineIterator(firstParent[Row], inputPartitions, split.index, nNodes, context), nNodes))
  }      
 
  @native def runQuery(iterator: Iterator[Row], nNodes: Int): Long
  @native def nextRow(rdd:Long): Long
  @native def freeRow(row:Long)
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
    val conf = new SparkConf().setAppName("Spark intergration with Kraps")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val nExecutors = 4
    val dataDir = "hdfs://strong:9121/"
    val lineitem = sqlContext.parquetFile(dataDir + "Lineitem.parquet")
    
    exec(new Q1(lineitem.rdd, nExecutors))
  }
}
