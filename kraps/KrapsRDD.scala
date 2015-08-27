import org.apache.spark._ 
import org.apache.spark.rdd._ 
import org.apache.spark.sql._ 
import org.apache.spark.sql.catalyst.expressions.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.executor.TaskMetrics
// import org.apache.spark.unsafe.memory.TaskMemoryManager
import org.apache.spark.util.TaskCompletionListener
import sun.misc.Unsafe
import org.apache.spark.sql.functions._

class RowIterator(input: RDD[Row], partitions: Array[Partition], index: Int, nNodes: Int, context: TaskContext, serialize: (Unsafe,Long,Row)=>Int)
{
  var iter : Iterator[Row] = null
  var i = index
  val unsafe = getUnsafe
 
  def getUnsafe = {
    val cons = classOf[Unsafe].getDeclaredConstructor()
    cons.setAccessible(true)
    cons.newInstance().asInstanceOf[Unsafe]
  }

  def next(row:Long): Boolean = {        
    while ((iter == null || !iter.hasNext) && i < partitions.length) { 
      val ctx = new CombineTaskContext(context.stageId, context.partitionId, context.taskAttemptId, context.attemptNumber/*, null context.taskMemoryManager*/, context.isRunningLocally, context.taskMetrics)     
      iter = input.compute(partitions(i), ctx)
      //ctx.complete()
      partitions(i) = null
      i = i + nNodes
    }
    if (iter != null && iter.hasNext) {
      serialize(unsafe, row, iter.next) != 0
    } else {
      false
    }
  }
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

object KrapsCluster()
{
  @native def start(hosts: Array[String], nCores: Int): Unit
  @native def stop(): Unit
}

class KrapsRDD(queryId: Int, nNodes: Int, @transient input: Array[RDD[Row]], @transient serializers: Array[(Unsafe,Long,Row)=>Int, @transient deserializer: (Unsafe,Long)=>Row)
extends RDD[Row]
{ 
  protected def getPartitions: Array[Partition] = Array.tabulate(nNodes){i => CombinePartition(i)}

  class KrapsIterator(input:Long) extends Iterator[Row] {
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
      val curr = row
      row = 0
      deserializer(unsafe, curr)
    }
  }

  def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    new KrapsIterator(createIterator(queryId, Array.tabulate(i => new RowIterator(input(i), input(i).partitions, split.index, nNodes, context, serializers(i)))))
  }

  @native def createIterator(queryId: Int, input: Array[RowIterator]) : Long
  @native def nextRow(iterator: Long)
}  
