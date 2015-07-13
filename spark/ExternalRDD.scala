package com.huawei.hispark

import org.apache.spark.rdd
import org.apache.spark.sql.catalyst.expressions.Row

class ExternalRDD(input: SchemaRDD, push: Boolean) extends RDD[Int]
{
    System.loadLibrary("nativerdd")

    class CoalescedRDD[T](
      @transient var prev: RDD[T],
      maxPartitions: Int,
      balanceSlack: Double = 0.10) extends CoalescedRDD[T](prev, maxPartitions, balanceSlack)
    {
      class CoalescedIterator(partitions: Iterator[Partition], context: TaskContext) extends Iterator[T]
      {
        var i : Iterator[T] = null
        def hasNext() : Boolean = {        
          while ((i == null || !i.hasNext()) && partitions.hasNext()) { 
            i = firstParent[T].iterator(partitions.next, context)
          }
          i != null && i.hasNext()
        }
      
        def next() = { i.next() }
     }
       
     override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
        new CoalescedIterator(partition.asInstanceOf[CoalescedRDDPartition].parents.iterator, context)
     }
   }

    def compute(split: Partition, context: TaskContext): Iterator[Int] = {
      val i = input.compute(split, context)
      val sum = if (push) nativeSumPush(i.toArray) else nativeSumPull(i)
      Seq(sum).iterator
      /*
      var jobResult = 0
      val sumPartition = (iter: Iterator[Row]) => nativeSum(i.toArray)
      val mergeResults = (index: Int, taskResult: Int) => jobResult = jobResult + taskResult
      sc.runJob(this, sumPartition, mergeResults)
      */
    }
      

    protected def getPartitions: Array[Partition] = input.getPartitions
    protected def getDependencies: Seq[Dependency[_]] = input.getDependencies

    @native def nativeSumPush(arr: Array[Row]):Int
    @native def nativeSumPull(iter: Iterator[Row]):Int
}