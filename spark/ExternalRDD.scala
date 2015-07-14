package com.huawei.hispark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._ 
import org.apache.spark.sql.catalyst.expressions.Row

class ExternalRDD(sc: SparkContext, input: RDD[Row], push: Boolean) extends RDD[Int](sc, input.dependencies)
{
    System.loadLibrary("nativerdd")

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
      

    class ExternalPartition(val index: Int) extends Partition {}
     
    protected def getPartitions: Array[Partition] = {
      val nCores = 16
      Array.tabulate(nCores)(i => new External Partition(i))
    }

    //    protected def getPartitions: Array[Partition] = input.partitions

    @native def nativeSumPush(arr: Array[Row]):Int
    @native def nativeSumPull(iter: Iterator[Row]):Int
}
