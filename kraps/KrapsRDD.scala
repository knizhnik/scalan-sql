package kraps

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._, catalyst.InternalRow, types.StructType
import java.net._
import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.annotation.meta.param

class RowIterator(
  input: RDD[InternalRow],
  partitions: Array[Partition],
  index: Int,
  nNodes: Int,
  context: TaskContext,
  serialize: (Long, InternalRow) => Int) {

  var iter: Iterator[InternalRow] = null
  var i = index

  def nextTile(row: Long, tileSize: Int): Int = {
    var j = 0
    var done = false
    while (j != tileSize && !done) {
      while ((iter == null || !iter.hasNext) && i < partitions.length) {
        iter = input.compute(partitions(i), context)
        partitions(i) = null
        i = i + nNodes
      }
      if (iter != null && iter.hasNext) {
        val rowSize = serialize(row, iter.next)
        if (rowSize != 0) {
          row += rowSize
          j += 1	  
      	} else {
          done = true
	    }
      } else {
        done = true
      }
    }
    j
  }
  
  def next(row: Long): Boolean = {
    while ((iter == null || !iter.hasNext) && i < partitions.length) {
      iter = input.compute(partitions(i), context)
      partitions(i) = null
      i = i + nNodes
    }
    if (iter != null && iter.hasNext) {
      serialize(row, iter.next) != 0
    } else {
      false
    }
  }
}

case class CombinePartition(index: Int, parts: Array[Array[Partition]]) extends Partition

object KrapsCluster {
  var port = 54321
  var masterAddress:String = null
  val clusterMap = new Map[Thread,Long]

  def configure(nWorkers: Int): String = {
    if (masterAddress == null) {
      val server = new ServerSocket(port)
      val address = server.getInetAddress().getHostName() + ":" + port
      val t = new Thread(new Runnable {
        def run() {
          val sockets = Array.tabulate(nWorkers)(i => server.accept())
          val hosts = sockets.map(s => s.getInetAddress().getHostName())
          for (i <- 0 until sockets.size) { 
            val out = new DataOutputStream(sockets(i).getOutputStream())
            out.writeInt(i)
            out.writeInt(hosts.size)
            hosts.foreach(h => out.writeUTF(h))
            out.close()
          }
          server.close()
        }
      })
      t.start
      masterAddress = address
    }
	masterAddress
  }
 
  def start(driver: String): Long = {
    var cluster = clusterMap.getOrElse(0)
    if (cluster == 0) {     
      val col = driver.indexOf(':')
      val host = driver.substring(0, col)
      val port = Integer.parseInt(driver.substring(col+1))
      val s = new Socket(InetAddress.getByName(host), port)
      val in = new DataInputStream(s.getInputStream())
      val nodeId = in.readInt()
      val nNodes = in.readInt()
      val hosts = Array.tabulate(nNodes)(i => in.readUTF())
      s.close()
      cluster = start(hosts, nodeId)
	  clusterMap(Thread.currentThread()) = cluster
    }
    cluster
  }

  def stop(): Unit = {
    for (cluster <- clusterMap.values) {
      stop(cluster)
    }
  }
  
  @native def start(hosts: Array[String], nodeId: Int): Long
  @native def stop(cluster:Long): Unit
}

class KrapsRDD(
  @(transient @param) sc: SparkContext,
  queryId: Int,
  nNodes: Int,
  schema: StructType,
  input: Array[RDD[InternalRow]],
  serializers: Array[(Long, InternalRow) => Int],
  deserializer: (Long, StructType) => InternalRow)
    extends RDD[InternalRow](sc, input.map(rdd => new OneToOneDependency(rdd))) {

  var cluster:Long = 0
  val krapsDriver = KrapsCluster.configure(nNodes)

  protected def getPartitions: Array[Partition] = {
    val parts: Array[Array[Partition]] = input.map(_.partitions)
    Array.tabulate(nNodes)(n => CombinePartition(n, parts))
  }

  class KrapsIterator(krapsInput: Long, scalaInput : Array[RowIterator]) extends Iterator[InternalRow] {
    var row: Long = 0
    var eof: Boolean = false

    def hasNext: Boolean = {
      if (eof) {
         false
      } else {
         if (row == 0) row = nextRow(cluster, krapsInput, scalaInput)
         if (row == 0) {
//         Cluster should be explictly stopped by application
//         KrapsCluster.stop(cluster)
           eof = true
           false
         } else {
           true
         }
      }
    }

    def next: InternalRow = {
      if (row == 0) throw new java.lang.IllegalStateException()
      val curr = row
      row = 0
      deserializer(curr, schema)
    }
  }

  def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    cluster = KrapsCluster.start(krapsDriver)
    logInfo("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! compute !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    val s = split.asInstanceOf[CombinePartition]
    val coalescedInput = Array.tabulate(input.size)(i =>
        new RowIterator(input(i), s.parts(i), split.index, nNodes, context, serializers(i)))
    val it = createIterator(cluster, queryId, coalescedInput)
    //logInfo(s"!!!!!!!!!!!!!!!!!!!!!!!!!!!!! KrapsIterator($it) !!!!!!!!!!!!!!!!!!!")
    new KrapsIterator(it, coalescedInput)
  }

  @native def createIterator(cluster: Long, queryId: Int, scalaInput : Array[RowIterator]): Long
  @native def nextRow(cluster: Long, iterator: Long, scalaInput : Array[RowIterator]): Long
}

object Test extends App {

  System.loadLibrary("krapsrdd")

  KrapsCluster.start(Array("localhost"), 0)
  println("start")

  Thread.sleep(500)
  println("sleep")

  KrapsCluster.stop()
  println("stop")
}
