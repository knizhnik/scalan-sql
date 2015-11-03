package kraps

import java.net._
import java.io._


import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._, types.StructType

import scala.collection.mutable.ArrayBuffer
import scala.annotation.meta.param
import scala.concurrent.ExecutionContext.implicits.global


class RowIterator(
  input: RDD[Row],
  partitions: Array[Partition],
  index: Int,
  nNodes: Int,
  context: TaskContext,
  serialize: (Long, Row) => Int) {

  var iter: Iterator[Row] = null
  var i = index

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
  def configure(port: Int, nWorkers: Int): String = {
    val server = new ServerSocket(port)
    val address = server.getInetAddress().getHostAddress() + ":" + port
    yield scala.concurrent.future {
      val sockets = Array.tabulate(nWorkers)(i => server.accept())
      hosts = sockets.map(s => s.getInetAddress().getHostAddress())
      sockets.map(s =>       
        val out = new DataOutputStream(s.getOutputStream())
        out.writeInt(hosts.size)
        hosts.map(h => out.writeUTF(h))
        s.close())
      server.close()
    }
    address 
  }
 
  def start(driver: String): Unit = {
     val col = driver.indexOf(':')
     val host = driver.substring(0, col)
     val port = Integer.parseInt(drive.substring(col+1))
     val s = new Socket(InetAddress.getByName(host), port)
     val in = new DataInputStream(s.getInputStream())
     val hosts = Array.tabulate(in.readInt())(i => in.readUTF())
     s.close()
     start(hosts, 1)
  }
  
  @native def start(hosts: Array[String], nCores: Int): Unit
  @native def stop(): Unit
}

class KrapsRDD(
  @(transient @param) sc: SparkContext,
  queryId: Int,
  nNodes: Int,
  schema: StructType,
  input: Array[RDD[Row]],
  serializers: Array[(Long, Row) => Int],
  deserializer: (Long, StructType) => Row)
    extends RDD[Row](sc, input.map(rdd => new OneToOneDependency(rdd))) {

  val krapsDriver = KrapsCluster.configure(54321, nNodes)

  protected def getPartitions: Array[Partition] = {
    val parts: Array[Array[Partition]] = input.map(_.partitions)
    Array.tabulate(nNodes)(n => CombinePartition(n, parts))
  }

  class KrapsIterator(krapsInput: Long, scalaInput : Array[RowIterator]) extends Iterator[Row] {
    var row: Long = 0
    var eof: Boolean = false

    def hasNext: Boolean = {
      if (eof) {
         false
      } else {
         if (row == 0) row = nextRow(krapsInput, scalaInput)
         if (row == 0) {
           eof = true
           false
         } else {
           true
         }
      }
    }

    def next: Row = {
      if (row == 0) throw new java.lang.IllegalStateException()
      val curr = row
      row = 0
      deserializer(curr, schema)
    }
  }

  def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    KrapsCluster.start(krapsDriver)
    logInfo("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! compute !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    val s = split.asInstanceOf[CombinePartition]
    val coalescedInput = Array.tabulate(input.size)(i =>
        new RowIterator(input(i), s.parts(i), split.index, nNodes, context, serializers(i)))
    val it = createIterator(queryId)
    logInfo(s"!!!!!!!!!!!!!!!!!!!!!!!!!!!!! KrapsIterator($it) !!!!!!!!!!!!!!!!!!!")
    new KrapsIterator(it,coalescedInput)
  }

  @native def createIterator(queryId: Int): Long
  @native def nextRow(iterator: Long, scalaInput : Array[RowIterator]): Long
}

object Test extends App {

  System.loadLibrary("krapsrdd")

  KrapsCluster.start(Array("localhost"), 4)
  println("start")

  Thread.sleep(500)
  println("sleep")

  KrapsCluster.stop()
  println("stop")
}
