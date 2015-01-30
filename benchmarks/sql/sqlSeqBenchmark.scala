def now: Long = java.lang.System.currentTimeMillis()
/*****************************************
  Emitting Generated Code                  
*******************************************/
class sqlSeqBenchmark extends ((Array[scala.Tuple2[Int, java.lang.String]])=>(scala.Tuple2[Int, scala.Tuple2[Int, Int]])) {
  def apply(x0: Array[scala.Tuple2[Int, java.lang.String]]): scala.Tuple2[Int, scala.Tuple2[Int, Int]] = {
    var start: Long = now
    val x1 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, java.lang.String]]
    val x2 = ("t1", x1)
    val x19 = { x14: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, java.lang.String]]], scala.Tuple2[Int, java.lang.String]]) =>
      val x16 = x14._2
      val x17 = x1 += x16
      x2: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, java.lang.String]]]
    }
    var x82: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, java.lang.String]]] = x2
    val x90 = x0.foreach {
      x83 =>
        val x84 = x82
        val x85 = x84.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, java.lang.String]]]]
        val x87 = x1 += x83
        x82 = x2

        ()
    }
    val x91 = x82
    val x92 = x91._1
    val x93 = x91._2
    var x95: Int = 0
    val x94 = x93.result
    println("Elapsed time for insert=" + (now - start))
//    java.lang.System.gc()
    start = now
 
    val x106 = x94.foreach {
      x96 =>
        val x98 = x96._2
        val x99 = x98.contains("123")
        val x97 = x96._1
        val x100 = x97 > 0
        val x101 = x99 && x100
        val x104 = if (x101) {
          val x102 = x95 += 1
          ()
        } else {
          ()
        }

        x104
    }
    val x107 = x95
    println("elapsed time = " + (now - start))
    start = now
    var x108: Int = 0
    val x114 = x94.foreach {
      x109 =>
        val x110 = x109._1
        val x112 = x108 += x110

        ()
    }
    val x115 = x108
    println("elapsed time = " + (now - start))
    start = now
    var x116: Int = 0
    val x128 = x94.foreach {
      x117 =>
        val x118 = x116
        val x122 = x117._2
        val x123 = x122.startsWith("1")
        val x125 = if (x123) {
          val x119 = x118.asInstanceOf[Int]
          val x121 = x117._1
          val x124 = x119 + x121
          x124
        } else {
          val x119 = x118.asInstanceOf[Int]
          x119
        }
        x116 = x125

        ()
    }
    val x129 = x116
    val x130 = (x115, x129)
    val x131 = (x107, x130)
    println("elapsed time = " + (now - start))
    x131
  }
}
/*****************************************
  End of Generated Code                  
*******************************************/

val benchmark = new sqlSeqBenchmark()
val nRecords = 50000000
val inputData = new Array[scala.Tuple2[Int, java.lang.String]](nRecords)
for (i <- 0 until nRecords) {
  inputData.update(i, (i+1, i.toString))
}
println(benchmark(inputData))

