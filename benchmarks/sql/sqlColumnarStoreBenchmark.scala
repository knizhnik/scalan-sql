/*****************************************
  Emitting Generated Code                  
*******************************************/
class sqlColumnarStoreBenchmark extends ((scala.Tuple2[Int, Array[scala.Tuple2[Int, java.lang.String]]])=>(scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, Int]]])) {
    def now: Long = java.lang.System.currentTimeMillis()
	def apply(x0: scala.Tuple2[Int, Array[scala.Tuple2[Int, java.lang.String]]]): scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, Int]]] = {
        var start: Long = now

		var x603: Int = 0
		val x398 = x0._1
		val x399 = x0._2
		val x408 = x399.length
		val x550 = { x476: (Int) =>
			val x500 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x498 = java.lang.String.valueOf(x476)
			val x499 = "left" + x498
			val x501 = (x499, x500)
			val x507 = { x502: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
				val x504 = x502._2
				val x505 = x500 += x504
				x501: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
			}
			var x508: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x501
			var x786_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			var x791_buf = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			for (x484 <- 0 until x408) {
				val x485 = x399.apply(x484)
				val x486 = x485._1
				val x488 = x486.hashCode
				val x489 = x488 % x398
				val x490 = x489 == x476
				val x487 = x485._2
				if (x490) x786_buf += x486
				if (x490) x791_buf += x487
			}
			val x786 = x786_buf.result
			val x791 = x791_buf.result
			val x787 = x786.foreach {
				x509 =>
					val x510 = x508
					val x511 = x510.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
					val x513 = x500 += x509
					x508 = x501

					()
			}
			val x788 = x508
			val x789 = x788._1
			val x790 = x788._2
			val x527 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x526 = "right" + x498
			val x528 = (x526, x527)
			val x534 = { x529: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], java.lang.String]) =>
				val x531 = x529._2
				val x532 = x527 += x531
				x528: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]
			}
			var x535: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]] = x528
			val x792 = x791.foreach {
				x536 =>
					val x537 = x535
					val x538 = x537.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]
					val x540 = x527 += x536
					x535 = x528

					()
			}
			val x793 = x535
			val x794 = x793._1
			val x795 = x793._2
			val x796 = x790.result
			val x797 = x795.result
			val x798 = (x796, x797)
			val x548 = x798
			x548: scala.Tuple2[Array[Int], Array[java.lang.String]]
		}
		// generating parallel execute
		val x551 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x398) yield scala.concurrent.future {
				x550(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
        println("Elapsed time for insert=" + (now - start))
        for (i <- 1 until 20){
        java.lang.System.gc() 
        println("-------------------------------")
        start = now

		val x601 = { x577: (Int) =>
			var x587: Int = 0
			val x578 = x551.apply(x577)
			val x579 = x578._1
			val x581 = x579.length
			val x580 = x578._2
			var x586 = new Array[scala.Tuple2[Int, java.lang.String]](x581)
			for (x582 <- 0 until x581) {
				val x583 = x579.apply(x582)
				val x584 = x580.apply(x582)
				val x585 = (x583, x584)
				x586(x582) = x585
			}
			val x598 = x586.foreach {
				x588 =>
					val x590 = x588._2
					val x591 = x590.contains("123")
					val x589 = x588._1
					val x592 = x589 > 0
					val x593 = x591 && x592
					val x596 = if (x593) {
						val x594 = x587 += 1
						()
					} else {
						()
					}

					x596
			}
			val x599 = x587
			x599: Int
		}
		// generating parallel execute
		val x602 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x398) yield scala.concurrent.future {
				x601(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x607 = x602.foreach {
			x604 =>
				val x605 = x603 += x604

				()
		}
		val x608 = x603

        println("elapsed time = " + (now - start))
        java.lang.System.gc()
        start = now

		var x632: Int = 0
		val x630 = { x620: (Int) =>
			val x621 = x551.apply(x620)
			val x623 = x621._2
			val x624 = x623.length
			var x628_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			for (x625 <- 0 until x624) {
				val x626 = x623.apply(x625)
				val x627 = x626.contains("123")
				if (x627) x628_buf += x625
			}
			val x628 = x628_buf.result
			val x629 = x628.length
			x629: Int
		}
		// generating parallel execute
		val x631 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x398) yield scala.concurrent.future {
				x630(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x636 = x631.foreach {
			x633 =>
				val x634 = x632 += x633

				()
		}
		val x637 = x632

        println("elapsed time = " + (now - start))
        java.lang.System.gc()
        start = now

		var x663: Int = 0
		val x661 = { x650: (Int) =>
			var x654: Int = 0
			val x651 = x551.apply(x650)
			val x652 = x651._1
			val x658 = x652.foreach {
				x655 =>
					val x656 = x654 += x655

					()
			}
			val x659 = x654
			x659: Int
		}
		// generating parallel execute
		val x662 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x398) yield scala.concurrent.future {
				x661(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x667 = x662.foreach {
			x664 =>
				val x665 = x663 += x664

				()
		}
		val x668 = x663

        println("elapsed time = " + (now - start))
        java.lang.System.gc()
        start = now

		var x762: Int = 0
		val x17 = (0, 0)
		val x760 = { x715: (Int) =>
			var x736: scala.Tuple2[Int, Int] = x17
			val x716 = x551.apply(x715)
			val x718 = x716._2
			val x730 = x718.length
			val x717 = x716._1
			val x755 = while ( {
				val x737 = x736
				val x738 = x737._1
				val x740 = x738 == x730
				val x741 = !x740
				x741
			}) {
				val x743 = x736
				val x744 = x743._1
				val x746 = x744 + 1
				val x747 = x718.apply(x744)
				val x748 = x747.startsWith("1")
				val x751 = if (x748) {
					val x745 = x743._2
					val x749 = x717.apply(x744)
					val x750 = x745 + x749
					x750
				} else {
					val x745 = x743._2
					x745
				}
				val x752 = (x746, x751)
				x736 = x752
				()
			}
			val x756 = x736
			val x758 = x756._2
			x758: Int
		}
		// generating parallel execute
		val x761 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x398) yield scala.concurrent.future {
				x760(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x766 = x761.foreach {
			x763 =>
				val x764 = x762 += x763

				()
		}
		val x767 = x762
		val x768 = (x668, x767)
		val x769 = (x637, x768)
		val x770 = (x608, x769)
		x770

        println("elapsed time = " + (now - start))
        }
        (0, (0, (0, 0)))
	}
}
/*****************************************
  End of Generated Code                  
*******************************************/
val benchmark = new sqlColumnarStoreBenchmark()
val nRecords = 50000000
val inputData =  Array.tabulate(nRecords)(i => (i+1, i.toString))
println(benchmark((1, inputData)))
