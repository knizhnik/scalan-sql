/*****************************************
  Emitting Generated Code                  
*******************************************/
class sqlAggJoin extends ((scala.Tuple2[Array[scala.Tuple2[Int, Int]], scala.Tuple2[Array[scala.Tuple2[Int, Int]], scala.Tuple2[Array[scala.Tuple2[Int, Int]], scala.Tuple2[Array[scala.Tuple2[Int, Int]], scala.Tuple2[Array[scala.Tuple2[Int, Int]], Int]]]]])=>(Int)) {
    def now: Long = java.lang.System.currentTimeMillis()
	def apply(x0: scala.Tuple2[Array[scala.Tuple2[Int, Int]], scala.Tuple2[Array[scala.Tuple2[Int, Int]], scala.Tuple2[Array[scala.Tuple2[Int, Int]], scala.Tuple2[Array[scala.Tuple2[Int, Int]], scala.Tuple2[Array[scala.Tuple2[Int, Int]], Int]]]]]): Int = {
        var start: Long = now
		val x7 = new java.util.HashMap[Int, scala.Tuple2[Int, Int]]()
		val x12 = scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]()
		val x13 = ("t2", x12)
		val x17 = { x14: (scala.Tuple2[Int, Int]) =>
			val x15 = x14._1
			x15: Int
		}
		val x18 = (x7, x17)
		val x19 = (x13, x18)
		val x20 = ("t2.pk", x19)
		val x39 = { x21: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]], scala.Tuple2[Int, Int]]) =>
			val x23 = x21._2
			val x24 = x23._1
			val x29 = { x26: (Unit) =>
				val x27 = x7.put(x24, x23)
				x7: Unit
			}
			val x30 = x7.containsKey(x24)
			val x36 = if (x30) {
				val x31 = x7.get(x24)
				val x32 = throw new Exception("Unique constraint violation")
				x32
			} else {
				val x34 = x7.put(x24, x23)
				x7
			}
			val x37 = x12 += x23
			x20: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]]
		}
		val x52 = scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]()
		val x54 = new java.util.HashMap[Int, scala.Tuple2[Int, Int]]()
		val x62 = scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]()
		val x68 = new java.util.HashMap[Int, scala.Tuple2[Int, Int]]()
		val x67 = ("t5", x62)
		val x72 = { x69: (scala.Tuple2[Int, Int]) =>
			val x70 = x69._1
			x70: Int
		}
		val x73 = (x68, x72)
		val x74 = (x67, x73)
		val x75 = ("t5.pk", x74)
		val x94 = { x76: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]], scala.Tuple2[Int, Int]]) =>
			val x78 = x76._2
			val x79 = x78._1
			val x84 = { x81: (Unit) =>
				val x82 = x68.put(x79, x78)
				x68: Unit
			}
			val x85 = x68.containsKey(x79)
			val x91 = if (x85) {
				val x86 = x68.get(x79)
				val x87 = throw new Exception("Unique constraint violation")
				x87
			} else {
				val x89 = x68.put(x79, x78)
				x68
			}
			val x92 = x62 += x78
			x75: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]]
		}
		val x53 = ("t3", x52)
		val x58 = { x55: (scala.Tuple2[Int, Int]) =>
			val x56 = x55._1
			x56: Int
		}
		val x59 = (x54, x58)
		val x60 = (x53, x59)
		val x61 = ("t3.pk", x60)
		val x117 = { x99: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]], scala.Tuple2[Int, Int]]) =>
			val x101 = x99._2
			val x102 = x101._1
			val x107 = { x104: (Unit) =>
				val x105 = x54.put(x102, x101)
				x54: Unit
			}
			val x108 = x54.containsKey(x102)
			val x114 = if (x108) {
				val x109 = x54.get(x102)
				val x110 = throw new Exception("Unique constraint violation")
				x110
			} else {
				val x112 = x54.put(x102, x101)
				x54
			}
			val x115 = x52 += x101
			x61: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]]
		}
		val x118 = scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]()
		val x120 = new java.util.HashMap[Int, scala.Tuple2[Int, Int]]()
		val x119 = ("t4", x118)
		val x124 = { x121: (scala.Tuple2[Int, Int]) =>
			val x122 = x121._1
			x122: Int
		}
		val x125 = (x120, x124)
		val x126 = (x119, x125)
		val x127 = ("t4.pk", x126)
		val x150 = { x132: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]], scala.Tuple2[Int, Int]]) =>
			val x134 = x132._2
			val x135 = x134._1
			val x140 = { x137: (Unit) =>
				val x138 = x120.put(x135, x134)
				x120: Unit
			}
			val x141 = x120.containsKey(x135)
			val x147 = if (x141) {
				val x142 = x120.get(x135)
				val x143 = throw new Exception("Unique constraint violation")
				x143
			} else {
				val x145 = x120.put(x135, x134)
				x120
			}
			val x148 = x118 += x134
			x127: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]]
		}
		val x486 = x0._2
		val x488 = x486._2
		val x490 = x488._2
		val x492 = x490._2
		val x494 = x492._2
		val x485 = x0._1
		val x503 = x485.length
		val x569 = { x533: (Int) =>
			val x550 = scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]()
			val x549 = java.lang.String.valueOf(x533)
			val x551 = (x549, x550)
			val x557 = { x552: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[Int, Int]]) =>
				val x554 = x552._2
				val x555 = x550 += x554
				x551: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]]
			}
			var x558: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]] = x551
			var x548_buf = new scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]
			for (x541 <- 0 until x503) {
				val x542 = x485.apply(x541)
				val x543 = x542._1
				val x545 = x543.hashCode
				val x546 = x545 % x494
				val x547 = x546 == x533
				if (x547) x548_buf += x542
			}
			val x548 = x548_buf.toArray
			val x566 = x548.foreach {
				x559 =>
					val x560 = x558
					val x561 = x560.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]]]
					val x563 = x550 += x559
					x558 = x551

					()
			}
			val x567 = x558
			x567: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]]
		}
		// generating parallel execute
		val x570 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x494) yield scala.concurrent.future {
				x569(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		var x571: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]] = x20
		val x487 = x486._1
		val x592 = x487.foreach {
			x572 =>
				val x573 = x571
				val x574 = x573.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]]]
				val x576 = x572._1
				val x581 = { x578: (Unit) =>
					val x579 = x7.put(x576, x572)
					x7: Unit
				}
				val x582 = x7.containsKey(x576)
				val x588 = if (x582) {
					val x583 = x7.get(x576)
					val x584 = throw new Exception("Unique constraint violation")
					x584
				} else {
					val x586 = x7.put(x576, x572)
					x7
				}
				val x589 = x12 += x572
				x571 = x20

				()
		}
		val x593 = x571
		val x594 = x593._1
		val x595 = x593._2
		var x600: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]] = x61
		val x489 = x488._1
		val x621 = x489.foreach {
			x601 =>
				val x602 = x600
				val x603 = x602.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]]]
				val x605 = x601._1
				val x610 = { x607: (Unit) =>
					val x608 = x54.put(x605, x601)
					x54: Unit
				}
				val x611 = x54.containsKey(x605)
				val x617 = if (x611) {
					val x612 = x54.get(x605)
					val x613 = throw new Exception("Unique constraint violation")
					x613
				} else {
					val x615 = x54.put(x605, x601)
					x54
				}
				val x618 = x52 += x601
				x600 = x61

				()
		}
		val x622 = x600
		val x623 = x622._1
		val x624 = x622._2
		var x629: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]] = x127
		val x491 = x490._1
		val x650 = x491.foreach {
			x630 =>
				val x631 = x629
				val x632 = x631.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]]]
				val x634 = x630._1
				val x639 = { x636: (Unit) =>
					val x637 = x120.put(x634, x630)
					x120: Unit
				}
				val x640 = x120.containsKey(x634)
				val x646 = if (x640) {
					val x641 = x120.get(x634)
					val x642 = throw new Exception("Unique constraint violation")
					x642
				} else {
					val x644 = x120.put(x634, x630)
					x120
				}
				val x647 = x118 += x630
				x629 = x127

				()
		}
		val x651 = x629
		val x652 = x651._1
		val x653 = x651._2
		var x658: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]] = x75
		val x493 = x492._1
		val x679 = x493.foreach {
			x659 =>
				val x660 = x658
				val x661 = x660.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, Int]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, Int]], scala.Function1[scala.Tuple2[Int, Int], Int]]]]]
				val x663 = x659._1
				val x668 = { x665: (Unit) =>
					val x666 = x68.put(x663, x659)
					x68: Unit
				}
				val x669 = x68.containsKey(x663)
				val x675 = if (x669) {
					val x670 = x68.get(x663)
					val x671 = throw new Exception("Unique constraint violation")
					x671
				} else {
					val x673 = x68.put(x663, x659)
					x68
				}
				val x676 = x62 += x659
				x658 = x75

				()
		}
        println("Elapsed time for insert=" + (now - start))
        java.lang.System.gc()

        for (i <- 1 until 20) {
        start = now

		val x680 = x658
		val x681 = x680._1
		val x682 = x680._2
		var x810: Int = 0
		val x597 = x595._2
		val x598 = x597._1
		val x626 = x624._2
		val x627 = x626._1
		val x655 = x653._2
		val x656 = x655._1
		val x684 = x682._2
		val x685 = x684._1
		val x808 = { x757: (Int) =>
			val x758 = x570.apply(x757)
			val x759 = x758._1
			val x760 = x758._2
			var x790: Int = 0
			val x761 = x760.toArray
			val x762 = x761.length
			var x832_buf = new scala.collection.mutable.ArrayBuffer[scala.Tuple2[scala.Tuple2[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, Int], scala.Tuple2[Int, Int]], scala.Tuple2[Int, Int]], scala.Tuple2[Int, Int]], scala.Tuple2[Int, Int]]]
			for (x763 <- 0 until x762) {
				val x764 = x761.apply(x763)
				val x765 = x764._1
				val x767 = x765 % 3
				val x768 = x767 == 0
				val x766 = x764._2
				val x818 = x598.get(x766)
				val x819 = (x764, x818)
				val x820 = x818._2
				val x821 = x627.get(x820)
				val x822 = (x819, x821)
				val x823 = x821._2
				val x824 = x656.get(x823)
				val x825 = (x822, x824)
				val x826 = x824._2
				val x827 = x685.get(x826)
				val x828 = (x825, x827)
				if (x768) x832_buf += x828
			}
			val x832 = x832_buf.toArray
			val x833 = x832.foreach {
				x791 =>
					val x792 = x790
					val x796 = x791._2
					val x798 = x796._2
					val x799 = x798 % 3
					val x800 = x799 == 0
					val x802 = if (x800) {
						val x793 = x792.asInstanceOf[Int]
						val x801 = x793 + x798
						x801
					} else {
						val x793 = x792.asInstanceOf[Int]
						x793
					}
					x790 = x802

					()
			}
			val x834 = x790
			val x806 = x834
			x806: Int
		}
		// generating parallel execute
		val x809 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x494) yield scala.concurrent.future {
				x808(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x814 = x809.foreach {
			x811 =>
				val x812 = x810 += x811

				()
		}
		val x815 = x810
        println("Elapsed time for query=" + (now - start))
		x815
        }
        0
	}
}
/*****************************************
  End of Generated Code                  
*******************************************/
val benchmark = new sqlAggJoin()
val nRecordsLog = 24
val nRecords = 1 << nRecordsLog
val t1 = Array.tabulate[scala.Tuple2[Int, Int]](nRecords)(i => (i, java.lang.Integer.reverse(i) >>> (32 - nRecordsLog)))
val t2 = Array.tabulate[scala.Tuple2[Int, Int]](nRecords)(i => (java.lang.Integer.reverse(i) >>> (32 - nRecordsLog), i))
println(benchmark((t1, (t2, (t1, (t2, (t1, 4))))))) 
