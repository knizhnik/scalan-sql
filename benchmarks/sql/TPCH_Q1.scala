def now: Long = java.lang.System.currentTimeMillis()
/*****************************************
  Emitting Generated Code                  
*******************************************/
class tpchQ1 extends ((Array[Array[java.lang.String]])=>(Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]])) {
  def apply(x0: Array[Array[java.lang.String]]): Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]] = {
    val x552 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]()
    val x396 = x0.length
    var x781_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    for (x399 <- 0 until x396) {
      val x662 = x0.apply(x399)
      val x663 = x662.apply(0)
      val x664 = x663.toInt
      val x665 = x662.apply(1)
      val x666 = x665.toInt
      val x667 = x662.apply(2)
      val x668 = x667.toInt
      val x669 = x662.apply(3)
      val x670 = x669.toInt
      val x671 = x662.apply(4)
      val x672 = x671.toDouble
      val x673 = x662.apply(5)
      val x674 = x673.toDouble
      val x675 = x662.apply(6)
      val x676 = x675.toDouble
      val x677 = x662.apply(7)
      val x678 = x677.toDouble
      val x679 = x662.apply(8)
      val x680 = x679.charAt(0)
      val x681 = x662.apply(9)
      val x682 = x681.charAt(0)
      val x690 = x662.apply(11)
      val x691 = x690.substring(0, 4)
      val x692 = x690.substring(5, 7)
      val x693 = x691 + x692
      val x694 = x690.substring(8, 10)
      val x695 = x693 + x694
      val x696 = x695.toInt
      val x697 = x662.apply(12)
      val x698 = x697.substring(0, 4)
      val x699 = x697.substring(5, 7)
      val x700 = x698 + x699
      val x701 = x697.substring(8, 10)
      val x702 = x700 + x701
      val x703 = x702.toInt
      val x704 = x662.apply(13)
      val x705 = x662.apply(14)
      val x706 = x662.apply(15)
      val x707 = (x705, x706)
      val x708 = (x704, x707)
      val x709 = (x703, x708)
      val x710 = (x696, x709)
      val x683 = x662.apply(10)
      val x684 = x683.substring(0, 4)
      val x685 = x683.substring(5, 7)
      val x686 = x684 + x685
      val x687 = x683.substring(8, 10)
      val x688 = x686 + x687
      val x689 = x688.toInt
      val x711 = (x689, x710)
      val x712 = (x682, x711)
      val x713 = (x680, x712)
      val x714 = (x678, x713)
      val x715 = (x676, x714)
      val x716 = (x674, x715)
      val x717 = (x672, x716)
      val x718 = (x670, x717)
      val x719 = (x668, x718)
      val x720 = (x666, x719)
      val x721 = (x664, x720)
      val x780 = x689 <= 19980811
      if (x780) x781_buf += x721
    }
    val x781 = x781_buf.result

    java.lang.System.gc()
    println("Start query")
    val start = now

    val x782 = x781.foreach {
      x553 =>
        val x555 = x553._2
        val x557 = x555._2
        val x559 = x557._2
        val x561 = x559._2
        val x563 = x561._2
        val x565 = x563._2
        val x567 = x565._2
        val x569 = x567._2
        val x570 = x569._1
        val x571 = x569._2
        val x572 = x571._1
        val x574 = (x570, x572)
        val x585 = x552.containsKey(x574)
        val x610 = if (x585) {
          val x586 = x552.get(x574)
          val x588 = x586._1
          val x589 = x586._2
          val x562 = x561._1
          val x590 = x588 + x562
          val x564 = x563._1
          val x591 = x589._1
          val x593 = x591 + x564
          val x566 = x565._1
          val x575 = 1.0 - x566
          val x576 = x564 * x575
          val x592 = x589._2
          val x594 = x592._1
          val x596 = x594 + x576
          val x568 = x567._1
          val x577 = x568 + 1.0
          val x578 = x576 * x577
          val x595 = x592._2
          val x597 = x595._1
          val x599 = x597 + x578
          val x598 = x595._2
          val x600 = x598._1
          val x602 = x600 + x566
          val x601 = x598._2
          val x603 = x601 + 1
          val x604 = (x602, x603)
          val x605 = (x599, x604)
          val x606 = (x596, x605)
          val x607 = (x593, x606)
          val x608 = (x590, x607)
          x608
        } else {
          val x562 = x561._1
          val x564 = x563._1
          val x566 = x565._1
          val x575 = 1.0 - x566
          val x576 = x564 * x575
          val x568 = x567._1
          val x577 = x568 + 1.0
          val x578 = x576 * x577
          val x579 = (x566, 1)
          val x580 = (x578, x579)
          val x581 = (x576, x580)
          val x582 = (x564, x581)
          val x583 = (x562, x582)
          x583
        }
        val x611 = x552.put(x574, x610)

        x611
    }
    val x783 = scala.collection.JavaConverters.asScalaSetConverter(x552.keySet).asScala.toIterable
    val x784 = x783.toArray
    val x785 = x784.length
    var x790 = new Array[scala.Tuple2[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]](x785)
    for (x617 <- 0 until x785) {
      val x786 = x784.apply(x617)
      val x787 = x552.get(x786)
      val x788 = (x786, x787)
      x790(x617) = x788
    }
    val x626 = { x623: (scala.Tuple2[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]) =>
      val x624 = x623._1
      x624: scala.Tuple2[Char, Char]
    }
    val x791 = x790.sortBy(x626)
    val x792 = x791.length
    var x823 = new Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]](x792)
    for (x629 <- 0 until x792) {
      val x793 = x791.apply(x629)
      val x794 = x793._1
      val x795 = x793._2
      val x796 = x794._1
      val x797 = x794._2
      val x798 = x795._1
      val x799 = x795._2
      val x800 = x799._1
      val x801 = x799._2
      val x802 = x801._1
      val x803 = x801._2
      val x804 = x803._1
      val x805 = x803._2
      val x806 = x805._2
      val x807 = x806.toFloat
      val x808 = x807.toDouble
      val x809 = x798 / x808
      val x810 = x800 / x808
      val x811 = x805._1
      val x812 = x811 / x808
      val x813 = (x812, x806)
      val x814 = (x810, x813)
      val x815 = (x809, x814)
      val x816 = (x804, x815)
      val x817 = (x802, x816)
      val x818 = (x800, x817)
      val x819 = (x798, x818)
      val x820 = (x797, x819)
      val x821 = (x796, x820)
      x823(x629) = x821
    }
    val x660 = x823
    println("Elapsed time: " + (now - start))   
    x660
  }
}
/*****************************************
  End of Generated Code                  
*******************************************/
val in = scala.io.Source.fromFile("/home/knizhnik/tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray
val benchmark = new tpchQ1()
val result = benchmark(in)
