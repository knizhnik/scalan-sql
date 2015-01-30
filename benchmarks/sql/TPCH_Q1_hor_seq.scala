/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q1_hor_seq extends ((Array[Array[java.lang.String]])=>(Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]])) {
  def apply(x0: Array[Array[java.lang.String]]): Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]] = {
    val x517 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]()
    val x428 = x0.length
    var x645_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    for (x429 <- 0 until x428) {
      val x430 = x0.apply(x429)
      val x431 = x430.apply(0)
      val x432 = x431.toInt
      val x433 = x430.apply(1)
      val x434 = x433.toInt
      val x435 = x430.apply(2)
      val x436 = x435.toInt
      val x437 = x430.apply(3)
      val x438 = x437.toInt
      val x439 = x430.apply(4)
      val x440 = x439.toDouble
      val x441 = x430.apply(5)
      val x442 = x441.toDouble
      val x443 = x430.apply(6)
      val x444 = x443.toDouble
      val x445 = x430.apply(7)
      val x446 = x445.toDouble
      val x447 = x430.apply(8)
      val x448 = x447.charAt(0)
      val x449 = x430.apply(9)
      val x450 = x449.charAt(0)
      val x458 = x430.apply(11)
      val x459 = x458.substring(0, 4)
      val x460 = x458.substring(5, 7)
      val x461 = x459 + x460
      val x462 = x458.substring(8, 10)
      val x463 = x461 + x462
      val x464 = x463.toInt
      val x465 = x430.apply(12)
      val x466 = x465.substring(0, 4)
      val x467 = x465.substring(5, 7)
      val x468 = x466 + x467
      val x469 = x465.substring(8, 10)
      val x470 = x468 + x469
      val x471 = x470.toInt
      val x472 = x430.apply(13)
      val x473 = x430.apply(14)
      val x474 = x430.apply(15)
      val x475 = (x473, x474)
      val x476 = (x472, x475)
      val x477 = (x471, x476)
      val x478 = (x464, x477)
      val x451 = x430.apply(10)
      val x452 = x451.substring(0, 4)
      val x453 = x451.substring(5, 7)
      val x454 = x452 + x453
      val x455 = x451.substring(8, 10)
      val x456 = x454 + x455
      val x457 = x456.toInt
      val x479 = (x457, x478)
      val x480 = (x450, x479)
      val x481 = (x448, x480)
      val x482 = (x446, x481)
      val x483 = (x444, x482)
      val x484 = (x442, x483)
      val x485 = (x440, x484)
      val x486 = (x438, x485)
      val x487 = (x436, x486)
      val x488 = (x434, x487)
      val x489 = (x432, x488)
      val x644 = x457 <= 19981201
      if (x644) x645_buf += x489
    }
    val x645 = x645_buf.result

    java.lang.System.gc()
    println("Start query")
    val start = now

    val x646 = x645.foreach {
      x518 =>
        val x520 = x518._2
        val x522 = x520._2
        val x524 = x522._2
        val x526 = x524._2
        val x528 = x526._2
        val x530 = x528._2
        val x532 = x530._2
        val x534 = x532._2
        val x535 = x534._1
        val x536 = x534._2
        val x537 = x536._1
        val x539 = (x535, x537)
        val x553 = x517.containsKey(x539)
        val x586 = if (x553) {
          val x554 = x517.get(x539)
          val x556 = x554._1
          val x557 = x554._2
          val x527 = x526._1
          val x558 = x556 + x527
          val x529 = x528._1
          val x559 = x557._1
          val x561 = x559 + x529
          val x531 = x530._1
          val x540 = 1.0 - x531
          val x541 = x529 * x540
          val x560 = x557._2
          val x562 = x560._1
          val x564 = x562 + x541
          val x533 = x532._1
          val x542 = 1.0 + x533
          val x543 = x540 * x542
          val x544 = x529 * x543
          val x563 = x560._2
          val x565 = x563._1
          val x567 = x565 + x544
          val x566 = x563._2
          val x568 = x566._1
          val x570 = x568 + x527
          val x569 = x566._2
          val x571 = x569._1
          val x573 = x571 + x529
          val x572 = x569._2
          val x574 = x572._1
          val x576 = x574 + x531
          val x575 = x572._2
          val x577 = x575 + 1
          val x578 = (x576, x577)
          val x579 = (x573, x578)
          val x580 = (x570, x579)
          val x581 = (x567, x580)
          val x582 = (x564, x581)
          val x583 = (x561, x582)
          val x584 = (x558, x583)
          x584
        } else {
          val x527 = x526._1
          val x529 = x528._1
          val x531 = x530._1
          val x540 = 1.0 - x531
          val x541 = x529 * x540
          val x533 = x532._1
          val x542 = 1.0 + x533
          val x543 = x540 * x542
          val x544 = x529 * x543
          val x545 = (x531, 1)
          val x546 = (x529, x545)
          val x547 = (x527, x546)
          val x548 = (x544, x547)
          val x549 = (x541, x548)
          val x550 = (x529, x549)
          val x551 = (x527, x550)
          x551
        }
        val x587 = x517.put(x539, x586)

        x587
    }
    val x647 = scala.collection.JavaConverters.asScalaSetConverter(x517.keySet).asScala.toIterable
    val x648 = x647.toArray
    val x649 = x648.length
    var x598 = new Array[scala.Tuple2[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]](x649)
    for (x593 <- 0 until x649) {
      val x650 = x648.apply(x593)
      val x651 = x517.get(x650)
      val x652 = (x650, x651)
      x598(x593) = x652
    }
    val x654 = x598.length
    val x641 = { x635: (scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]) =>
      val x636 = x635._1
      val x637 = x635._2
      val x638 = x637._1
      val x640 = (x636, x638)
      x640: scala.Tuple2[Char, Char]
    }
    var x655 = new Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]](x654)
    for (x600 <- 0 until x654) {
      val x601 = x598.apply(x600)
      val x602 = x601._1
      val x603 = x601._2
      val x604 = x602._1
      val x605 = x602._2
      val x606 = x603._1
      val x607 = x603._2
      val x608 = x607._1
      val x609 = x607._2
      val x610 = x609._1
      val x611 = x609._2
      val x612 = x611._1
      val x613 = x611._2
      val x614 = x613._1
      val x615 = x613._2
      val x617 = x615._2
      val x619 = x617._2
      val x620 = x619.toDouble
      val x621 = x614 / x620
      val x616 = x615._1
      val x622 = x616 / x620
      val x618 = x617._1
      val x623 = x618 / x620
      val x624 = (x623, x619)
      val x625 = (x622, x624)
      val x626 = (x621, x625)
      val x627 = (x612, x626)
      val x628 = (x610, x627)
      val x629 = (x608, x628)
      val x630 = (x606, x629)
      val x631 = (x605, x630)
      val x632 = (x604, x631)
      x655(x600) = x632
    }
    val x656 = x655.sortBy(x641)
    val x642 = x656
    println("Elapsed time: " + (now - start))   
    x642
  }
}
/*****************************************
  End of Generated Code                  
*******************************************/
def now: Long = java.lang.System.currentTimeMillis()
val in = scala.io.Source.fromFile("/home/knizhnik/tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray
val benchmark = new TPCH_Q1_hor_seq()
val result = benchmark(in)
