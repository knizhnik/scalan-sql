/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q1_ver_par extends ((Array[Array[java.lang.String]])=>(Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]])) {
	def apply(x0: Array[Array[java.lang.String]]): Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]] = {
		val x221 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]()
		val x210 = (' ', ' ')
		val x211 = (0.0, 0)
		val x212 = (0.0, x211)
		val x213 = (0.0, x212)
		val x214 = (0.0, x213)
		val x215 = (0.0, x214)
		val x216 = (0.0, x215)
		val x217 = (0.0, x216)
		var x223: Int = 0
		val x226 = while (x223 < 0) {
			2
			x223 = x223 + 1
		}
		val x2645 = x0.length
		var x2707 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]](x2645)
		for (x2646 <- 0 until x2645) {
			val x2647 = x0.apply(x2646)
			val x2648 = x2647.apply(0)
			val x2649 = x2648.toInt
			val x2650 = x2647.apply(1)
			val x2651 = x2650.toInt
			val x2652 = x2647.apply(2)
			val x2653 = x2652.toInt
			val x2654 = x2647.apply(3)
			val x2655 = x2654.toInt
			val x2656 = x2647.apply(4)
			val x2657 = x2656.toDouble
			val x2658 = x2647.apply(5)
			val x2659 = x2658.toDouble
			val x2660 = x2647.apply(6)
			val x2661 = x2660.toDouble
			val x2662 = x2647.apply(7)
			val x2663 = x2662.toDouble
			val x2664 = x2647.apply(8)
			val x2665 = x2664.charAt(0)
			val x2666 = x2647.apply(9)
			val x2667 = x2666.charAt(0)
			val x2668 = x2647.apply(10)
			val x2669 = x2668.substring(0, 4)
			val x2670 = x2668.substring(5, 7)
			val x2671 = x2669 + x2670
			val x2672 = x2668.substring(8, 10)
			val x2673 = x2671 + x2672
			val x2674 = x2673.toInt
			val x2675 = x2647.apply(11)
			val x2676 = x2675.substring(0, 4)
			val x2677 = x2675.substring(5, 7)
			val x2678 = x2676 + x2677
			val x2679 = x2675.substring(8, 10)
			val x2680 = x2678 + x2679
			val x2681 = x2680.toInt
			val x2682 = x2647.apply(12)
			val x2683 = x2682.substring(0, 4)
			val x2684 = x2682.substring(5, 7)
			val x2685 = x2683 + x2684
			val x2686 = x2682.substring(8, 10)
			val x2687 = x2685 + x2686
			val x2688 = x2687.toInt
			val x2689 = x2647.apply(13)
			val x2690 = x2647.apply(14)
			val x2691 = x2647.apply(15)
			val x2692 = (x2690, x2691)
			val x2693 = (x2689, x2692)
			val x2694 = (x2688, x2693)
			val x2695 = (x2681, x2694)
			val x2696 = (x2674, x2695)
			val x2697 = (x2667, x2696)
			val x2698 = (x2665, x2697)
			val x2699 = (x2663, x2698)
			val x2700 = (x2661, x2699)
			val x2701 = (x2659, x2700)
			val x2702 = (x2657, x2701)
			val x2703 = (x2655, x2702)
			val x2704 = (x2653, x2703)
			val x2705 = (x2651, x2704)
			val x2706 = (x2649, x2705)
			x2707(x2646) = x2706
		}
		val x4021 = { x3365: (Int) =>
			val x3389 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3387 = java.lang.String.valueOf(x3365)
			val x3388 = x3387 + ".l_orderkey"
			val x3390 = (x3388, x3389)
			val x3396 = { x3391: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
				val x3393 = x3391._2
				val x3394 = x3389 += x3393
				x3390: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
			}
			var x3397: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x3390
			var x4782_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			var x4785_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			var x4788_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			var x4791_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			var x4794_buf = scala.collection.mutable.ArrayBuilder.make[Double]
			var x4797_buf = scala.collection.mutable.ArrayBuilder.make[Double]
			var x4800_buf = scala.collection.mutable.ArrayBuilder.make[Double]
			var x4803_buf = scala.collection.mutable.ArrayBuilder.make[Double]
			var x4806_buf = scala.collection.mutable.ArrayBuilder.make[Char]
			var x4809_buf = scala.collection.mutable.ArrayBuilder.make[Char]
			var x4812_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			var x4815_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			var x4818_buf = scala.collection.mutable.ArrayBuilder.make[Int]
			var x4821_buf = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			var x4824_buf = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			var x4827_buf = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			for (x3373 <- 0 until x2645) {
				val x3374 = x2707.apply(x3373)
				val x3375 = x3374._1
				val x3377 = x3375.hashCode
				val x3378 = x3377 % 4
				val x3379 = x3378 == x3365
				val x3376 = x3374._2
				val x4708 = x3376._1
				val x4711 = x3376._2
				val x4712 = x4711._1
				val x4715 = x4711._2
				val x4716 = x4715._1
				val x4719 = x4715._2
				val x4720 = x4719._1
				val x4723 = x4719._2
				val x4724 = x4723._1
				val x4727 = x4723._2
				val x4728 = x4727._1
				val x4731 = x4727._2
				val x4732 = x4731._1
				val x4735 = x4731._2
				val x4736 = x4735._1
				val x4739 = x4735._2
				val x4740 = x4739._1
				val x4743 = x4739._2
				val x4744 = x4743._1
				val x4747 = x4743._2
				val x4748 = x4747._1
				val x4751 = x4747._2
				val x4752 = x4751._1
				val x4755 = x4751._2
				val x4756 = x4755._1
				val x4759 = x4755._2
				val x4760 = x4759._1
				val x4763 = x4759._2
				if (x3379) x4782_buf += x3375
				if (x3379) x4785_buf += x4708
				if (x3379) x4788_buf += x4712
				if (x3379) x4791_buf += x4716
				if (x3379) x4794_buf += x4720
				if (x3379) x4797_buf += x4724
				if (x3379) x4800_buf += x4728
				if (x3379) x4803_buf += x4732
				if (x3379) x4806_buf += x4736
				if (x3379) x4809_buf += x4740
				if (x3379) x4812_buf += x4744
				if (x3379) x4815_buf += x4748
				if (x3379) x4818_buf += x4752
				if (x3379) x4821_buf += x4756
				if (x3379) x4824_buf += x4760
				if (x3379) x4827_buf += x4763
			}
			val x4782 = x4782_buf.result
			val x4785 = x4785_buf.result
			val x4788 = x4788_buf.result
			val x4791 = x4791_buf.result
			val x4794 = x4794_buf.result
			val x4797 = x4797_buf.result
			val x4800 = x4800_buf.result
			val x4803 = x4803_buf.result
			val x4806 = x4806_buf.result
			val x4809 = x4809_buf.result
			val x4812 = x4812_buf.result
			val x4815 = x4815_buf.result
			val x4818 = x4818_buf.result
			val x4821 = x4821_buf.result
			val x4824 = x4824_buf.result
			val x4827 = x4827_buf.result
			val x4783 = x4782.foreach {
				x3398 =>
					val x3399 = x3397
					val x3400 = x3399.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
					val x3402 = x3389 += x3398
					x3397 = x3390

					()
			}
			val x4784 = x3397
			val x3415 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3414 = x3387 + ".l_partkey"
			val x3416 = (x3414, x3415)
			val x3422 = { x3417: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
				val x3419 = x3417._2
				val x3420 = x3415 += x3419
				x3416: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
			}
			var x3423: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x3416
			val x4786 = x4785.foreach {
				x3424 =>
					val x3425 = x3423
					val x3426 = x3425.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
					val x3428 = x3415 += x3424
					x3423 = x3416

					()
			}
			val x4787 = x3423
			val x3443 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3442 = x3387 + ".l_suppkey"
			val x3444 = (x3442, x3443)
			val x3450 = { x3445: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
				val x3447 = x3445._2
				val x3448 = x3443 += x3447
				x3444: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
			}
			var x3451: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x3444
			val x4789 = x4788.foreach {
				x3452 =>
					val x3453 = x3451
					val x3454 = x3453.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
					val x3456 = x3443 += x3452
					x3451 = x3444

					()
			}
			val x4790 = x3451
			val x3473 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3472 = x3387 + ".l_linenumber"
			val x3474 = (x3472, x3473)
			val x3480 = { x3475: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
				val x3477 = x3475._2
				val x3478 = x3473 += x3477
				x3474: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
			}
			var x3481: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x3474
			val x4792 = x4791.foreach {
				x3482 =>
					val x3483 = x3481
					val x3484 = x3483.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
					val x3486 = x3473 += x3482
					x3481 = x3474

					()
			}
			val x4793 = x3481
			val x3505 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x3504 = x3387 + ".l_quantity"
			val x3506 = (x3504, x3505)
			val x3512 = { x3507: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], Double]) =>
				val x3509 = x3507._2
				val x3510 = x3505 += x3509
				x3506: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]
			}
			var x3513: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]] = x3506
			val x4795 = x4794.foreach {
				x3514 =>
					val x3515 = x3513
					val x3516 = x3515.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]]
					val x3518 = x3505 += x3514
					x3513 = x3506

					()
			}
			val x4796 = x3513
			val x3539 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x3538 = x3387 + ".l_extendedprice"
			val x3540 = (x3538, x3539)
			val x3546 = { x3541: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], Double]) =>
				val x3543 = x3541._2
				val x3544 = x3539 += x3543
				x3540: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]
			}
			var x3547: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]] = x3540
			val x4798 = x4797.foreach {
				x3548 =>
					val x3549 = x3547
					val x3550 = x3549.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]]
					val x3552 = x3539 += x3548
					x3547 = x3540

					()
			}
			val x4799 = x3547
			val x3575 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x3574 = x3387 + ".l_discount"
			val x3576 = (x3574, x3575)
			val x3582 = { x3577: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], Double]) =>
				val x3579 = x3577._2
				val x3580 = x3575 += x3579
				x3576: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]
			}
			var x3583: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]] = x3576
			val x4801 = x4800.foreach {
				x3584 =>
					val x3585 = x3583
					val x3586 = x3585.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]]
					val x3588 = x3575 += x3584
					x3583 = x3576

					()
			}
			val x4802 = x3583
			val x3613 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x3612 = x3387 + ".l_tax"
			val x3614 = (x3612, x3613)
			val x3620 = { x3615: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], Double]) =>
				val x3617 = x3615._2
				val x3618 = x3613 += x3617
				x3614: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]
			}
			var x3621: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]] = x3614
			val x4804 = x4803.foreach {
				x3622 =>
					val x3623 = x3621
					val x3624 = x3623.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]]
					val x3626 = x3613 += x3622
					x3621 = x3614

					()
			}
			val x4805 = x3621
			val x3653 = scala.collection.mutable.ArrayBuilder.make[Char]
			val x3652 = x3387 + ".l_returnflag"
			val x3654 = (x3652, x3653)
			val x3660 = { x3655: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], Char]) =>
				val x3657 = x3655._2
				val x3658 = x3653 += x3657
				x3654: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]]
			}
			var x3661: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]] = x3654
			val x4807 = x4806.foreach {
				x3662 =>
					val x3663 = x3661
					val x3664 = x3663.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]]]
					val x3666 = x3653 += x3662
					x3661 = x3654

					()
			}
			val x4808 = x3661
			val x3695 = scala.collection.mutable.ArrayBuilder.make[Char]
			val x3694 = x3387 + ".l_linestatus"
			val x3696 = (x3694, x3695)
			val x3702 = { x3697: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], Char]) =>
				val x3699 = x3697._2
				val x3700 = x3695 += x3699
				x3696: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]]
			}
			var x3703: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]] = x3696
			val x4810 = x4809.foreach {
				x3704 =>
					val x3705 = x3703
					val x3706 = x3705.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]]]
					val x3708 = x3695 += x3704
					x3703 = x3696

					()
			}
			val x4811 = x3703
			val x3739 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3738 = x3387 + ".l_shipdate"
			val x3740 = (x3738, x3739)
			val x3746 = { x3741: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
				val x3743 = x3741._2
				val x3744 = x3739 += x3743
				x3740: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
			}
			var x3747: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x3740
			val x4813 = x4812.foreach {
				x3748 =>
					val x3749 = x3747
					val x3750 = x3749.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
					val x3752 = x3739 += x3748
					x3747 = x3740

					()
			}
			val x4814 = x3747
			val x3785 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3784 = x3387 + ".l_commitdate"
			val x3786 = (x3784, x3785)
			val x3792 = { x3787: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
				val x3789 = x3787._2
				val x3790 = x3785 += x3789
				x3786: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
			}
			var x3793: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x3786
			val x4816 = x4815.foreach {
				x3794 =>
					val x3795 = x3793
					val x3796 = x3795.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
					val x3798 = x3785 += x3794
					x3793 = x3786

					()
			}
			val x4817 = x3793
			val x3833 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3832 = x3387 + ".l_receiptdate"
			val x3834 = (x3832, x3833)
			val x3840 = { x3835: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
				val x3837 = x3835._2
				val x3838 = x3833 += x3837
				x3834: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
			}
			var x3841: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x3834
			val x4819 = x4818.foreach {
				x3842 =>
					val x3843 = x3841
					val x3844 = x3843.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
					val x3846 = x3833 += x3842
					x3841 = x3834

					()
			}
			val x4820 = x3841
			val x3883 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x3882 = x3387 + ".l_shipinstruct"
			val x3884 = (x3882, x3883)
			val x3890 = { x3885: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], java.lang.String]) =>
				val x3887 = x3885._2
				val x3888 = x3883 += x3887
				x3884: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]
			}
			var x3891: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]] = x3884
			val x4822 = x4821.foreach {
				x3892 =>
					val x3893 = x3891
					val x3894 = x3893.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]
					val x3896 = x3883 += x3892
					x3891 = x3884

					()
			}
			val x4823 = x3891
			val x3935 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x3934 = x3387 + ".l_shipmode"
			val x3936 = (x3934, x3935)
			val x3942 = { x3937: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], java.lang.String]) =>
				val x3939 = x3937._2
				val x3940 = x3935 += x3939
				x3936: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]
			}
			var x3943: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]] = x3936
			val x4825 = x4824.foreach {
				x3944 =>
					val x3945 = x3943
					val x3946 = x3945.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]
					val x3948 = x3935 += x3944
					x3943 = x3936

					()
			}
			val x4826 = x3943
			val x3987 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x3986 = x3387 + ".l_comment"
			val x3988 = (x3986, x3987)
			val x3994 = { x3989: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], java.lang.String]) =>
				val x3991 = x3989._2
				val x3992 = x3987 += x3991
				x3988: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]
			}
			var x3995: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]] = x3988
			val x4828 = x4827.foreach {
				x3996 =>
					val x3997 = x3995
					val x3998 = x3997.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]
					val x4000 = x3987 += x3996
					x3995 = x3988

					()
			}
			val x4829 = x3995
			val x4830 = (x4826, x4829)
			val x4831 = (x4823, x4830)
			val x4832 = (x4820, x4831)
			val x4833 = (x4817, x4832)
			val x4834 = (x4814, x4833)
			val x4835 = (x4811, x4834)
			val x4836 = (x4808, x4835)
			val x4837 = (x4805, x4836)
			val x4838 = (x4802, x4837)
			val x4839 = (x4799, x4838)
			val x4840 = (x4796, x4839)
			val x4841 = (x4793, x4840)
			val x4842 = (x4790, x4841)
			val x4843 = (x4787, x4842)
			val x4844 = (x4784, x4843)
			val x4019 = x4844
			x4019: scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]]
		}
		// generating parallel execute
		val x4022 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until 4) yield scala.concurrent.future {
				x4021(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}

        println("Start GC")
        java.lang.System.gc()
        println("Start query")
        val start = now

		val x4558 = { x4291: (Int) =>
			val x4292 = x4022.apply(x4291)
			val x4293 = x4292._1
			val x4294 = x4292._2
			val x4295 = x4294._1
			val x4296 = x4294._2
			val x4349 = x4293._1
			val x4350 = x4293._2
			val x4469 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]()
			val x4298 = x4296._2
			val x4300 = x4298._2
			val x4302 = x4300._2
			val x4304 = x4302._2
			val x4306 = x4304._2
			val x4308 = x4306._2
			val x4310 = x4308._2
			val x4312 = x4310._2
			val x4314 = x4312._2
			val x4316 = x4314._2
			val x4318 = x4316._2
			val x4319 = x4318._1
			val x4322 = x4319._2
			val x4323 = x4322.result
			val x4324 = x4323.length
			val x4313 = x4312._1
			val x4333 = x4313._2
			val x4334 = x4333.result
			val x4309 = x4308._1
			val x4353 = x4309._2
			val x4354 = x4353.result
			val x4311 = x4310._1
			val x4342 = x4311._2
			val x4343 = x4342.result
			val x4301 = x4300._1
			val x4339 = x4301._2
			val x4340 = x4339.result
			val x4303 = x4302._1
			val x4365 = x4303._2
			val x4366 = x4365.result
			val x4305 = x4304._1
			val x4356 = x4305._2
			val x4357 = x4356.result
			val x4307 = x4306._1
			val x4362 = x4307._2
			val x4363 = x4362.result
			var x4471: Int = 0
			val x4556 = while (x4471 < x4324) {
				val x4482 = x4334.apply(x4471)
				val x4503 = x4482 <= 19981201
				val x4554 = if (x4503) {
					val x4480 = x4354.apply(x4471)
					val x4481 = x4343.apply(x4471)
					val x4504 = (x4480, x4481)
					val x4518 = x4469.containsKey(x4504)
					val x4551 = if (x4518) {
						val x4519 = x4469.get(x4504)
						val x4521 = x4519._1
						val x4522 = x4519._2
						val x4476 = x4340.apply(x4471)
						val x4523 = x4521 + x4476
						val x4477 = x4366.apply(x4471)
						val x4524 = x4522._1
						val x4526 = x4524 + x4477
						val x4478 = x4357.apply(x4471)
						val x4505 = 1.0 - x4478
						val x4506 = x4477 * x4505
						val x4525 = x4522._2
						val x4527 = x4525._1
						val x4529 = x4527 + x4506
						val x4479 = x4363.apply(x4471)
						val x4507 = 1.0 + x4479
						val x4508 = x4505 * x4507
						val x4509 = x4477 * x4508
						val x4528 = x4525._2
						val x4530 = x4528._1
						val x4532 = x4530 + x4509
						val x4531 = x4528._2
						val x4533 = x4531._1
						val x4535 = x4533 + x4476
						val x4534 = x4531._2
						val x4536 = x4534._1
						val x4538 = x4536 + x4477
						val x4537 = x4534._2
						val x4539 = x4537._1
						val x4541 = x4539 + x4478
						val x4540 = x4537._2
						val x4542 = x4540 + 1
						val x4543 = (x4541, x4542)
						val x4544 = (x4538, x4543)
						val x4545 = (x4535, x4544)
						val x4546 = (x4532, x4545)
						val x4547 = (x4529, x4546)
						val x4548 = (x4526, x4547)
						val x4549 = (x4523, x4548)
						x4549
					} else {
						val x4476 = x4340.apply(x4471)
						val x4477 = x4366.apply(x4471)
						val x4478 = x4357.apply(x4471)
						val x4505 = 1.0 - x4478
						val x4506 = x4477 * x4505
						val x4479 = x4363.apply(x4471)
						val x4507 = 1.0 + x4479
						val x4508 = x4505 * x4507
						val x4509 = x4477 * x4508
						val x4510 = (x4478, 1)
						val x4511 = (x4477, x4510)
						val x4512 = (x4476, x4511)
						val x4513 = (x4509, x4512)
						val x4514 = (x4506, x4513)
						val x4515 = (x4477, x4514)
						val x4516 = (x4476, x4515)
						x4516
					}
					val x4552 = x4469.put(x4504, x4551)
					x4552
				} else {
					()
				}

				x4471 = x4471 + 1
			}
			x4469: java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]
		}
		// generating parallel execute
		val x4559 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until 4) yield scala.concurrent.future {
				x4558(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		var x4560: java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]] = x221
		val x4634 = x4559.foreach {
			x4561 =>
				val x4562 = x4560
				val x4563 = x4562.asInstanceOf[java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]
				val x4565 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]()
				val x4566 = scala.collection.JavaConverters.asScalaSetConverter(x4563.keySet).asScala.toIterable
				val x4621 = x4566.foreach {
					x4567 =>
						val x4568 = x4561.containsKey(x4567)
						val x4618 = if (x4568) {
							val x4569 = x4563.get(x4567)
							val x4572 = x4569._1
							val x4573 = x4569._2
							val x4570 = x4561.get(x4567)
							val x4574 = x4570._1
							val x4576 = x4572 + x4574
							val x4577 = x4573._1
							val x4575 = x4570._2
							val x4579 = x4575._1
							val x4581 = x4577 + x4579
							val x4578 = x4573._2
							val x4582 = x4578._1
							val x4580 = x4575._2
							val x4584 = x4580._1
							val x4586 = x4582 + x4584
							val x4583 = x4578._2
							val x4587 = x4583._1
							val x4585 = x4580._2
							val x4589 = x4585._1
							val x4591 = x4587 + x4589
							val x4588 = x4583._2
							val x4592 = x4588._1
							val x4590 = x4585._2
							val x4594 = x4590._1
							val x4596 = x4592 + x4594
							val x4593 = x4588._2
							val x4597 = x4593._1
							val x4595 = x4590._2
							val x4599 = x4595._1
							val x4601 = x4597 + x4599
							val x4598 = x4593._2
							val x4602 = x4598._1
							val x4600 = x4595._2
							val x4604 = x4600._1
							val x4606 = x4602 + x4604
							val x4603 = x4598._2
							val x4605 = x4600._2
							val x4607 = x4603 + x4605
							val x4608 = (x4606, x4607)
							val x4609 = (x4601, x4608)
							val x4610 = (x4596, x4609)
							val x4611 = (x4591, x4610)
							val x4612 = (x4586, x4611)
							val x4613 = (x4581, x4612)
							val x4614 = (x4576, x4613)
							x4614
						} else {
							val x4616 = x4563.get(x4567)
							x4616
						}
						val x4619 = x4565.put(x4567, x4618)

						x4619
				}
				val x4622 = scala.collection.JavaConverters.asScalaSetConverter(x4561.keySet).asScala.toIterable
				val x4631 = x4622.foreach {
					x4623 =>
						val x4624 = x4563.containsKey(x4623)
						val x4625 = !x4624
						val x4629 = if (x4625) {
							val x4626 = x4561.get(x4623)
							val x4627 = x4565.put(x4623, x4626)
							x4627
						} else {
							()
						}

						x4629
				}
				x4560 = x4565

				()
		}
		val x4635 = x4560
		val x4636 = scala.collection.JavaConverters.asScalaSetConverter(x4635.keySet).asScala.toIterable
		val x4637 = x4636.toArray
		val x4638 = x4637.length
		var x4644 = new Array[scala.Tuple2[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]](x4638)
		for (x4639 <- 0 until x4638) {
			val x4640 = x4637.apply(x4639)
			val x4641 = x4635.get(x4640)
			val x4642 = (x4640, x4641)
			x4644(x4639) = x4642
		}
		val x4645 = x4644.length
		var x4680 = new Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]](x4645)
		for (x4646 <- 0 until x4645) {
			val x4647 = x4644.apply(x4646)
			val x4648 = x4647._1
			val x4649 = x4647._2
			val x4650 = x4648._1
			val x4651 = x4648._2
			val x4652 = x4649._1
			val x4653 = x4649._2
			val x4654 = x4653._1
			val x4655 = x4653._2
			val x4656 = x4655._1
			val x4657 = x4655._2
			val x4658 = x4657._1
			val x4659 = x4657._2
			val x4660 = x4659._1
			val x4661 = x4659._2
			val x4663 = x4661._2
			val x4665 = x4663._2
			val x4666 = x4665.toDouble
			val x4667 = x4660 / x4666
			val x4662 = x4661._1
			val x4668 = x4662 / x4666
			val x4664 = x4663._1
			val x4669 = x4664 / x4666
			val x4670 = (x4669, x4665)
			val x4671 = (x4668, x4670)
			val x4672 = (x4667, x4671)
			val x4673 = (x4658, x4672)
			val x4674 = (x4656, x4673)
			val x4675 = (x4654, x4674)
			val x4676 = (x4652, x4675)
			val x4677 = (x4651, x4676)
			val x4678 = (x4650, x4677)
			x4680(x4646) = x4678
		}
		val x4687 = { x4681: (scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]) =>
			val x4682 = x4681._1
			val x4683 = x4681._2
			val x4684 = x4683._1
			val x4686 = (x4682, x4684)
			x4686: scala.Tuple2[Char, Char]
		}
		val x4688 = x4680.sortBy(x4687)
        println("Elapsed time: " + (now - start))   
		x4688
	}
}
/*****************************************
  End of Generated Code                  
*******************************************/
def now: Long = java.lang.System.currentTimeMillis()
val in = scala.io.Source.fromFile("/home/knizhnik/tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray
val benchmark = new TPCH_Q1_ver_par()
val result = benchmark(in)
