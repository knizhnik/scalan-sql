/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q10_hor_seq extends ((Array[Array[Array[java.lang.String]]])=>(Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]])) {
  def apply(x0: Array[Array[Array[java.lang.String]]]): Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]] = {
    val x6 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    val x8 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]()
    val x7 = ("Lineitem", x6)
    val x12 = { x9: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]) =>
      val x10 = x9._1
      x10: Int
    }
    val x13 = (x8, x12)
    val x14 = (x7, x13)
    val x15 = ("Lineitem.sk", x14)
    val x32 = { x16: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]) =>
      val x18 = x16._2
      val x19 = x18._1
      val x21 = x8.containsKey(x19)
      val x29 = if (x21) {
        val x22 = x8.get(x19)
        val x23 = x22 += x18
        x8
      } else {
        val x25 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
        val x26 = x25 += x18
        val x27 = x8.put(x19, x25)
        x8
      }
      val x30 = x6 += x18
      x15: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]
    }
    val x50 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    val x33 = ("", "")
    val x34 = ("", x33)
    val x35 = (0, x34)
    val x36 = (0, x35)
    val x37 = (0, x36)
    val x38 = (' ', x37)
    val x39 = (' ', x38)
    val x40 = (0.0, x39)
    val x41 = (0.0, x40)
    val x42 = (0.0, x41)
    val x43 = (0.0, x42)
    val x44 = (0, x43)
    val x45 = (0, x44)
    val x46 = (0, x45)
    val x48 = (0, x46)
    var x52: Int = 0
    val x55 = while (x52 < 0) {
      val x53 = x50 += x48

      x52 = x52 + 1
    }
    val x56 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]
    val x58 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]()
    val x66 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]
    val x72 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]()
    val x71 = ("Customer", x66)
    val x76 = { x73: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]) =>
      val x74 = x73._1
      x74: Int
    }
    val x77 = (x72, x76)
    val x78 = (x71, x77)
    val x79 = ("Customer.pk", x78)
    val x98 = { x80: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]) =>
      val x82 = x80._2
      val x83 = x82._1
      val x88 = { x85: (Unit) =>
        val x86 = x72.put(x83, x82)
        x72: Unit
      }
      val x89 = x72.containsKey(x83)
      val x95 = if (x89) {
        val x90 = x72.get(x83)
        val x91 = throw new Exception("Unique constraint violation")
        x91
      } else {
        val x93 = x72.put(x83, x82)
        x72
      }
      val x96 = x66 += x82
      x79: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]
    }
    val x119 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
    val x57 = ("Nation", x56)
    val x62 = { x59: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]) =>
      val x60 = x59._1
      x60: Int
    }
    val x63 = (x58, x62)
    val x64 = (x57, x63)
    val x65 = ("Nation.pk", x64)
    val x182 = { x164: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]) =>
      val x166 = x164._2
      val x167 = x166._1
      val x172 = { x169: (Unit) =>
        val x170 = x58.put(x167, x166)
        x58: Unit
      }
      val x173 = x58.containsKey(x167)
      val x179 = if (x173) {
        val x174 = x58.get(x167)
        val x175 = throw new Exception("Unique constraint violation")
        x175
      } else {
        val x177 = x58.put(x167, x166)
        x58
      }
      val x180 = x56 += x166
      x65: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]], Int]]]]
    }
    val x120 = ("Orders", x119)
    val x229 = { x224: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]) =>
      val x226 = x224._2
      val x227 = x119 += x226
      x120: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
    }
    var x857: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]] = x120
    val x824 = x0.apply(1)
    val x825 = x824.length
    var x856 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]](x825)
    for (x826 <- 0 until x825) {
      val x827 = x824.apply(x826)
      val x828 = x827.apply(0)
      val x829 = x828.toInt
      val x830 = x827.apply(1)
      val x831 = x830.toInt
      val x832 = x827.apply(2)
      val x833 = x832.charAt(0)
      val x834 = x827.apply(3)
      val x835 = x834.toDouble
      val x836 = x827.apply(4)
      val x837 = x836.substring(0, 4)
      val x838 = x836.substring(5, 7)
      val x839 = x837 + x838
      val x840 = x836.substring(8, 10)
      val x841 = x839 + x840
      val x842 = x841.toInt
      val x843 = x827.apply(5)
      val x844 = x827.apply(6)
      val x845 = x827.apply(7)
      val x846 = x845.toInt
      val x847 = x827.apply(8)
      val x848 = (x846, x847)
      val x849 = (x844, x848)
      val x850 = (x843, x849)
      val x851 = (x842, x850)
      val x852 = (x835, x851)
      val x853 = (x833, x852)
      val x854 = (x831, x853)
      val x855 = (x829, x854)
      x856(x826) = x855
    }
    val x865 = x856.foreach {
      x858 =>
        val x859 = x857
        val x860 = x859.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]]
        val x862 = x119 += x858
        x857 = x120

        ()
    }
    val x866 = x857
    val x867 = x866._1
    val x868 = x866._2
    var x910: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]] = x79
    val x887 = x0.apply(0)
    val x888 = x887.length
    var x909 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]](x888)
    for (x889 <- 0 until x888) {
      val x890 = x887.apply(x889)
      val x891 = x890.apply(0)
      val x892 = x891.toInt
      val x893 = x890.apply(1)
      val x894 = x890.apply(2)
      val x895 = x890.apply(3)
      val x896 = x895.toInt
      val x897 = x890.apply(4)
      val x898 = x890.apply(5)
      val x899 = x898.toDouble
      val x900 = x890.apply(6)
      val x901 = x890.apply(7)
      val x902 = (x900, x901)
      val x903 = (x899, x902)
      val x904 = (x897, x903)
      val x905 = (x896, x904)
      val x906 = (x894, x905)
      val x907 = (x893, x906)
      val x908 = (x892, x907)
      x909(x889) = x908
    }
    val x931 = x909.foreach {
      x911 =>
        val x912 = x910
        val x913 = x912.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]]
        val x915 = x911._1
        val x920 = { x917: (Unit) =>
          val x918 = x72.put(x915, x911)
          x72: Unit
        }
        val x921 = x72.containsKey(x915)
        val x927 = if (x921) {
          val x922 = x72.get(x915)
          val x923 = throw new Exception("Unique constraint violation")
          x923
        } else {
          val x925 = x72.put(x915, x911)
          x72
        }
        val x928 = x66 += x911
        x910 = x79

        ()
    }
    val x932 = x910
    val x933 = x932._1
    val x934 = x932._2
    var x1021: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]] = x15
    val x957 = x0.apply(2)
    val x958 = x957.length
    var x1020 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]](x958)
    for (x959 <- 0 until x958) {
      val x960 = x957.apply(x959)
      val x961 = x960.apply(0)
      val x962 = x961.toInt
      val x963 = x960.apply(1)
      val x964 = x963.toInt
      val x965 = x960.apply(2)
      val x966 = x965.toInt
      val x967 = x960.apply(3)
      val x968 = x967.toInt
      val x969 = x960.apply(4)
      val x970 = x969.toDouble
      val x971 = x960.apply(5)
      val x972 = x971.toDouble
      val x973 = x960.apply(6)
      val x974 = x973.toDouble
      val x975 = x960.apply(7)
      val x976 = x975.toDouble
      val x977 = x960.apply(8)
      val x978 = x977.charAt(0)
      val x979 = x960.apply(9)
      val x980 = x979.charAt(0)
      val x981 = x960.apply(10)
      val x982 = x981.substring(0, 4)
      val x983 = x981.substring(5, 7)
      val x984 = x982 + x983
      val x985 = x981.substring(8, 10)
      val x986 = x984 + x985
      val x987 = x986.toInt
      val x988 = x960.apply(11)
      val x989 = x988.substring(0, 4)
      val x990 = x988.substring(5, 7)
      val x991 = x989 + x990
      val x992 = x988.substring(8, 10)
      val x993 = x991 + x992
      val x994 = x993.toInt
      val x995 = x960.apply(12)
      val x996 = x995.substring(0, 4)
      val x997 = x995.substring(5, 7)
      val x998 = x996 + x997
      val x999 = x995.substring(8, 10)
      val x1000 = x998 + x999
      val x1001 = x1000.toInt
      val x1002 = x960.apply(13)
      val x1003 = x960.apply(14)
      val x1004 = x960.apply(15)
      val x1005 = (x1003, x1004)
      val x1006 = (x1002, x1005)
      val x1007 = (x1001, x1006)
      val x1008 = (x994, x1007)
      val x1009 = (x987, x1008)
      val x1010 = (x980, x1009)
      val x1011 = (x978, x1010)
      val x1012 = (x976, x1011)
      val x1013 = (x974, x1012)
      val x1014 = (x972, x1013)
      val x1015 = (x970, x1014)
      val x1016 = (x968, x1015)
      val x1017 = (x966, x1016)
      val x1018 = (x964, x1017)
      val x1019 = (x962, x1018)
      x1020(x959) = x1019
    }

    val x1040 = x1020.foreach {
      x1022 =>
        val x1023 = x1021
        val x1024 = x1023.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]]
        val x1026 = x1022._1
        val x1028 = x8.containsKey(x1026)
        val x1036 = if (x1028) {
          val x1029 = x8.get(x1026)
          val x1030 = x1029 += x1022
          x8
        } else {
          val x1032 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
          val x1033 = x1032 += x1022
          val x1034 = x8.put(x1026, x1032)
          x8
        }
        val x1037 = x6 += x1022
        x1021 = x15

        ()
    }

    java.lang.System.gc()
    println("Start query")
    val start = now

    val x1041 = x1021
    val x1042 = x1041._1
    val x1043 = x1041._2
    val x1068 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    val x1045 = x1043._2
    val x1046 = x1045._1
    val x869 = x868.result
    val x870 = x869.length
    val x936 = x934._2
    val x937 = x936._1
    var x1405_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]
    for (x871 <- 0 until x870) {
      val x872 = x869.apply(x871)
      val x874 = x872._2
      val x876 = x874._2
      val x878 = x876._2
      val x880 = x878._2
      val x881 = x880._1
      val x883 = x881 >= 19941101
      val x884 = x881 < 19950201
      val x885 = x883 && x884
      val x875 = x874._1
      val x1324 = x937.get(x875)
      val x1325 = (x872, x1324)
      if (x885) x1405_buf += x1325
    }
    val x1405 = x1405_buf.result
    val x1406 = x1405.foreach {
      x1069 =>
        val x1070 = x1069._1
        val x1072 = x1070._1
        val x1074 = x1046.containsKey(x1072)
        val x1076 = if (x1074) {
          val x1075 = x1046.get(x1072)
          x1075
        } else {
          x50
        }
        val x1077 = x1076.result
        val x1081 = x1077.length
        var x1086 = new Array[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]](x1081)
        for (x1082 <- 0 until x1081) {
          val x1083 = x1077.apply(x1082)
          val x1084 = (x1069, x1083)
          x1086(x1082) = x1084
        }
        val x1090 = x1086.foreach {
          x1087 =>
            val x1088 = x1068 += x1087

            x1088
        }

        x1090
    }
    val x1407 = x1068.result
    val x1408 = x1407.length
    var x1135: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]], Int]]]] = x65
    var x1424_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    for (x1095 <- 0 until x1408) {
      val x1409 = x1407.apply(x1095)
      val x1410 = x1409._2
      val x1411 = x1410._2
      val x1412 = x1411._2
      val x1413 = x1412._2
      val x1414 = x1413._2
      val x1415 = x1414._2
      val x1416 = x1415._2
      val x1417 = x1416._2
      val x1418 = x1417._2
      val x1419 = x1418._1
      val x1420 = java.lang.String.valueOf(x1419)
      val x1421 = x1420 == "R"
      val x1422 = x1409._1
      if (x1421) x1424_buf += x1409
    }
    val x1424 = x1424_buf.result
    val x1121 = x0.apply(3)
    val x1122 = x1121.length
    var x1134 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]](x1122)
    for (x1123 <- 0 until x1122) {
      val x1124 = x1121.apply(x1123)
      val x1125 = x1124.apply(0)
      val x1126 = x1125.toInt
      val x1127 = x1124.apply(1)
      val x1128 = x1124.apply(2)
      val x1129 = x1128.toInt
      val x1130 = x1124.apply(3)
      val x1131 = (x1129, x1130)
      val x1132 = (x1127, x1131)
      val x1133 = (x1126, x1132)
      x1134(x1123) = x1133
    }
    val x1425 = x1134.foreach {
      x1136 =>
        val x1137 = x1135
        val x1138 = x1137.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]], Int]]]]]
        val x1140 = x1136._1
        val x1145 = { x1142: (Unit) =>
          val x1143 = x58.put(x1140, x1136)
          x58: Unit
        }
        val x1146 = x58.containsKey(x1140)
        val x1152 = if (x1146) {
          val x1147 = x58.get(x1140)
          val x1148 = throw new Exception("Unique constraint violation")
          x1148
        } else {
          val x1150 = x58.put(x1140, x1136)
          x58
        }
        val x1153 = x56 += x1136
        x1135 = x65

        ()
    }
    val x1426 = x1135
    val x1427 = x1426._1
    val x1428 = x1426._2
    val x1429 = new java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]], Double]()
    val x1430 = x1428._2
    val x1431 = x1430._1
    val x1449 = x1407.foreach {
      x1213 =>
        val x1215 = x1213._2
        val x1217 = x1215._2
        val x1219 = x1217._2
        val x1221 = x1219._2
        val x1223 = x1221._2
        val x1225 = x1223._2
        val x1227 = x1225._2
        val x1229 = x1227._2
        val x1231 = x1229._2
        val x1232 = x1231._1
        val x1234 = java.lang.String.valueOf(x1232)
        val x1235 = x1234 == "R"
        val x1447 = if (x1235) {
          val x1214 = x1213._1
          val x1237 = x1214._2
          val x1238 = x1237._1
          val x1239 = x1237._2
          val x1240 = x1239._1
          val x1241 = x1239._2
          val x1243 = x1241._2
          val x1245 = x1243._2
          val x1247 = x1245._2
          val x1248 = x1247._1
          val x1246 = x1245._1
          val x1244 = x1243._1
          val x1432 = x1431.get(x1244)
          val x1433 = x1432._2
          val x1434 = x1433._1
          val x1242 = x1241._1
          val x1249 = x1247._2
          val x1256 = x1249._2
          val x1257 = (x1242, x1256)
          val x1435 = (x1434, x1257)
          val x1436 = (x1246, x1435)
          val x1437 = (x1248, x1436)
          val x1438 = (x1240, x1437)
          val x1439 = (x1238, x1438)
          val x1440 = x1429.containsKey(x1439)
          val x1444 = if (x1440) {
            val x1441 = x1429.get(x1439)
            val x1226 = x1225._1
            val x1228 = x1227._1
            val x1263 = 1.0 - x1228
            val x1264 = x1226 * x1263
            val x1442 = x1441 + x1264
            x1442
          } else {
            val x1226 = x1225._1
            val x1228 = x1227._1
            val x1263 = 1.0 - x1228
            val x1264 = x1226 * x1263
            x1264
          }
          val x1445 = x1429.put(x1439, x1444)
          x1445
        } else {
          ()
        }

        x1447
    }
    val x1450 = scala.collection.JavaConverters.asScalaSetConverter(x1429.keySet).asScala.toIterable
    val x1451 = x1450.toArray
    val x1452 = x1451.length
    var x1457 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]], Double]](x1452)
    for (x1280 <- 0 until x1452) {
      val x1453 = x1451.apply(x1280)
      val x1454 = x1429.get(x1453)
      val x1455 = (x1453, x1454)
      x1457(x1280) = x1455
    }
    val x1458 = x1457.length
    val x1320 = { x1312: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]) =>
      val x1314 = x1312._2
      val x1316 = x1314._2
      val x1317 = x1316._1
      val x1319 = 0.0 - x1317
      x1319: Double
    }
    var x1482 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]](x1458)
    for (x1287 <- 0 until x1458) {
      val x1459 = x1457.apply(x1287)
      val x1460 = x1459._1
      val x1461 = x1459._2
      val x1462 = x1460._1
      val x1463 = x1460._2
      val x1464 = x1463._1
      val x1465 = x1463._2
      val x1466 = x1465._1
      val x1467 = x1465._2
      val x1468 = x1467._2
      val x1469 = x1468._1
      val x1470 = x1468._2
      val x1471 = x1470._1
      val x1472 = x1467._1
      val x1473 = x1470._2
      val x1474 = (x1472, x1473)
      val x1475 = (x1471, x1474)
      val x1476 = (x1469, x1475)
      val x1477 = (x1466, x1476)
      val x1478 = (x1461, x1477)
      val x1479 = (x1464, x1478)
      val x1480 = (x1462, x1479)
      x1482(x1287) = x1480
    }
    val x1483 = x1482.sortBy(x1320)
    val x1321 = x1483
    println("Elapsed time for selecting " + x1321.length + " records: " + (now - start))   
    x1321
  }
}
/*****************************************
  End of Generated Code                  
*******************************************/
def now: Long = java.lang.System.currentTimeMillis()
val in = Array(scala.io.Source.fromFile("./tpch-data/sf1/customer.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("./tpch-data/sf1/orders.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("./tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("./tpch-data/sf1/nation.tbl").getLines().map(s => s.split("\\|")).toArray)    
val benchmark = new TPCH_Q10_hor_seq()
val result = benchmark(in)
