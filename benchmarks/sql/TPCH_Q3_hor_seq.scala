/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q3_hor_seq extends ((Array[Array[Array[java.lang.String]]])=>(Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]])) {
  def apply(x0: Array[Array[Array[java.lang.String]]]): Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]] = {
    val x41 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    val x43 = new java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]()
    val x58 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]()
    val x79 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]
    val x81 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]()
    val x99 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
    val x89 = (0, "")
    val x90 = ("", x89)
    val x91 = ("", x90)
    val x92 = (0, x91)
    val x93 = (0.0, x92)
    val x94 = (' ', x93)
    val x95 = (0, x94)
    val x97 = (0, x95)
    var x101: Int = 0
    val x104 = while (x101 < 0) {
      val x102 = x99 += x97

      x101 = x101 + 1
    }
    val x80 = ("Customer", x79)
    val x85 = { x82: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]) =>
      val x83 = x82._1
      x83: Int
    }
    val x86 = (x81, x85)
    val x87 = (x80, x86)
    val x88 = ("Customer.pk", x87)
    val x127 = { x109: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]) =>
      val x111 = x109._2
      val x112 = x111._1
      val x117 = { x114: (Unit) =>
        val x115 = x81.put(x112, x111)
        x81: Unit
      }
      val x118 = x81.containsKey(x112)
      val x124 = if (x118) {
        val x119 = x81.get(x112)
        val x120 = throw new Exception("Unique constraint violation")
        x120
      } else {
        val x122 = x81.put(x112, x111)
        x81
      }
      val x125 = x79 += x111
      x88: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]
    }
    val x143 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
    val x145 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]()
    val x153 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]()
    val x144 = ("Orders", x143)
    val x149 = { x146: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]) =>
      val x147 = x146._1
      x147: Int
    }
    val x150 = (x145, x149)
    val x151 = (x144, x150)
    val x152 = ("Orders.pk", x151)
    val x159 = { x154: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]) =>
      val x156 = x154._2
      val x157 = x156._1
      x157: Int
    }
    val x160 = (x153, x159)
    val x161 = (x152, x160)
    val x162 = ("Orders.pk.sk", x161)
    val x196 = { x167: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]) =>
      val x169 = x167._2
      val x171 = x169._2
      val x172 = x171._1
      val x174 = x153.containsKey(x172)
      val x182 = if (x174) {
        val x175 = x153.get(x172)
        val x176 = x175 += x169
        x153
      } else {
        val x178 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
        val x179 = x178 += x169
        val x180 = x153.put(x172, x178)
        x153
      }
      val x170 = x169._1
      val x186 = { x183: (Unit) =>
        val x184 = x145.put(x170, x169)
        x145: Unit
      }
      val x187 = x145.containsKey(x170)
      val x193 = if (x187) {
        val x188 = x145.get(x170)
        val x189 = throw new Exception("Unique constraint violation")
        x189
      } else {
        val x191 = x145.put(x170, x169)
        x145
      }
      val x194 = x143 += x169
      x162: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]
    }
    val x380 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    val x363 = ("", "")
    val x364 = ("", x363)
    val x365 = (0, x364)
    val x366 = (0, x365)
    val x367 = (0, x366)
    val x368 = (' ', x367)
    val x369 = (' ', x368)
    val x370 = (0.0, x369)
    val x371 = (0.0, x370)
    val x372 = (0.0, x371)
    val x373 = (0.0, x372)
    val x374 = (0, x373)
    val x375 = (0, x374)
    val x376 = (0, x375)
    val x378 = (0, x376)
    var x381: Int = 0
    val x384 = while (x381 < 0) {
      val x382 = x380 += x378

      x381 = x381 + 1
    }
    val x42 = ("Lineitem", x41)
    val x54 = { x44: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]) =>
      val x45 = x44._1
      val x46 = x44._2
      val x48 = x46._2
      val x50 = x48._2
      val x51 = x50._1
      val x53 = (x45, x51)
      x53: scala.Tuple2[Int, Int]
    }
    val x55 = (x43, x54)
    val x56 = (x42, x55)
    val x57 = ("Lineitem.pk", x56)
    val x62 = { x59: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]) =>
      val x60 = x59._1
      x60: Int
    }
    val x63 = (x58, x62)
    val x64 = (x57, x63)
    val x65 = ("Lineitem.pk.sk", x64)
    val x423 = { x389: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]) =>
      val x391 = x389._2
      val x392 = x391._1
      val x394 = x58.containsKey(x392)
      val x402 = if (x394) {
        val x395 = x58.get(x392)
        val x396 = x395 += x391
        x58
      } else {
        val x398 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
        val x399 = x398 += x391
        val x400 = x58.put(x392, x398)
        x58
      }
      val x393 = x391._2
      val x404 = x393._2
      val x406 = x404._2
      val x407 = x406._1
      val x409 = (x392, x407)
      val x413 = { x410: (Unit) =>
        val x411 = x43.put(x409, x391)
        x43: Unit
      }
      val x414 = x43.containsKey(x409)
      val x420 = if (x414) {
        val x415 = x43.get(x409)
        val x416 = throw new Exception("Unique constraint violation")
        x416
      } else {
        val x418 = x43.put(x409, x391)
        x43
      }
      val x421 = x41 += x391
      x65: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]
    }
    var x932: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]] = x88
    val x909 = x0.apply(0)
    val x910 = x909.length
    var x931 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]](x910)
    for (x911 <- 0 until x910) {
      val x912 = x909.apply(x911)
      val x913 = x912.apply(0)
      val x914 = x913.toInt
      val x915 = x912.apply(1)
      val x916 = x912.apply(2)
      val x917 = x912.apply(3)
      val x918 = x917.toInt
      val x919 = x912.apply(4)
      val x920 = x912.apply(5)
      val x921 = x920.toDouble
      val x922 = x912.apply(6)
      val x923 = x912.apply(7)
      val x924 = (x922, x923)
      val x925 = (x921, x924)
      val x926 = (x919, x925)
      val x927 = (x918, x926)
      val x928 = (x916, x927)
      val x929 = (x915, x928)
      val x930 = (x914, x929)
      x931(x911) = x930
    }
    val x953 = x931.foreach {
      x933 =>
        val x934 = x932
        val x935 = x934.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]]
        val x937 = x933._1
        val x942 = { x939: (Unit) =>
          val x940 = x81.put(x937, x933)
          x81: Unit
        }
        val x943 = x81.containsKey(x937)
        val x949 = if (x943) {
          val x944 = x81.get(x937)
          val x945 = throw new Exception("Unique constraint violation")
          x945
        } else {
          val x947 = x81.put(x937, x933)
          x81
        }
        val x950 = x79 += x933
        x932 = x88

        ()
    }
    val x954 = x932
    val x955 = x954._1
    val x956 = x954._2
    var x1014: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]] = x162
    val x981 = x0.apply(1)
    val x982 = x981.length
    var x1013 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]](x982)
    for (x983 <- 0 until x982) {
      val x984 = x981.apply(x983)
      val x985 = x984.apply(0)
      val x986 = x985.toInt
      val x987 = x984.apply(1)
      val x988 = x987.toInt
      val x989 = x984.apply(2)
      val x990 = x989.charAt(0)
      val x991 = x984.apply(3)
      val x992 = x991.toDouble
      val x993 = x984.apply(4)
      val x994 = x993.substring(0, 4)
      val x995 = x993.substring(5, 7)
      val x996 = x994 + x995
      val x997 = x993.substring(8, 10)
      val x998 = x996 + x997
      val x999 = x998.toInt
      val x1000 = x984.apply(5)
      val x1001 = x984.apply(6)
      val x1002 = x984.apply(7)
      val x1003 = x1002.toInt
      val x1004 = x984.apply(8)
      val x1005 = (x1003, x1004)
      val x1006 = (x1001, x1005)
      val x1007 = (x1000, x1006)
      val x1008 = (x999, x1007)
      val x1009 = (x992, x1008)
      val x1010 = (x990, x1009)
      val x1011 = (x988, x1010)
      val x1012 = (x986, x1011)
      x1013(x983) = x1012
    }

    val x1046 = x1013.foreach {
      x1015 =>
        val x1016 = x1014
        val x1017 = x1016.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]]
        val x1020 = x1015._2
        val x1021 = x1020._1
        val x1023 = x153.containsKey(x1021)
        val x1031 = if (x1023) {
          val x1024 = x153.get(x1021)
          val x1025 = x1024 += x1015
          x153
        } else {
          val x1027 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
          val x1028 = x1027 += x1015
          val x1029 = x153.put(x1021, x1027)
          x153
        }
        val x1019 = x1015._1
        val x1035 = { x1032: (Unit) =>
          val x1033 = x145.put(x1019, x1015)
          x145: Unit
        }
        val x1036 = x145.containsKey(x1019)
        val x1042 = if (x1036) {
          val x1037 = x145.get(x1019)
          val x1038 = throw new Exception("Unique constraint violation")
          x1038
        } else {
          val x1040 = x145.put(x1019, x1015)
          x145
        }
        val x1043 = x143 += x1015
        x1014 = x162

        ()
    }

    var x1178: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]] = x65
    val x1114 = x0.apply(2)
    val x1115 = x1114.length
    var x1177 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]](x1115)
    for (x1116 <- 0 until x1115) {
      val x1117 = x1114.apply(x1116)
      val x1118 = x1117.apply(0)
      val x1119 = x1118.toInt
      val x1120 = x1117.apply(1)
      val x1121 = x1120.toInt
      val x1122 = x1117.apply(2)
      val x1123 = x1122.toInt
      val x1124 = x1117.apply(3)
      val x1125 = x1124.toInt
      val x1126 = x1117.apply(4)
      val x1127 = x1126.toDouble
      val x1128 = x1117.apply(5)
      val x1129 = x1128.toDouble
      val x1130 = x1117.apply(6)
      val x1131 = x1130.toDouble
      val x1132 = x1117.apply(7)
      val x1133 = x1132.toDouble
      val x1134 = x1117.apply(8)
      val x1135 = x1134.charAt(0)
      val x1136 = x1117.apply(9)
      val x1137 = x1136.charAt(0)
      val x1138 = x1117.apply(10)
      val x1139 = x1138.substring(0, 4)
      val x1140 = x1138.substring(5, 7)
      val x1141 = x1139 + x1140
      val x1142 = x1138.substring(8, 10)
      val x1143 = x1141 + x1142
      val x1144 = x1143.toInt
      val x1145 = x1117.apply(11)
      val x1146 = x1145.substring(0, 4)
      val x1147 = x1145.substring(5, 7)
      val x1148 = x1146 + x1147
      val x1149 = x1145.substring(8, 10)
      val x1150 = x1148 + x1149
      val x1151 = x1150.toInt
      val x1152 = x1117.apply(12)
      val x1153 = x1152.substring(0, 4)
      val x1154 = x1152.substring(5, 7)
      val x1155 = x1153 + x1154
      val x1156 = x1152.substring(8, 10)
      val x1157 = x1155 + x1156
      val x1158 = x1157.toInt
      val x1159 = x1117.apply(13)
      val x1160 = x1117.apply(14)
      val x1161 = x1117.apply(15)
      val x1162 = (x1160, x1161)
      val x1163 = (x1159, x1162)
      val x1164 = (x1158, x1163)
      val x1165 = (x1151, x1164)
      val x1166 = (x1144, x1165)
      val x1167 = (x1137, x1166)
      val x1168 = (x1135, x1167)
      val x1169 = (x1133, x1168)
      val x1170 = (x1131, x1169)
      val x1171 = (x1129, x1170)
      val x1172 = (x1127, x1171)
      val x1173 = (x1125, x1172)
      val x1174 = (x1123, x1173)
      val x1175 = (x1121, x1174)
      val x1176 = (x1119, x1175)
      x1177(x1116) = x1176
    }
    val x1215 = x1177.foreach {
      x1179 =>
        val x1180 = x1178
        val x1181 = x1180.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]]
        val x1183 = x1179._1
        val x1185 = x58.containsKey(x1183)
        val x1193 = if (x1185) {
          val x1186 = x58.get(x1183)
          val x1187 = x1186 += x1179
          x58
        } else {
          val x1189 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
          val x1190 = x1189 += x1179
          val x1191 = x58.put(x1183, x1189)
          x58
        }
        val x1184 = x1179._2
        val x1195 = x1184._2
        val x1197 = x1195._2
        val x1198 = x1197._1
        val x1200 = (x1183, x1198)
        val x1204 = { x1201: (Unit) =>
          val x1202 = x43.put(x1200, x1179)
          x43: Unit
        }
        val x1205 = x43.containsKey(x1200)
        val x1211 = if (x1205) {
          val x1206 = x43.get(x1200)
          val x1207 = throw new Exception("Unique constraint violation")
          x1207
        } else {
          val x1209 = x43.put(x1200, x1179)
          x43
        }
        val x1212 = x41 += x1179
        x1178 = x65

        ()
    }
    val x1216 = x1178
    val x1217 = x1216._1
    val x1218 = x1216._2

    java.lang.System.gc()
    println("Start query")
    val start = now

    val x1047 = x1014
    val x1048 = x1047._1
    val x1049 = x1047._2
    val x1072 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
    val x957 = x956._1
    val x960 = x957._2
    val x961 = x960.result
    val x962 = x961.length
    var x980_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]
    for (x963 <- 0 until x962) {
      val x964 = x961.apply(x963)
      val x966 = x964._2
      val x968 = x966._2
      val x970 = x968._2
      val x972 = x970._2
      val x974 = x972._2
      val x976 = x974._2
      val x977 = x976._1
      val x979 = x977 == "HOUSEHOLD"
      if (x979) x980_buf += x964
    }
    val x980 = x980_buf.result
    val x1051 = x1049._2
    val x1052 = x1051._1
    val x1094 = x980.foreach {
      x1073 =>
        val x1074 = x1073._1
        val x1076 = x1052.containsKey(x1074)
        val x1078 = if (x1076) {
          val x1077 = x1052.get(x1074)
          x1077
        } else {
          x99
        }
        val x1079 = x1078.result
        val x1083 = x1079.length
        var x1088 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]](x1083)
        for (x1084 <- 0 until x1083) {
          val x1085 = x1079.apply(x1084)
          val x1086 = (x1073, x1085)
          x1088(x1084) = x1086
        }
        val x1092 = x1088.foreach {
          x1089 =>
            val x1090 = x1072 += x1089

            x1090
        }

        x1092
    }
    val x1095 = x1072.result
    val x1096 = x1095.length
    var x1113_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
    for (x1097 <- 0 until x1096) {
      val x1098 = x1095.apply(x1097)
      val x1100 = x1098._2
      val x1102 = x1100._2
      val x1104 = x1102._2
      val x1106 = x1104._2
      val x1108 = x1106._2
      val x1109 = x1108._1
      val x1111 = x1109 < 19950304
      val x1099 = x1098._1
      if (x1111) x1113_buf += x1098
    }
    val x1113 = x1113_buf.result

    val x1243 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    val x1220 = x1218._2
    val x1221 = x1220._1
    val x1267 = x1113.foreach {
      x1244 =>
        val x1246 = x1244._2
        val x1247 = x1246._1
        val x1249 = x1221.containsKey(x1247)
        val x1251 = if (x1249) {
          val x1250 = x1221.get(x1247)
          x1250
        } else {
          x380
        }
        val x1252 = x1251.result
        val x1256 = x1252.length
        var x1261 = new Array[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]](x1256)
        for (x1257 <- 0 until x1256) {
          val x1258 = x1252.apply(x1257)
          val x1259 = (x1244, x1258)
          x1261(x1257) = x1259
        }
        val x1265 = x1261.foreach {
          x1262 =>
            val x1263 = x1243 += x1262

            x1263
        }

        x1265
    }
    val x1268 = x1243.result
    val x1269 = x1268.length
    var x1298_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    for (x1270 <- 0 until x1269) {
      val x1271 = x1268.apply(x1270)
      val x1273 = x1271._2
      val x1275 = x1273._2
      val x1277 = x1275._2
      val x1279 = x1277._2
      val x1281 = x1279._2
      val x1283 = x1281._2
      val x1285 = x1283._2
      val x1287 = x1285._2
      val x1289 = x1287._2
      val x1291 = x1289._2
      val x1293 = x1291._2
      val x1294 = x1293._1
      val x1296 = x1294 > 19950304
      val x1272 = x1271._1
      if (x1296) x1298_buf += x1271
    }
    val x1298 = x1298_buf.result
    val x1299 = new java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]()
    val x1359 = x1268.foreach {
      x1300 =>
        val x1302 = x1300._2
        val x1304 = x1302._2
        val x1306 = x1304._2
        val x1308 = x1306._2
        val x1310 = x1308._2
        val x1312 = x1310._2
        val x1314 = x1312._2
        val x1316 = x1314._2
        val x1318 = x1316._2
        val x1320 = x1318._2
        val x1322 = x1320._2
        val x1323 = x1322._1
        val x1325 = x1323 > 19950304
        val x1357 = if (x1325) {
          val x1303 = x1302._1
          val x1301 = x1300._1
          val x1327 = x1301._2
          val x1329 = x1327._2
          val x1331 = x1329._2
          val x1333 = x1331._2
          val x1335 = x1333._2
          val x1336 = x1335._1
          val x1337 = x1335._2
          val x1339 = x1337._2
          val x1341 = x1339._2
          val x1342 = x1341._1
          val x1344 = (x1336, x1342)
          val x1345 = (x1303, x1344)
          val x1349 = x1299.containsKey(x1345)
          val x1354 = if (x1349) {
            val x1350 = x1299.get(x1345)
            val x1313 = x1312._1
            val x1315 = x1314._1
            val x1346 = 1.0 - x1315
            val x1347 = x1313 * x1346
            val x1352 = x1350 + x1347
            x1352
          } else {
            val x1313 = x1312._1
            val x1315 = x1314._1
            val x1346 = 1.0 - x1315
            val x1347 = x1313 * x1346
            x1347
          }
          val x1355 = x1299.put(x1345, x1354)
          x1355
        } else {
          ()
        }

        x1357
    }
    val x1360 = scala.collection.JavaConverters.asScalaSetConverter(x1299.keySet).asScala.toIterable
    val x1361 = x1360.toArray
    val x1362 = x1361.length
    var x1368 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]](x1362)
    for (x1363 <- 0 until x1362) {
      val x1364 = x1361.apply(x1363)
      val x1365 = x1299.get(x1364)
      val x1366 = (x1364, x1365)
      x1368(x1363) = x1366
    }
    val x1369 = x1368.length
    var x1379 = new Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]](x1369)
    for (x1370 <- 0 until x1369) {
      val x1371 = x1368.apply(x1370)
      val x1372 = x1371._1
      val x1373 = x1371._2
      val x1374 = x1372._1
      val x1375 = x1372._2
      val x1376 = (x1373, x1375)
      val x1377 = (x1374, x1376)
      x1379(x1370) = x1377
    }
    val x1389 = { x1380: (scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]) =>
      val x1382 = x1380._2
      val x1383 = x1382._1
      val x1385 = 0.0 - x1383
      val x1384 = x1382._2
      val x1386 = x1384._1
      val x1388 = (x1385, x1386)
      x1388: scala.Tuple2[Double, Int]
    }
    val x1390 = x1379.sortBy(x1389)
    println("Elapsed time for selecting " + x1390.length + " records: " + (now - start))   
    x1390
  }
}
/*****************************************
  End of Generated Code                  
*******************************************/
def now: Long = java.lang.System.currentTimeMillis()
val in = Array(scala.io.Source.fromFile("./tpch-data/sf1/customer.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("./tpch-data/sf1/orders.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("./tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray)    
val benchmark = new TPCH_Q3_hor_seq()
val result = benchmark(in)
