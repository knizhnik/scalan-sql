/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q10_ver_seq extends ((Array[Array[Array[java.lang.String]]])=>(Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]])) {
  def apply(x0: Array[Array[Array[java.lang.String]]]): Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]] = {
    val x1 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x3 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x5 = scala.collection.mutable.ArrayBuilder.make[Char]
    val x10 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x12 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x15 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x17 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x19 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x21 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x31 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]()
    val x11 = ("Orders.o_orderkey", x10)
    val x13 = ("Orders.o_custkey", x12)
    val x14 = ("Orders.o_orderstatus", x5)
    val x16 = ("Orders.o_totalprice", x15)
    val x18 = ("Orders.o_orderdate", x17)
    val x20 = ("Orders.o_orderpriority", x19)
    val x4 = ("Orders.o_clerk", x3)
    val x2 = ("Orders.o_comment", x1)
    val x22 = ("Orders.o_shippriority", x21)
    val x23 = (x22, x2)
    val x24 = (x4, x23)
    val x25 = (x20, x24)
    val x26 = (x18, x25)
    val x27 = (x16, x26)
    val x28 = (x14, x27)
    val x29 = (x13, x28)
    val x30 = (x11, x29)
    val x35 = { x32: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]) =>
      val x33 = x32._1
      x33: Int
    }
    val x36 = (x31, x35)
    val x37 = (x30, x36)
    val x38 = ("Orders.o_orderkey+Orders.o_custkey+Orders.o_orderstatus+Orders.o_totalprice+Orders.o_orderdate+Orders.o_orderpriority+Orders.o_clerk+Orders.o_shippriority+Orders.o_comment.pk", x37)
    val x79 = { x39: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]) =>
      val x41 = x39._2
      val x42 = x41._1
      val x47 = { x44: (Unit) =>
        val x45 = x31.put(x42, x41)
        x31: Unit
      }
      val x48 = x31.containsKey(x42)
      val x54 = if (x48) {
        val x49 = x31.get(x42)
        val x50 = throw new Exception("Unique constraint violation")
        x50
      } else {
        val x52 = x31.put(x42, x41)
        x31
      }
      val x55 = x10 += x42
      val x43 = x41._2
      val x56 = x43._1
      val x58 = x12 += x56
      val x57 = x43._2
      val x59 = x57._1
      val x61 = x5 += x59
      val x60 = x57._2
      val x62 = x60._1
      val x64 = x15 += x62
      val x63 = x60._2
      val x65 = x63._1
      val x67 = x17 += x65
      val x66 = x63._2
      val x68 = x66._1
      val x70 = x19 += x68
      val x69 = x66._2
      val x71 = x69._1
      val x73 = x3 += x71
      val x72 = x69._2
      val x74 = x72._1
      val x76 = x21 += x74
      val x75 = x72._2
      val x77 = x1 += x75
      x38: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]
    }
    val x80 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x82 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x84 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x86 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x88 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x90 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x92 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x94 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x96 = scala.collection.mutable.ArrayBuilder.make[Char]
    val x98 = scala.collection.mutable.ArrayBuilder.make[Char]
    val x100 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x102 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x104 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x106 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x108 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x110 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x127 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]()
    val x81 = ("Lineitem.l_orderkey", x80)
    val x83 = ("Lineitem.l_partkey", x82)
    val x85 = ("Lineitem.l_suppkey", x84)
    val x87 = ("Lineitem.l_linenumber", x86)
    val x89 = ("Lineitem.l_quantity", x88)
    val x91 = ("Lineitem.l_extendedprice", x90)
    val x93 = ("Lineitem.l_discount", x92)
    val x95 = ("Lineitem.l_tax", x94)
    val x97 = ("Lineitem.l_returnflag", x96)
    val x99 = ("Lineitem.l_linestatus", x98)
    val x101 = ("Lineitem.l_shipdate", x100)
    val x103 = ("Lineitem.l_commitdate", x102)
    val x105 = ("Lineitem.l_receiptdate", x104)
    val x107 = ("Lineitem.l_shipinstruct", x106)
    val x109 = ("Lineitem.l_shipmode", x108)
    val x111 = ("Lineitem.l_comment", x110)
    val x112 = (x109, x111)
    val x113 = (x107, x112)
    val x114 = (x105, x113)
    val x115 = (x103, x114)
    val x116 = (x101, x115)
    val x117 = (x99, x116)
    val x118 = (x97, x117)
    val x119 = (x95, x118)
    val x120 = (x93, x119)
    val x121 = (x91, x120)
    val x122 = (x89, x121)
    val x123 = (x87, x122)
    val x124 = (x85, x123)
    val x125 = (x83, x124)
    val x126 = (x81, x125)
    val x131 = { x128: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]) =>
      val x129 = x128._1
      x129: Int
    }
    val x132 = (x127, x131)
    val x133 = (x126, x132)
    val x134 = ("Lineitem.l_orderkey+Lineitem.l_partkey+Lineitem.l_suppkey+Lineitem.l_linenumber+Lineitem.l_quantity+Lineitem.l_extendedprice+Lineitem.l_discount+Lineitem.l_tax+Lineitem.l_returnflag+Lineitem.l_linestatus+Lineitem.l_shipdate+Lineitem.l_commitdate+Lineitem.l_receiptdate+Lineitem.l_shipinstruct+Lineitem.l_shipmode+Lineitem.l_comment.sk", x133)
    val x248 = { x189: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]) =>
      val x191 = x189._2
      val x192 = x191._1
      val x194 = x127.containsKey(x192)
      val x202 = if (x194) {
        val x195 = x127.get(x192)
        val x196 = x195 += x191
        x127
      } else {
        val x198 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
        val x199 = x198 += x191
        val x200 = x127.put(x192, x198)
        x127
      }
      val x203 = x80 += x192
      val x193 = x191._2
      val x204 = x193._1
      val x206 = x82 += x204
      val x205 = x193._2
      val x207 = x205._1
      val x209 = x84 += x207
      val x208 = x205._2
      val x210 = x208._1
      val x212 = x86 += x210
      val x211 = x208._2
      val x213 = x211._1
      val x215 = x88 += x213
      val x214 = x211._2
      val x216 = x214._1
      val x218 = x90 += x216
      val x217 = x214._2
      val x219 = x217._1
      val x221 = x92 += x219
      val x220 = x217._2
      val x222 = x220._1
      val x224 = x94 += x222
      val x223 = x220._2
      val x225 = x223._1
      val x227 = x96 += x225
      val x226 = x223._2
      val x228 = x226._1
      val x230 = x98 += x228
      val x229 = x226._2
      val x231 = x229._1
      val x233 = x100 += x231
      val x232 = x229._2
      val x234 = x232._1
      val x236 = x102 += x234
      val x235 = x232._2
      val x237 = x235._1
      val x239 = x104 += x237
      val x238 = x235._2
      val x240 = x238._1
      val x242 = x106 += x240
      val x241 = x238._2
      val x243 = x241._1
      val x245 = x108 += x243
      val x244 = x241._2
      val x246 = x110 += x244
      x134: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]
    }
    val x368 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]()
    val x369 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x371 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x373 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x375 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x370 = ("Nation.n_nationkey", x369)
    val x372 = ("Nation.n_name", x371)
    val x374 = ("Nation.n_regionkey", x373)
    val x376 = ("Nation.n_comment", x375)
    val x377 = (x374, x376)
    val x378 = (x372, x377)
    val x379 = (x370, x378)
    val x387 = { x384: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]) =>
      val x385 = x384._1
      x385: Int
    }
    val x388 = (x368, x387)
    val x389 = (x379, x388)
    val x390 = ("Nation.n_nationkey+Nation.n_name+Nation.n_regionkey+Nation.n_comment.pk", x389)
    val x416 = { x391: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]) =>
      val x393 = x391._2
      val x394 = x393._1
      val x399 = { x396: (Unit) =>
        val x397 = x368.put(x394, x393)
        x368: Unit
      }
      val x400 = x368.containsKey(x394)
      val x406 = if (x400) {
        val x401 = x368.get(x394)
        val x402 = throw new Exception("Unique constraint violation")
        x402
      } else {
        val x404 = x368.put(x394, x393)
        x368
      }
      val x407 = x369 += x394
      val x395 = x393._2
      val x408 = x395._1
      val x410 = x371 += x408
      val x409 = x395._2
      val x411 = x409._1
      val x413 = x373 += x411
      val x412 = x409._2
      val x414 = x375 += x412
      x390: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]], Int]]]]
    }
    val x417 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x419 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x421 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x423 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x425 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x427 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x429 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x431 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x440 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]()
    val x418 = ("Customer.c_custkey", x417)
    val x420 = ("Customer.c_name", x419)
    val x422 = ("Customer.c_address", x421)
    val x424 = ("Customer.c_nationkey", x423)
    val x426 = ("Customer.c_phone", x425)
    val x428 = ("Customer.c_acctbal", x427)
    val x430 = ("Customer.c_mktsegment", x429)
    val x432 = ("Customer.c_comment", x431)
    val x433 = (x430, x432)
    val x434 = (x428, x433)
    val x435 = (x426, x434)
    val x436 = (x424, x435)
    val x437 = (x422, x436)
    val x438 = (x420, x437)
    val x439 = (x418, x438)
    val x444 = { x441: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]) =>
      val x442 = x441._1
      x442: Int
    }
    val x445 = (x440, x444)
    val x446 = (x439, x445)
    val x447 = ("Customer.c_custkey+Customer.c_name+Customer.c_address+Customer.c_nationkey+Customer.c_phone+Customer.c_acctbal+Customer.c_mktsegment+Customer.c_comment.pk", x446)
    val x494 = { x457: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]) =>
      val x459 = x457._2
      val x460 = x459._1
      val x465 = { x462: (Unit) =>
        val x463 = x440.put(x460, x459)
        x440: Unit
      }
      val x466 = x440.containsKey(x460)
      val x472 = if (x466) {
        val x467 = x440.get(x460)
        val x468 = throw new Exception("Unique constraint violation")
        x468
      } else {
        val x470 = x440.put(x460, x459)
        x440
      }
      val x473 = x417 += x460
      val x461 = x459._2
      val x474 = x461._1
      val x476 = x419 += x474
      val x475 = x461._2
      val x477 = x475._1
      val x479 = x421 += x477
      val x478 = x475._2
      val x480 = x478._1
      val x482 = x423 += x480
      val x481 = x478._2
      val x483 = x481._1
      val x485 = x425 += x483
      val x484 = x481._2
      val x486 = x484._1
      val x488 = x427 += x486
      val x487 = x484._2
      val x489 = x487._1
      val x491 = x429 += x489
      val x490 = x487._2
      val x492 = x431 += x490
      x447: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]
    }
    val x532 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    val x515 = ("", "")
    val x516 = ("", x515)
    val x517 = (0, x516)
    val x518 = (0, x517)
    val x519 = (0, x518)
    val x520 = (' ', x519)
    val x521 = (' ', x520)
    val x522 = (0.0, x521)
    val x523 = (0.0, x522)
    val x524 = (0.0, x523)
    val x525 = (0.0, x524)
    val x526 = (0, x525)
    val x527 = (0, x526)
    val x528 = (0, x527)
    val x530 = (0, x528)
    var x534: Int = 0
    val x537 = while (x534 < 0) {
      val x535 = x532 += x530

      x534 = x534 + 1
    }
    var x1263: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]] = x38
    val x1230 = x0.apply(1)
    val x1231 = x1230.length
    var x1262 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]](x1231)
    for (x1232 <- 0 until x1231) {
      val x1233 = x1230.apply(x1232)
      val x1234 = x1233.apply(0)
      val x1235 = x1234.toInt
      val x1236 = x1233.apply(1)
      val x1237 = x1236.toInt
      val x1238 = x1233.apply(2)
      val x1239 = x1238.charAt(0)
      val x1240 = x1233.apply(3)
      val x1241 = x1240.toDouble
      val x1242 = x1233.apply(4)
      val x1243 = x1242.substring(0, 4)
      val x1244 = x1242.substring(5, 7)
      val x1245 = x1243 + x1244
      val x1246 = x1242.substring(8, 10)
      val x1247 = x1245 + x1246
      val x1248 = x1247.toInt
      val x1249 = x1233.apply(5)
      val x1250 = x1233.apply(6)
      val x1251 = x1233.apply(7)
      val x1252 = x1251.toInt
      val x1253 = x1233.apply(8)
      val x1254 = (x1252, x1253)
      val x1255 = (x1250, x1254)
      val x1256 = (x1249, x1255)
      val x1257 = (x1248, x1256)
      val x1258 = (x1241, x1257)
      val x1259 = (x1239, x1258)
      val x1260 = (x1237, x1259)
      val x1261 = (x1235, x1260)
      x1262(x1232) = x1261
    }
    val x1306 = x1262.foreach {
      x1264 =>
        val x1265 = x1263
        val x1266 = x1265.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]]
        val x1268 = x1264._1
        val x1273 = { x1270: (Unit) =>
          val x1271 = x31.put(x1268, x1264)
          x31: Unit
        }
        val x1274 = x31.containsKey(x1268)
        val x1280 = if (x1274) {
          val x1275 = x31.get(x1268)
          val x1276 = throw new Exception("Unique constraint violation")
          x1276
        } else {
          val x1278 = x31.put(x1268, x1264)
          x31
        }
        val x1281 = x10 += x1268
        val x1269 = x1264._2
        val x1282 = x1269._1
        val x1284 = x12 += x1282
        val x1283 = x1269._2
        val x1285 = x1283._1
        val x1287 = x5 += x1285
        val x1286 = x1283._2
        val x1288 = x1286._1
        val x1290 = x15 += x1288
        val x1289 = x1286._2
        val x1291 = x1289._1
        val x1293 = x17 += x1291
        val x1292 = x1289._2
        val x1294 = x1292._1
        val x1296 = x19 += x1294
        val x1295 = x1292._2
        val x1297 = x1295._1
        val x1299 = x3 += x1297
        val x1298 = x1295._2
        val x1300 = x1298._1
        val x1302 = x21 += x1300
        val x1301 = x1298._2
        val x1303 = x1 += x1301
        x1263 = x38

        ()
    }
    val x1307 = x1263
    val x1308 = x1307._1
    val x1309 = x1307._2
    var x1438: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]] = x447
    val x1415 = x0.apply(0)
    val x1416 = x1415.length
    var x1437 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]](x1416)
    for (x1417 <- 0 until x1416) {
      val x1418 = x1415.apply(x1417)
      val x1419 = x1418.apply(0)
      val x1420 = x1419.toInt
      val x1421 = x1418.apply(1)
      val x1422 = x1418.apply(2)
      val x1423 = x1418.apply(3)
      val x1424 = x1423.toInt
      val x1425 = x1418.apply(4)
      val x1426 = x1418.apply(5)
      val x1427 = x1426.toDouble
      val x1428 = x1418.apply(6)
      val x1429 = x1418.apply(7)
      val x1430 = (x1428, x1429)
      val x1431 = (x1427, x1430)
      val x1432 = (x1425, x1431)
      val x1433 = (x1424, x1432)
      val x1434 = (x1422, x1433)
      val x1435 = (x1421, x1434)
      val x1436 = (x1420, x1435)
      x1437(x1417) = x1436
    }
    val x1478 = x1437.foreach {
      x1439 =>
        val x1440 = x1438
        val x1441 = x1440.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]]
        val x1443 = x1439._1
        val x1448 = { x1445: (Unit) =>
          val x1446 = x440.put(x1443, x1439)
          x440: Unit
        }
        val x1449 = x440.containsKey(x1443)
        val x1455 = if (x1449) {
          val x1450 = x440.get(x1443)
          val x1451 = throw new Exception("Unique constraint violation")
          x1451
        } else {
          val x1453 = x440.put(x1443, x1439)
          x440
        }
        val x1456 = x417 += x1443
        val x1444 = x1439._2
        val x1457 = x1444._1
        val x1459 = x419 += x1457
        val x1458 = x1444._2
        val x1460 = x1458._1
        val x1462 = x421 += x1460
        val x1461 = x1458._2
        val x1463 = x1461._1
        val x1465 = x423 += x1463
        val x1464 = x1461._2
        val x1466 = x1464._1
        val x1468 = x425 += x1466
        val x1467 = x1464._2
        val x1469 = x1467._1
        val x1471 = x427 += x1469
        val x1470 = x1467._2
        val x1472 = x1470._1
        val x1474 = x429 += x1472
        val x1473 = x1470._2
        val x1475 = x431 += x1473
        x1438 = x447

        ()
    }
    val x1479 = x1438
    val x1480 = x1479._1
    val x1481 = x1479._2
    var x1568: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]] = x134
    val x1504 = x0.apply(2)
    val x1505 = x1504.length
    var x1567 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]](x1505)
    for (x1506 <- 0 until x1505) {
      val x1507 = x1504.apply(x1506)
      val x1508 = x1507.apply(0)
      val x1509 = x1508.toInt
      val x1510 = x1507.apply(1)
      val x1511 = x1510.toInt
      val x1512 = x1507.apply(2)
      val x1513 = x1512.toInt
      val x1514 = x1507.apply(3)
      val x1515 = x1514.toInt
      val x1516 = x1507.apply(4)
      val x1517 = x1516.toDouble
      val x1518 = x1507.apply(5)
      val x1519 = x1518.toDouble
      val x1520 = x1507.apply(6)
      val x1521 = x1520.toDouble
      val x1522 = x1507.apply(7)
      val x1523 = x1522.toDouble
      val x1524 = x1507.apply(8)
      val x1525 = x1524.charAt(0)
      val x1526 = x1507.apply(9)
      val x1527 = x1526.charAt(0)
      val x1528 = x1507.apply(10)
      val x1529 = x1528.substring(0, 4)
      val x1530 = x1528.substring(5, 7)
      val x1531 = x1529 + x1530
      val x1532 = x1528.substring(8, 10)
      val x1533 = x1531 + x1532
      val x1534 = x1533.toInt
      val x1535 = x1507.apply(11)
      val x1536 = x1535.substring(0, 4)
      val x1537 = x1535.substring(5, 7)
      val x1538 = x1536 + x1537
      val x1539 = x1535.substring(8, 10)
      val x1540 = x1538 + x1539
      val x1541 = x1540.toInt
      val x1542 = x1507.apply(12)
      val x1543 = x1542.substring(0, 4)
      val x1544 = x1542.substring(5, 7)
      val x1545 = x1543 + x1544
      val x1546 = x1542.substring(8, 10)
      val x1547 = x1545 + x1546
      val x1548 = x1547.toInt
      val x1549 = x1507.apply(13)
      val x1550 = x1507.apply(14)
      val x1551 = x1507.apply(15)
      val x1552 = (x1550, x1551)
      val x1553 = (x1549, x1552)
      val x1554 = (x1548, x1553)
      val x1555 = (x1541, x1554)
      val x1556 = (x1534, x1555)
      val x1557 = (x1527, x1556)
      val x1558 = (x1525, x1557)
      val x1559 = (x1523, x1558)
      val x1560 = (x1521, x1559)
      val x1561 = (x1519, x1560)
      val x1562 = (x1517, x1561)
      val x1563 = (x1515, x1562)
      val x1564 = (x1513, x1563)
      val x1565 = (x1511, x1564)
      val x1566 = (x1509, x1565)
      x1567(x1506) = x1566
    }
    val x1630 = x1567.foreach {
      x1569 =>
        val x1570 = x1568
        val x1571 = x1570.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]]
        val x1573 = x1569._1
        val x1575 = x127.containsKey(x1573)
        val x1583 = if (x1575) {
          val x1576 = x127.get(x1573)
          val x1577 = x1576 += x1569
          x127
        } else {
          val x1579 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
          val x1580 = x1579 += x1569
          val x1581 = x127.put(x1573, x1579)
          x127
        }
        val x1584 = x80 += x1573
        val x1574 = x1569._2
        val x1585 = x1574._1
        val x1587 = x82 += x1585
        val x1586 = x1574._2
        val x1588 = x1586._1
        val x1590 = x84 += x1588
        val x1589 = x1586._2
        val x1591 = x1589._1
        val x1593 = x86 += x1591
        val x1592 = x1589._2
        val x1594 = x1592._1
        val x1596 = x88 += x1594
        val x1595 = x1592._2
        val x1597 = x1595._1
        val x1599 = x90 += x1597
        val x1598 = x1595._2
        val x1600 = x1598._1
        val x1602 = x92 += x1600
        val x1601 = x1598._2
        val x1603 = x1601._1
        val x1605 = x94 += x1603
        val x1604 = x1601._2
        val x1606 = x1604._1
        val x1608 = x96 += x1606
        val x1607 = x1604._2
        val x1609 = x1607._1
        val x1611 = x98 += x1609
        val x1610 = x1607._2
        val x1612 = x1610._1
        val x1614 = x100 += x1612
        val x1613 = x1610._2
        val x1615 = x1613._1
        val x1617 = x102 += x1615
        val x1616 = x1613._2
        val x1618 = x1616._1
        val x1620 = x104 += x1618
        val x1619 = x1616._2
        val x1621 = x1619._1
        val x1623 = x106 += x1621
        val x1622 = x1619._2
        val x1624 = x1622._1
        val x1626 = x108 += x1624
        val x1625 = x1622._2
        val x1627 = x110 += x1625
        x1568 = x134

        ()
    }
    val x1631 = x1568
    val x1632 = x1631._1
    val x1633 = x1631._2
    val x1658 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    val x1635 = x1633._2
    val x1636 = x1635._1
    val x1310 = x1309._1
    val x1313 = x1310._2
    val x1315 = x1313._2
    val x1317 = x1315._2
    val x1319 = x1317._2
    val x1321 = x1319._2
    val x1323 = x1321._2
    val x1324 = x1323._1
    val x1327 = x1324._2
    val x1328 = x1327.result
    val x1329 = x1328.length
    val x1320 = x1319._1
    val x1358 = x1320._2
    val x1359 = x1358.result
    val x1312 = x1310._1
    val x1335 = x1312._2
    val x1336 = x1335.result
    val x1316 = x1315._1
    val x1352 = x1316._2
    val x1353 = x1352.result
    val x1318 = x1317._1
    val x1338 = x1318._2
    val x1339 = x1338.result
    val x1322 = x1321._1
    val x1349 = x1322._2
    val x1350 = x1349.result
    val x1325 = x1323._2
    val x1340 = x1325._1
    val x1346 = x1340._2
    val x1347 = x1346.result
    val x1341 = x1325._2
    val x1343 = x1341._2
    val x1344 = x1343.result
    val x1314 = x1313._1
    val x1355 = x1314._2
    val x1356 = x1355.result
    val x1483 = x1481._2
    val x1484 = x1483._1

    java.lang.System.gc()
    println("Start query")
    val start = now

     var x2117_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]
    for (x1332 <- 0 until x1329) {
      val x1925 = x1359.apply(x1332)
      val x2112 = x1925 >= 19941101
      val x2113 = x1925 < 19950201
      val x2114 = x2112 && x2113
      val x1921 = x1336.apply(x1332)
      val x1923 = x1353.apply(x1332)
      val x1924 = x1339.apply(x1332)
      val x1926 = x1350.apply(x1332)
      val x1927 = x1328.apply(x1332)
      val x1928 = x1347.apply(x1332)
      val x1929 = x1344.apply(x1332)
      val x1930 = (x1928, x1929)
      val x1931 = (x1927, x1930)
      val x1932 = (x1926, x1931)
      val x1933 = (x1925, x1932)
      val x1934 = (x1924, x1933)
      val x1935 = (x1923, x1934)
      val x1922 = x1356.apply(x1332)
      val x1936 = (x1922, x1935)
      val x1937 = (x1921, x1936)
      val x2115 = x1484.get(x1922)
      val x2116 = (x1937, x2115)
      if (x2114) x2117_buf += x2116
    }
    val x2117 = x2117_buf.result
    val x2118 = x2117.foreach {
      x1659 =>
        val x1660 = x1659._1
        val x1662 = x1660._1
        val x1664 = x1636.containsKey(x1662)
        val x1666 = if (x1664) {
          val x1665 = x1636.get(x1662)
          x1665
        } else {
          x532
        }
        val x1667 = x1666.result
        val x1671 = x1667.length
        var x1676 = new Array[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]](x1671)
        for (x1672 <- 0 until x1671) {
          val x1673 = x1667.apply(x1672)
          val x1674 = (x1659, x1673)
          x1676(x1672) = x1674
        }
        val x1680 = x1676.foreach {
          x1677 =>
            val x1678 = x1658 += x1677

            x1678
        }

        x1680
    }
    val x2119 = x1658.result
    val x2120 = x2119.length
    var x1725: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]], Int]]]] = x390
    var x2136_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    for (x1685 <- 0 until x2120) {
      val x2121 = x2119.apply(x1685)
      val x2122 = x2121._2
      val x2123 = x2122._2
      val x2124 = x2123._2
      val x2125 = x2124._2
      val x2126 = x2125._2
      val x2127 = x2126._2
      val x2128 = x2127._2
      val x2129 = x2128._2
      val x2130 = x2129._2
      val x2131 = x2130._1
      val x2132 = java.lang.String.valueOf(x2131)
      val x2133 = x2132 == "R"
      val x2134 = x2121._1
      if (x2133) x2136_buf += x2121
    }
    val x2136 = x2136_buf.result
    val x1711 = x0.apply(3)
    val x1712 = x1711.length
    var x1724 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]](x1712)
    for (x1713 <- 0 until x1712) {
      val x1714 = x1711.apply(x1713)
      val x1715 = x1714.apply(0)
      val x1716 = x1715.toInt
      val x1717 = x1714.apply(1)
      val x1718 = x1714.apply(2)
      val x1719 = x1718.toInt
      val x1720 = x1714.apply(3)
      val x1721 = (x1719, x1720)
      val x1722 = (x1717, x1721)
      val x1723 = (x1716, x1722)
      x1724(x1713) = x1723
    }
    val x2137 = x1724.foreach {
      x1726 =>
        val x1727 = x1725
        val x1728 = x1727.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]], Int]]]]]
        val x1730 = x1726._1
        val x1735 = { x1732: (Unit) =>
          val x1733 = x368.put(x1730, x1726)
          x368: Unit
        }
        val x1736 = x368.containsKey(x1730)
        val x1742 = if (x1736) {
          val x1737 = x368.get(x1730)
          val x1738 = throw new Exception("Unique constraint violation")
          x1738
        } else {
          val x1740 = x368.put(x1730, x1726)
          x368
        }
        val x1743 = x369 += x1730
        val x1731 = x1726._2
        val x1744 = x1731._1
        val x1746 = x371 += x1744
        val x1745 = x1731._2
        val x1747 = x1745._1
        val x1749 = x373 += x1747
        val x1748 = x1745._2
        val x1750 = x375 += x1748
        x1725 = x390

        ()
    }
    val x2138 = x1725
    val x2139 = x2138._1
    val x2140 = x2138._2
    val x2141 = new java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]], Double]()
    val x2142 = x2140._2
    val x2143 = x2142._1
    val x2161 = x2119.foreach {
      x1810 =>
        val x1812 = x1810._2
        val x1814 = x1812._2
        val x1816 = x1814._2
        val x1818 = x1816._2
        val x1820 = x1818._2
        val x1822 = x1820._2
        val x1824 = x1822._2
        val x1826 = x1824._2
        val x1828 = x1826._2
        val x1829 = x1828._1
        val x1831 = java.lang.String.valueOf(x1829)
        val x1832 = x1831 == "R"
        val x2159 = if (x1832) {
          val x1811 = x1810._1
          val x1834 = x1811._2
          val x1835 = x1834._1
          val x1836 = x1834._2
          val x1837 = x1836._1
          val x1838 = x1836._2
          val x1840 = x1838._2
          val x1842 = x1840._2
          val x1844 = x1842._2
          val x1845 = x1844._1
          val x1843 = x1842._1
          val x1841 = x1840._1
          val x2144 = x2143.get(x1841)
          val x2145 = x2144._2
          val x2146 = x2145._1
          val x1839 = x1838._1
          val x1846 = x1844._2
          val x1853 = x1846._2
          val x1854 = (x1839, x1853)
          val x2147 = (x2146, x1854)
          val x2148 = (x1843, x2147)
          val x2149 = (x1845, x2148)
          val x2150 = (x1837, x2149)
          val x2151 = (x1835, x2150)
          val x2152 = x2141.containsKey(x2151)
          val x2156 = if (x2152) {
            val x2153 = x2141.get(x2151)
            val x1823 = x1822._1
            val x1825 = x1824._1
            val x1860 = 1.0 - x1825
            val x1861 = x1823 * x1860
            val x2154 = x2153 + x1861
            x2154
          } else {
            val x1823 = x1822._1
            val x1825 = x1824._1
            val x1860 = 1.0 - x1825
            val x1861 = x1823 * x1860
            x1861
          }
          val x2157 = x2141.put(x2151, x2156)
          x2157
        } else {
          ()
        }

        x2159
    }
    val x2162 = scala.collection.JavaConverters.asScalaSetConverter(x2141.keySet).asScala.toIterable
    val x2163 = x2162.toArray
    val x2164 = x2163.length
    var x2169 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]], Double]](x2164)
    for (x1877 <- 0 until x2164) {
      val x2165 = x2163.apply(x1877)
      val x2166 = x2141.get(x2165)
      val x2167 = (x2165, x2166)
      x2169(x1877) = x2167
    }
    val x2170 = x2169.length
    val x1917 = { x1909: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]) =>
      val x1911 = x1909._2
      val x1913 = x1911._2
      val x1914 = x1913._1
      val x1916 = 0.0 - x1914
      x1916: Double
    }
    var x2194 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]](x2170)
    for (x1884 <- 0 until x2170) {
      val x2171 = x2169.apply(x1884)
      val x2172 = x2171._1
      val x2173 = x2171._2
      val x2174 = x2172._1
      val x2175 = x2172._2
      val x2176 = x2175._1
      val x2177 = x2175._2
      val x2178 = x2177._1
      val x2179 = x2177._2
      val x2180 = x2179._2
      val x2181 = x2180._1
      val x2182 = x2180._2
      val x2183 = x2182._1
      val x2184 = x2179._1
      val x2185 = x2182._2
      val x2186 = (x2184, x2185)
      val x2187 = (x2183, x2186)
      val x2188 = (x2181, x2187)
      val x2189 = (x2178, x2188)
      val x2190 = (x2173, x2189)
      val x2191 = (x2176, x2190)
      val x2192 = (x2174, x2191)
      x2194(x1884) = x2192
    }
    val x2195 = x2194.sortBy(x1917)
    val x1918 = x2195
    println("Elapsed time for selecting " + x1918.length + " records: " + (now - start))   
    x1918
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
val benchmark = new TPCH_Q10_ver_seq()
val result = benchmark(in)
