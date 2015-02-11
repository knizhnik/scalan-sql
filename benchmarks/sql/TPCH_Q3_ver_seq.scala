/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q3_ver_seq extends ((Array[Array[Array[java.lang.String]]])=>(Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]])) {
  def apply(x0: Array[Array[Array[java.lang.String]]]): Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]] = {
    val x63 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x65 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x67 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x69 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x71 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x77 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x79 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x81 = scala.collection.mutable.ArrayBuilder.make[Char]
    val x83 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x89 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]()
    val x101 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]()
    val x78 = ("Orders.o_orderkey", x77)
    val x80 = ("Orders.o_custkey", x79)
    val x82 = ("Orders.o_orderstatus", x81)
    val x64 = ("Orders.o_orderdate", x63)
    val x66 = ("Orders.o_orderpriority", x65)
    val x68 = ("Orders.o_clerk", x67)
    val x70 = ("Orders.o_shippriority", x69)
    val x72 = ("Orders.o_comment", x71)
    val x73 = (x70, x72)
    val x74 = (x68, x73)
    val x75 = (x66, x74)
    val x76 = (x64, x75)
    val x84 = ("Orders.o_totalprice", x83)
    val x85 = (x84, x76)
    val x86 = (x82, x85)
    val x87 = (x80, x86)
    val x88 = (x78, x87)
    val x93 = { x90: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]) =>
      val x91 = x90._1
      x91: Int
    }
    val x94 = (x89, x93)
    val x95 = (x88, x94)
    val x96 = ("Orders.o_orderkey+Orders.o_custkey+Orders.o_orderstatus+Orders.o_totalprice+Orders.o_orderdate+Orders.o_orderpriority+Orders.o_clerk+Orders.o_shippriority+Orders.o_comment.pk", x95)
    val x107 = { x102: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]) =>
      val x104 = x102._2
      val x105 = x104._1
      x105: Int
    }
    val x108 = (x101, x107)
    val x109 = (x96, x108)
    val x110 = ("Orders.o_orderkey+Orders.o_custkey+Orders.o_orderstatus+Orders.o_totalprice+Orders.o_orderdate+Orders.o_orderpriority+Orders.o_clerk+Orders.o_shippriority+Orders.o_comment.pk.sk", x109)
    val x160 = { x111: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]) =>
      val x113 = x111._2
      val x115 = x113._2
      val x116 = x115._1
      val x118 = x101.containsKey(x116)
      val x126 = if (x118) {
        val x119 = x101.get(x116)
        val x120 = x119 += x113
        x101
      } else {
        val x122 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
        val x123 = x122 += x113
        val x124 = x101.put(x116, x122)
        x101
      }
      val x114 = x113._1
      val x130 = { x127: (Unit) =>
        val x128 = x89.put(x114, x113)
        x89: Unit
      }
      val x131 = x89.containsKey(x114)
      val x137 = if (x131) {
        val x132 = x89.get(x114)
        val x133 = throw new Exception("Unique constraint violation")
        x133
      } else {
        val x135 = x89.put(x114, x113)
        x89
      }
      val x138 = x77 += x114
      val x139 = x79 += x116
      val x117 = x115._2
      val x140 = x117._1
      val x142 = x81 += x140
      val x141 = x117._2
      val x143 = x141._1
      val x145 = x83 += x143
      val x144 = x141._2
      val x146 = x144._1
      val x148 = x63 += x146
      val x147 = x144._2
      val x149 = x147._1
      val x151 = x65 += x149
      val x150 = x147._2
      val x152 = x150._1
      val x154 = x67 += x152
      val x153 = x150._2
      val x155 = x153._1
      val x157 = x69 += x155
      val x156 = x153._2
      val x158 = x71 += x156
      x110: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]
    }
    val x161 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x163 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x165 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x167 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x169 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x171 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x173 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x175 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x188 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]()
    val x164 = ("Customer.c_custkey", x163)
    val x166 = ("Customer.c_name", x165)
    val x168 = ("Customer.c_address", x167)
    val x170 = ("Customer.c_nationkey", x169)
    val x172 = ("Customer.c_phone", x171)
    val x162 = ("Customer.c_acctbal", x161)
    val x174 = ("Customer.c_mktsegment", x173)
    val x176 = ("Customer.c_comment", x175)
    val x177 = (x174, x176)
    val x178 = (x162, x177)
    val x179 = (x172, x178)
    val x180 = (x170, x179)
    val x181 = (x168, x180)
    val x182 = (x166, x181)
    val x183 = (x164, x182)
    val x192 = { x189: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]) =>
      val x190 = x189._1
      x190: Int
    }
    val x193 = (x188, x192)
    val x194 = (x183, x193)
    val x195 = ("Customer.c_custkey+Customer.c_name+Customer.c_address+Customer.c_nationkey+Customer.c_phone+Customer.c_acctbal+Customer.c_mktsegment+Customer.c_comment.pk", x194)
    val x233 = { x196: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]) =>
      val x198 = x196._2
      val x199 = x198._1
      val x204 = { x201: (Unit) =>
        val x202 = x188.put(x199, x198)
        x188: Unit
      }
      val x205 = x188.containsKey(x199)
      val x211 = if (x205) {
        val x206 = x188.get(x199)
        val x207 = throw new Exception("Unique constraint violation")
        x207
      } else {
        val x209 = x188.put(x199, x198)
        x188
      }
      val x212 = x163 += x199
      val x200 = x198._2
      val x213 = x200._1
      val x215 = x165 += x213
      val x214 = x200._2
      val x216 = x214._1
      val x218 = x167 += x216
      val x217 = x214._2
      val x219 = x217._1
      val x221 = x169 += x219
      val x220 = x217._2
      val x222 = x220._1
      val x224 = x171 += x222
      val x223 = x220._2
      val x225 = x223._1
      val x227 = x161 += x225
      val x226 = x223._2
      val x228 = x226._1
      val x230 = x173 += x228
      val x229 = x226._2
      val x231 = x175 += x229
      x195: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]
    }
    val x244 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
    val x234 = (0, "")
    val x235 = ("", x234)
    val x236 = ("", x235)
    val x237 = (0, x236)
    val x238 = (0.0, x237)
    val x239 = (' ', x238)
    val x240 = (0, x239)
    val x242 = (0, x240)
    var x246: Int = 0
    val x249 = while (x246 < 0) {
      val x247 = x244 += x242

      x246 = x246 + 1
    }
    val x422 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x424 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x426 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x428 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x430 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x432 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x434 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x436 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x438 = scala.collection.mutable.ArrayBuilder.make[Char]
    val x440 = scala.collection.mutable.ArrayBuilder.make[Char]
    val x442 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x444 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x446 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x448 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x463 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x465 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x469 = new java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]()
    val x488 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]()
    val x464 = ("Lineitem.l_orderkey", x463)
    val x429 = ("Lineitem.l_suppkey", x428)
    val x431 = ("Lineitem.l_linenumber", x430)
    val x433 = ("Lineitem.l_quantity", x432)
    val x435 = ("Lineitem.l_extendedprice", x434)
    val x437 = ("Lineitem.l_discount", x436)
    val x425 = ("Lineitem.l_tax", x424)
    val x439 = ("Lineitem.l_returnflag", x438)
    val x441 = ("Lineitem.l_linestatus", x440)
    val x443 = ("Lineitem.l_shipdate", x442)
    val x423 = ("Lineitem.l_commitdate", x422)
    val x427 = ("Lineitem.l_receiptdate", x426)
    val x445 = ("Lineitem.l_shipinstruct", x444)
    val x447 = ("Lineitem.l_shipmode", x446)
    val x449 = ("Lineitem.l_comment", x448)
    val x450 = (x447, x449)
    val x451 = (x445, x450)
    val x452 = (x427, x451)
    val x453 = (x423, x452)
    val x454 = (x443, x453)
    val x455 = (x441, x454)
    val x456 = (x439, x455)
    val x457 = (x425, x456)
    val x458 = (x437, x457)
    val x459 = (x435, x458)
    val x460 = (x433, x459)
    val x461 = (x431, x460)
    val x462 = (x429, x461)
    val x466 = ("Lineitem.l_partkey", x465)
    val x467 = (x466, x462)
    val x468 = (x464, x467)
    val x480 = { x470: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]) =>
      val x471 = x470._1
      val x472 = x470._2
      val x474 = x472._2
      val x476 = x474._2
      val x477 = x476._1
      val x479 = (x471, x477)
      x479: scala.Tuple2[Int, Int]
    }
    val x481 = (x469, x480)
    val x482 = (x468, x481)
    val x483 = ("Lineitem.l_orderkey+Lineitem.l_partkey+Lineitem.l_suppkey+Lineitem.l_linenumber+Lineitem.l_quantity+Lineitem.l_extendedprice+Lineitem.l_discount+Lineitem.l_tax+Lineitem.l_returnflag+Lineitem.l_linestatus+Lineitem.l_shipdate+Lineitem.l_commitdate+Lineitem.l_receiptdate+Lineitem.l_shipinstruct+Lineitem.l_shipmode+Lineitem.l_comment.pk", x482)
    val x492 = { x489: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]) =>
      val x490 = x489._1
      x490: Int
    }
    val x493 = (x488, x492)
    val x494 = (x483, x493)
    val x495 = ("Lineitem.l_orderkey+Lineitem.l_partkey+Lineitem.l_suppkey+Lineitem.l_linenumber+Lineitem.l_quantity+Lineitem.l_extendedprice+Lineitem.l_discount+Lineitem.l_tax+Lineitem.l_returnflag+Lineitem.l_linestatus+Lineitem.l_shipdate+Lineitem.l_commitdate+Lineitem.l_receiptdate+Lineitem.l_shipinstruct+Lineitem.l_shipmode+Lineitem.l_comment.pk.sk", x494)
    val x567 = { x496: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]) =>
      val x498 = x496._2
      val x499 = x498._1
      val x501 = x488.containsKey(x499)
      val x509 = if (x501) {
        val x502 = x488.get(x499)
        val x503 = x502 += x498
        x488
      } else {
        val x505 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
        val x506 = x505 += x498
        val x507 = x488.put(x499, x505)
        x488
      }
      val x500 = x498._2
      val x511 = x500._2
      val x513 = x511._2
      val x514 = x513._1
      val x516 = (x499, x514)
      val x520 = { x517: (Unit) =>
        val x518 = x469.put(x516, x498)
        x469: Unit
      }
      val x521 = x469.containsKey(x516)
      val x527 = if (x521) {
        val x522 = x469.get(x516)
        val x523 = throw new Exception("Unique constraint violation")
        x523
      } else {
        val x525 = x469.put(x516, x498)
        x469
      }
      val x528 = x463 += x499
      val x510 = x500._1
      val x529 = x465 += x510
      val x512 = x511._1
      val x530 = x428 += x512
      val x531 = x430 += x514
      val x515 = x513._2
      val x532 = x515._1
      val x534 = x432 += x532
      val x533 = x515._2
      val x535 = x533._1
      val x537 = x434 += x535
      val x536 = x533._2
      val x538 = x536._1
      val x540 = x436 += x538
      val x539 = x536._2
      val x541 = x539._1
      val x543 = x424 += x541
      val x542 = x539._2
      val x544 = x542._1
      val x546 = x438 += x544
      val x545 = x542._2
      val x547 = x545._1
      val x549 = x440 += x547
      val x548 = x545._2
      val x550 = x548._1
      val x552 = x442 += x550
      val x551 = x548._2
      val x553 = x551._1
      val x555 = x422 += x553
      val x554 = x551._2
      val x556 = x554._1
      val x558 = x426 += x556
      val x557 = x554._2
      val x559 = x557._1
      val x561 = x444 += x559
      val x560 = x557._2
      val x562 = x560._1
      val x564 = x446 += x562
      val x563 = x560._2
      val x565 = x448 += x563
      x495: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]
    }
    val x585 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    val x568 = ("", "")
    val x569 = ("", x568)
    val x570 = (0, x569)
    val x571 = (0, x570)
    val x572 = (0, x571)
    val x573 = (' ', x572)
    val x574 = (' ', x573)
    val x575 = (0.0, x574)
    val x576 = (0.0, x575)
    val x577 = (0.0, x576)
    val x578 = (0.0, x577)
    val x579 = (0, x578)
    val x580 = (0, x579)
    val x581 = (0, x580)
    val x583 = (0, x581)
    var x586: Int = 0
    val x589 = while (x586 < 0) {
      val x587 = x585 += x583

      x586 = x586 + 1
    }
    var x1248: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]] = x195
    val x1225 = x0.apply(0)
    val x1226 = x1225.length
    var x1247 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]](x1226)
    for (x1227 <- 0 until x1226) {
      val x1228 = x1225.apply(x1227)
      val x1229 = x1228.apply(0)
      val x1230 = x1229.toInt
      val x1231 = x1228.apply(1)
      val x1232 = x1228.apply(2)
      val x1233 = x1228.apply(3)
      val x1234 = x1233.toInt
      val x1235 = x1228.apply(4)
      val x1236 = x1228.apply(5)
      val x1237 = x1236.toDouble
      val x1238 = x1228.apply(6)
      val x1239 = x1228.apply(7)
      val x1240 = (x1238, x1239)
      val x1241 = (x1237, x1240)
      val x1242 = (x1235, x1241)
      val x1243 = (x1234, x1242)
      val x1244 = (x1232, x1243)
      val x1245 = (x1231, x1244)
      val x1246 = (x1230, x1245)
      x1247(x1227) = x1246
    }
    val x1288 = x1247.foreach {
      x1249 =>
        val x1250 = x1248
        val x1251 = x1250.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]]
        val x1253 = x1249._1
        val x1258 = { x1255: (Unit) =>
          val x1256 = x188.put(x1253, x1249)
          x188: Unit
        }
        val x1259 = x188.containsKey(x1253)
        val x1265 = if (x1259) {
          val x1260 = x188.get(x1253)
          val x1261 = throw new Exception("Unique constraint violation")
          x1261
        } else {
          val x1263 = x188.put(x1253, x1249)
          x188
        }
        val x1266 = x163 += x1253
        val x1254 = x1249._2
        val x1267 = x1254._1
        val x1269 = x165 += x1267
        val x1268 = x1254._2
        val x1270 = x1268._1
        val x1272 = x167 += x1270
        val x1271 = x1268._2
        val x1273 = x1271._1
        val x1275 = x169 += x1273
        val x1274 = x1271._2
        val x1276 = x1274._1
        val x1278 = x171 += x1276
        val x1277 = x1274._2
        val x1279 = x1277._1
        val x1281 = x161 += x1279
        val x1280 = x1277._2
        val x1282 = x1280._1
        val x1284 = x173 += x1282
        val x1283 = x1280._2
        val x1285 = x175 += x1283
        x1248 = x195

        ()
    }
    val x1289 = x1248
    val x1290 = x1289._1
    val x1291 = x1289._2
    var x1423: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]] = x110
    val x1390 = x0.apply(1)
    val x1391 = x1390.length
    var x1422 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]](x1391)
    for (x1392 <- 0 until x1391) {
      val x1393 = x1390.apply(x1392)
      val x1394 = x1393.apply(0)
      val x1395 = x1394.toInt
      val x1396 = x1393.apply(1)
      val x1397 = x1396.toInt
      val x1398 = x1393.apply(2)
      val x1399 = x1398.charAt(0)
      val x1400 = x1393.apply(3)
      val x1401 = x1400.toDouble
      val x1402 = x1393.apply(4)
      val x1403 = x1402.substring(0, 4)
      val x1404 = x1402.substring(5, 7)
      val x1405 = x1403 + x1404
      val x1406 = x1402.substring(8, 10)
      val x1407 = x1405 + x1406
      val x1408 = x1407.toInt
      val x1409 = x1393.apply(5)
      val x1410 = x1393.apply(6)
      val x1411 = x1393.apply(7)
      val x1412 = x1411.toInt
      val x1413 = x1393.apply(8)
      val x1414 = (x1412, x1413)
      val x1415 = (x1410, x1414)
      val x1416 = (x1409, x1415)
      val x1417 = (x1408, x1416)
      val x1418 = (x1401, x1417)
      val x1419 = (x1399, x1418)
      val x1420 = (x1397, x1419)
      val x1421 = (x1395, x1420)
      x1422(x1392) = x1421
    }
    val x1475 = x1422.foreach {
      x1424 =>
        val x1425 = x1423
        val x1426 = x1425.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]]
        val x1429 = x1424._2
        val x1430 = x1429._1
        val x1432 = x101.containsKey(x1430)
        val x1440 = if (x1432) {
          val x1433 = x101.get(x1430)
          val x1434 = x1433 += x1424
          x101
        } else {
          val x1436 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
          val x1437 = x1436 += x1424
          val x1438 = x101.put(x1430, x1436)
          x101
        }
        val x1428 = x1424._1
        val x1444 = { x1441: (Unit) =>
          val x1442 = x89.put(x1428, x1424)
          x89: Unit
        }
        val x1445 = x89.containsKey(x1428)
        val x1451 = if (x1445) {
          val x1446 = x89.get(x1428)
          val x1447 = throw new Exception("Unique constraint violation")
          x1447
        } else {
          val x1449 = x89.put(x1428, x1424)
          x89
        }
        val x1452 = x77 += x1428
        val x1453 = x79 += x1430
        val x1431 = x1429._2
        val x1454 = x1431._1
        val x1456 = x81 += x1454
        val x1455 = x1431._2
        val x1457 = x1455._1
        val x1459 = x83 += x1457
        val x1458 = x1455._2
        val x1460 = x1458._1
        val x1462 = x63 += x1460
        val x1461 = x1458._2
        val x1463 = x1461._1
        val x1465 = x65 += x1463
        val x1464 = x1461._2
        val x1466 = x1464._1
        val x1468 = x67 += x1466
        val x1467 = x1464._2
        val x1469 = x1467._1
        val x1471 = x69 += x1469
        val x1470 = x1467._2
        val x1472 = x71 += x1470
        x1423 = x110

        ()
    }
    val x1476 = x1423
    val x1477 = x1476._1
    val x1478 = x1476._2
    val x1501 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
    val x1480 = x1478._2
    val x1481 = x1480._1
    val x1292 = x1291._1
    val x1295 = x1292._2
    val x1297 = x1295._2
    val x1299 = x1297._2
    val x1301 = x1299._2
    val x1303 = x1301._2
    val x1304 = x1303._1
    val x1307 = x1304._2
    val x1308 = x1307.result
    val x1309 = x1308.length
    val x1294 = x1292._1
    val x1335 = x1294._2
    val x1336 = x1335.result
    val x1296 = x1295._1
    val x1329 = x1296._2
    val x1330 = x1329.result
    val x1298 = x1297._1
    val x1332 = x1298._2
    val x1333 = x1332.result
    val x1300 = x1299._1
    val x1315 = x1300._2
    val x1316 = x1315.result
    val x1302 = x1301._1
    val x1323 = x1302._2
    val x1324 = x1323.result
    val x1305 = x1303._2
    val x1318 = x1305._2
    val x1326 = x1318._2
    val x1327 = x1326.result
    val x1317 = x1305._1
    val x1320 = x1317._2
    val x1321 = x1320.result
    var x1967_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]
    for (x1312 <- 0 until x1309) {
      val x1858 = x1336.apply(x1312)
      val x1859 = x1330.apply(x1312)
      val x1860 = x1333.apply(x1312)
      val x1861 = x1316.apply(x1312)
      val x1862 = x1324.apply(x1312)
      val x1863 = x1308.apply(x1312)
      val x1865 = x1327.apply(x1312)
      val x1864 = x1321.apply(x1312)
      val x1866 = (x1864, x1865)
      val x1867 = (x1863, x1866)
      val x1868 = (x1862, x1867)
      val x1869 = (x1861, x1868)
      val x1870 = (x1860, x1869)
      val x1871 = (x1859, x1870)
      val x1872 = (x1858, x1871)
      val x1966 = x1864 == "HOUSEHOLD"
      if (x1966) x1967_buf += x1872
    }
    val x1967 = x1967_buf.result
    val x1968 = x1967.foreach {
      x1502 =>
        val x1503 = x1502._1
        val x1505 = x1481.containsKey(x1503)
        val x1507 = if (x1505) {
          val x1506 = x1481.get(x1503)
          x1506
        } else {
          x244
        }
        val x1508 = x1507.result
        val x1512 = x1508.length
        var x1517 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]](x1512)
        for (x1513 <- 0 until x1512) {
          val x1514 = x1508.apply(x1513)
          val x1515 = (x1502, x1514)
          x1517(x1513) = x1515
        }
        val x1521 = x1517.foreach {
          x1518 =>
            val x1519 = x1501 += x1518

            x1519
        }

        x1521
    }
    val x1969 = x1501.result
    val x1970 = x1969.length
    var x1607: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]] = x495
    var x1981_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
    for (x1526 <- 0 until x1970) {
      val x1971 = x1969.apply(x1526)
      val x1972 = x1971._2
      val x1973 = x1972._2
      val x1974 = x1973._2
      val x1975 = x1974._2
      val x1976 = x1975._2
      val x1977 = x1976._1
      val x1978 = x1977 < 19950304
      val x1979 = x1971._1
      if (x1978) x1981_buf += x1971
    }
    val x1981 = x1981_buf.result
    val x1543 = x0.apply(2)
    val x1544 = x1543.length
    var x1606 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]](x1544)
    for (x1545 <- 0 until x1544) {
      val x1546 = x1543.apply(x1545)
      val x1547 = x1546.apply(0)
      val x1548 = x1547.toInt
      val x1549 = x1546.apply(1)
      val x1550 = x1549.toInt
      val x1551 = x1546.apply(2)
      val x1552 = x1551.toInt
      val x1553 = x1546.apply(3)
      val x1554 = x1553.toInt
      val x1555 = x1546.apply(4)
      val x1556 = x1555.toDouble
      val x1557 = x1546.apply(5)
      val x1558 = x1557.toDouble
      val x1559 = x1546.apply(6)
      val x1560 = x1559.toDouble
      val x1561 = x1546.apply(7)
      val x1562 = x1561.toDouble
      val x1563 = x1546.apply(8)
      val x1564 = x1563.charAt(0)
      val x1565 = x1546.apply(9)
      val x1566 = x1565.charAt(0)
      val x1567 = x1546.apply(10)
      val x1568 = x1567.substring(0, 4)
      val x1569 = x1567.substring(5, 7)
      val x1570 = x1568 + x1569
      val x1571 = x1567.substring(8, 10)
      val x1572 = x1570 + x1571
      val x1573 = x1572.toInt
      val x1574 = x1546.apply(11)
      val x1575 = x1574.substring(0, 4)
      val x1576 = x1574.substring(5, 7)
      val x1577 = x1575 + x1576
      val x1578 = x1574.substring(8, 10)
      val x1579 = x1577 + x1578
      val x1580 = x1579.toInt
      val x1581 = x1546.apply(12)
      val x1582 = x1581.substring(0, 4)
      val x1583 = x1581.substring(5, 7)
      val x1584 = x1582 + x1583
      val x1585 = x1581.substring(8, 10)
      val x1586 = x1584 + x1585
      val x1587 = x1586.toInt
      val x1588 = x1546.apply(13)
      val x1589 = x1546.apply(14)
      val x1590 = x1546.apply(15)
      val x1591 = (x1589, x1590)
      val x1592 = (x1588, x1591)
      val x1593 = (x1587, x1592)
      val x1594 = (x1580, x1593)
      val x1595 = (x1573, x1594)
      val x1596 = (x1566, x1595)
      val x1597 = (x1564, x1596)
      val x1598 = (x1562, x1597)
      val x1599 = (x1560, x1598)
      val x1600 = (x1558, x1599)
      val x1601 = (x1556, x1600)
      val x1602 = (x1554, x1601)
      val x1603 = (x1552, x1602)
      val x1604 = (x1550, x1603)
      val x1605 = (x1548, x1604)
      x1606(x1545) = x1605
    }
    val x1982 = x1606.foreach {
      x1608 =>
        val x1609 = x1607
        val x1610 = x1609.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]]
        val x1612 = x1608._1
        val x1614 = x488.containsKey(x1612)
        val x1622 = if (x1614) {
          val x1615 = x488.get(x1612)
          val x1616 = x1615 += x1608
          x488
        } else {
          val x1618 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
          val x1619 = x1618 += x1608
          val x1620 = x488.put(x1612, x1618)
          x488
        }
        val x1613 = x1608._2
        val x1624 = x1613._2
        val x1626 = x1624._2
        val x1627 = x1626._1
        val x1629 = (x1612, x1627)
        val x1633 = { x1630: (Unit) =>
          val x1631 = x469.put(x1629, x1608)
          x469: Unit
        }
        val x1634 = x469.containsKey(x1629)
        val x1640 = if (x1634) {
          val x1635 = x469.get(x1629)
          val x1636 = throw new Exception("Unique constraint violation")
          x1636
        } else {
          val x1638 = x469.put(x1629, x1608)
          x469
        }
        val x1641 = x463 += x1612
        val x1623 = x1613._1
        val x1642 = x465 += x1623
        val x1625 = x1624._1
        val x1643 = x428 += x1625
        val x1644 = x430 += x1627
        val x1628 = x1626._2
        val x1645 = x1628._1
        val x1647 = x432 += x1645
        val x1646 = x1628._2
        val x1648 = x1646._1
        val x1650 = x434 += x1648
        val x1649 = x1646._2
        val x1651 = x1649._1
        val x1653 = x436 += x1651
        val x1652 = x1649._2
        val x1654 = x1652._1
        val x1656 = x424 += x1654
        val x1655 = x1652._2
        val x1657 = x1655._1
        val x1659 = x438 += x1657
        val x1658 = x1655._2
        val x1660 = x1658._1
        val x1662 = x440 += x1660
        val x1661 = x1658._2
        val x1663 = x1661._1
        val x1665 = x442 += x1663
        val x1664 = x1661._2
        val x1666 = x1664._1
        val x1668 = x422 += x1666
        val x1667 = x1664._2
        val x1669 = x1667._1
        val x1671 = x426 += x1669
        val x1670 = x1667._2
        val x1672 = x1670._1
        val x1674 = x444 += x1672
        val x1673 = x1670._2
        val x1675 = x1673._1
        val x1677 = x446 += x1675
        val x1676 = x1673._2
        val x1678 = x448 += x1676
        x1607 = x495

        ()
    }

    java.lang.System.gc()
    println("Start query")
    val start = now

    val x1983 = x1607
    val x1984 = x1983._1
    val x1985 = x1983._2
    val x1709 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    val x1986 = x1985._2
    val x1987 = x1986._1
    val x1999 = x1981.foreach {
      x1710 =>
        val x1712 = x1710._2
        val x1713 = x1712._1
        val x1988 = x1987.containsKey(x1713)
        val x1990 = if (x1988) {
          val x1989 = x1987.get(x1713)
          x1989
        } else {
          x585
        }
        val x1991 = x1990.result
        val x1992 = x1991.length
        var x1996 = new Array[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]](x1992)
        for (x1723 <- 0 until x1992) {
          val x1993 = x1991.apply(x1723)
          val x1994 = (x1710, x1993)
          x1996(x1723) = x1994
        }
        val x1997 = x1996.foreach {
          x1728 =>
            val x1729 = x1709 += x1728

            x1729
        }

        x1997
    }
    val x2000 = x1709.result
    val x2001 = x2000.length
    val x2002 = new java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]()
    val x2012 = x2000.foreach {
      x1766 =>
        val x1768 = x1766._2
        val x1770 = x1768._2
        val x1772 = x1770._2
        val x1774 = x1772._2
        val x1776 = x1774._2
        val x1778 = x1776._2
        val x1780 = x1778._2
        val x1782 = x1780._2
        val x1784 = x1782._2
        val x1786 = x1784._2
        val x1788 = x1786._2
        val x1789 = x1788._1
        val x1791 = x1789 > 19950304
        val x2010 = if (x1791) {
          val x1769 = x1768._1
          val x1767 = x1766._1
          val x1793 = x1767._2
          val x1795 = x1793._2
          val x1797 = x1795._2
          val x1799 = x1797._2
          val x1801 = x1799._2
          val x1802 = x1801._1
          val x1803 = x1801._2
          val x1805 = x1803._2
          val x1807 = x1805._2
          val x1808 = x1807._1
          val x1810 = (x1802, x1808)
          val x1811 = (x1769, x1810)
          val x2003 = x2002.containsKey(x1811)
          val x2007 = if (x2003) {
            val x2004 = x2002.get(x1811)
            val x1779 = x1778._1
            val x1781 = x1780._1
            val x1812 = 1.0 - x1781
            val x1813 = x1779 * x1812
            val x2005 = x2004 + x1813
            x2005
          } else {
            val x1779 = x1778._1
            val x1781 = x1780._1
            val x1812 = 1.0 - x1781
            val x1813 = x1779 * x1812
            x1813
          }
          val x2008 = x2002.put(x1811, x2007)
          x2008
        } else {
          ()
        }

        x2010
    }
    val x2013 = scala.collection.JavaConverters.asScalaSetConverter(x2002.keySet).asScala.toIterable
    val x2014 = x2013.toArray
    val x2015 = x2014.length
    var x2020 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]](x2015)
    for (x1829 <- 0 until x2015) {
      val x2016 = x2014.apply(x1829)
      val x2017 = x2002.get(x2016)
      val x2018 = (x2016, x2017)
      x2020(x1829) = x2018
    }
    val x2021 = x2020.length
    var x2048_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    for (x1736 <- 0 until x2001) {
      val x2032 = x2000.apply(x1736)
      val x2033 = x2032._2
      val x2034 = x2033._2
      val x2035 = x2034._2
      val x2036 = x2035._2
      val x2037 = x2036._2
      val x2038 = x2037._2
      val x2039 = x2038._2
      val x2040 = x2039._2
      val x2041 = x2040._2
      val x2042 = x2041._2
      val x2043 = x2042._2
      val x2044 = x2043._1
      val x2045 = x2044 > 19950304
      val x2046 = x2032._1
      if (x2045) x2048_buf += x2032
    }
    val x2048 = x2048_buf.result
    val x1855 = { x1846: (scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]) =>
      val x1848 = x1846._2
      val x1849 = x1848._1
      val x1851 = 0.0 - x1849
      val x1850 = x1848._2
      val x1852 = x1850._1
      val x1854 = (x1851, x1852)
      x1854: scala.Tuple2[Double, Int]
    }
    var x2030 = new Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]](x2021)
    for (x1836 <- 0 until x2021) {
      val x2022 = x2020.apply(x1836)
      val x2023 = x2022._1
      val x2024 = x2022._2
      val x2025 = x2023._1
      val x2026 = x2023._2
      val x2027 = (x2024, x2026)
      val x2028 = (x2025, x2027)
      x2030(x1836) = x2028
    }
    val x2031 = x2030.sortBy(x1855)
    val x1856 = x2031
    println("Elapsed time: " + (now - start))   
    x1856
  }
}
/*****************************************
  End of Generated Code                  
*******************************************/
def now: Long = java.lang.System.currentTimeMillis()
val in = Array(scala.io.Source.fromFile("~/tpch-data/sf1/customer.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("~/tpch-data/sf1/orders.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("~/tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray)    
val benchmark = new TPCH_Q3_ver_seq()
val result = benchmark(in)
