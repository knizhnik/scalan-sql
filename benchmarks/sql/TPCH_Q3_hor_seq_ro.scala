/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q3_hor_seq extends ((Array[Array[Array[java.lang.String]]])=>(Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]])) {
  def apply(x0: Array[Array[Array[java.lang.String]]]): Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]] = {
    val x108 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
    val x98 = (0, "")
    val x99 = ("", x98)
    val x100 = ("", x99)
    val x101 = (0, x100)
    val x102 = (0.0, x101)
    val x103 = (' ', x102)
    val x104 = (0, x103)
    val x106 = (0, x104)
    var x110: Int = 0
    val x113 = while (x110 < 0) {
      val x111 = x108 += x106

      x110 = x110 + 1
    }
    val x141 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
    val x124 = ("", "")
    val x125 = ("", x124)
    val x126 = (0, x125)
    val x127 = (0, x126)
    val x128 = (0, x127)
    val x129 = (' ', x128)
    val x130 = (' ', x129)
    val x131 = (0.0, x130)
    val x132 = (0.0, x131)
    val x133 = (0.0, x132)
    val x134 = (0.0, x133)
    val x135 = (0, x134)
    val x136 = (0, x135)
    val x137 = (0, x136)
    val x139 = (0, x137)
    var x142: Int = 0
    val x145 = while (x142 < 0) {
      val x143 = x141 += x139

      x142 = x142 + 1
    }
    val x800 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]()
    val x759 = x0.apply(1)
    val x760 = x759.length
    var x802: Int = 0
    val x847 = while (x802 < x760) {
      val x832 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
      val x803 = x759.apply(x802)
      val x806 = x803.apply(0)
      val x807 = x806.toInt
      val x804 = x803.apply(1)
      val x805 = x804.toInt
      val x808 = x803.apply(2)
      val x809 = x808.charAt(0)
      val x810 = x803.apply(3)
      val x811 = x810.toDouble
      val x812 = x803.apply(4)
      val x813 = x812.substring(0, 4)
      val x814 = x812.substring(5, 7)
      val x815 = x813 + x814
      val x816 = x812.substring(8, 10)
      val x817 = x815 + x816
      val x818 = x817.toInt
      val x819 = x803.apply(5)
      val x820 = x803.apply(6)
      val x821 = x803.apply(7)
      val x822 = x821.toInt
      val x823 = x803.apply(8)
      val x824 = (x822, x823)
      val x825 = (x820, x824)
      val x826 = (x819, x825)
      val x827 = (x818, x826)
      val x828 = (x811, x827)
      val x829 = (x809, x828)
      val x830 = (x805, x829)
      val x831 = (x807, x830)
      val x833 = x832 += x831
      val x835 = x800.containsKey(x805)
      val x844 = if (x835) {
        val x836 = x800.get(x805)
        val x838 = x832.result
        val x842 = x838.foreach {
          x839 =>
            val x840 = x836 += x839

            x840
        }
        x836
      } else {
        x832
      }
      val x845 = x800.put(x805, x844)

      x802 = x802 + 1
    }

    val x982 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]()
    val x910 = x0.apply(2)
    val x911 = x910.length
    var x984: Int = 0
    val x1060 = while (x984 < x911) {
      val x1045 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
      val x985 = x910.apply(x984)
      val x986 = x985.apply(0)
      val x987 = x986.toInt
      val x988 = x985.apply(1)
      val x989 = x988.toInt
      val x990 = x985.apply(2)
      val x991 = x990.toInt
      val x992 = x985.apply(3)
      val x993 = x992.toInt
      val x994 = x985.apply(4)
      val x995 = x994.toDouble
      val x996 = x985.apply(5)
      val x997 = x996.toDouble
      val x998 = x985.apply(6)
      val x999 = x998.toDouble
      val x1000 = x985.apply(7)
      val x1001 = x1000.toDouble
      val x1002 = x985.apply(8)
      val x1003 = x1002.charAt(0)
      val x1004 = x985.apply(9)
      val x1005 = x1004.charAt(0)
      val x1006 = x985.apply(10)
      val x1007 = x1006.substring(0, 4)
      val x1008 = x1006.substring(5, 7)
      val x1009 = x1007 + x1008
      val x1010 = x1006.substring(8, 10)
      val x1011 = x1009 + x1010
      val x1012 = x1011.toInt
      val x1013 = x985.apply(11)
      val x1014 = x1013.substring(0, 4)
      val x1015 = x1013.substring(5, 7)
      val x1016 = x1014 + x1015
      val x1017 = x1013.substring(8, 10)
      val x1018 = x1016 + x1017
      val x1019 = x1018.toInt
      val x1020 = x985.apply(12)
      val x1021 = x1020.substring(0, 4)
      val x1022 = x1020.substring(5, 7)
      val x1023 = x1021 + x1022
      val x1024 = x1020.substring(8, 10)
      val x1025 = x1023 + x1024
      val x1026 = x1025.toInt
      val x1027 = x985.apply(13)
      val x1028 = x985.apply(14)
      val x1029 = x985.apply(15)
      val x1030 = (x1028, x1029)
      val x1031 = (x1027, x1030)
      val x1032 = (x1026, x1031)
      val x1033 = (x1019, x1032)
      val x1034 = (x1012, x1033)
      val x1035 = (x1005, x1034)
      val x1036 = (x1003, x1035)
      val x1037 = (x1001, x1036)
      val x1038 = (x999, x1037)
      val x1039 = (x997, x1038)
      val x1040 = (x995, x1039)
      val x1041 = (x993, x1040)
      val x1042 = (x991, x1041)
      val x1043 = (x989, x1042)
      val x1044 = (x987, x1043)
      val x1046 = x1045 += x1044
      val x1048 = x982.containsKey(x987)
      val x1057 = if (x1048) {
        val x1049 = x982.get(x987)
        val x1051 = x1045.result
        val x1055 = x1051.foreach {
          x1052 =>
            val x1053 = x1049 += x1052

            x1053
        }
        x1049
      } else {
        x1045
      }
      val x1058 = x982.put(x987, x1057)

      x984 = x984 + 1
    }

    java.lang.System.gc()
    println("Start query")
    val start = now

    val x867 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
    val x718 = x0.apply(0)
    val x719 = x718.length
    var x1233_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]
    for (x720 <- 0 until x719) {
      val x721 = x718.apply(x720)
      val x722 = x721.apply(0)
      val x723 = x722.toInt
      val x724 = x721.apply(1)
      val x725 = x721.apply(2)
      val x726 = x721.apply(3)
      val x727 = x726.toInt
      val x728 = x721.apply(4)
      val x729 = x721.apply(5)
      val x730 = x729.toDouble
      val x732 = x721.apply(7)
      val x731 = x721.apply(6)
      val x733 = (x731, x732)
      val x734 = (x730, x733)
      val x735 = (x728, x734)
      val x736 = (x727, x735)
      val x737 = (x725, x736)
      val x738 = (x724, x737)
      val x739 = (x723, x738)
      val x1232 = x731 == "HOUSEHOLD"
      if (x1232) x1233_buf += x739
    }
    val x1233 = x1233_buf.result
    val x1234 = x1233.foreach {
      x868 =>
        val x869 = x868._1
        val x871 = x800.containsKey(x869)
        val x874 = if (x871) {
          val x872 = x800.get(x869)
          x872
        } else {
          x108
        }
        val x875 = x874.result
        val x879 = x875.length
        var x884 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]](x879)
        for (x880 <- 0 until x879) {
          val x881 = x875.apply(x880)
          val x882 = (x868, x881)
          x884(x880) = x882
        }
        val x888 = x884.foreach {
          x885 =>
            val x886 = x867 += x885

            x886
        }

        x888
    }
    val x1235 = x867.result
    val x1236 = x1235.length



    val x1082 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    var x1247_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
    for (x893 <- 0 until x1236) {
      val x1237 = x1235.apply(x893)
      val x1238 = x1237._2
      val x1239 = x1238._2
      val x1240 = x1239._2
      val x1241 = x1240._2
      val x1242 = x1241._2
      val x1243 = x1242._1
      val x1244 = x1243 < 19950304
      val x1245 = x1237._1
      if (x1244) x1247_buf += x1237
    }
    val x1247 = x1247_buf.result
    val x1248 = x1247.foreach {
      x1083 =>
        val x1085 = x1083._2
        val x1086 = x1085._1
        val x1088 = x982.containsKey(x1086)
        val x1091 = if (x1088) {
          val x1089 = x982.get(x1086)
          x1089
        } else {
          x141
        }
        val x1092 = x1091.result
        val x1096 = x1092.length
        var x1101 = new Array[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]](x1096)
        for (x1097 <- 0 until x1096) {
          val x1098 = x1092.apply(x1097)
          val x1099 = (x1083, x1098)
          x1101(x1097) = x1099
        }
        val x1105 = x1101.foreach {
          x1102 =>
            val x1103 = x1082 += x1102

            x1103
        }

        x1105
    }
    val x1249 = x1082.result
    val x1250 = x1249.length
    val x1139 = new java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]()
    val x1268 = x1249.foreach {
      x1140 =>
        val x1142 = x1140._2
        val x1144 = x1142._2
        val x1146 = x1144._2
        val x1148 = x1146._2
        val x1150 = x1148._2
        val x1152 = x1150._2
        val x1154 = x1152._2
        val x1156 = x1154._2
        val x1158 = x1156._2
        val x1160 = x1158._2
        val x1162 = x1160._2
        val x1163 = x1162._1
        val x1165 = x1163 > 19950304
        val x1197 = if (x1165) {
          val x1143 = x1142._1
          val x1141 = x1140._1
          val x1167 = x1141._2
          val x1169 = x1167._2
          val x1171 = x1169._2
          val x1173 = x1171._2
          val x1175 = x1173._2
          val x1176 = x1175._1
          val x1177 = x1175._2
          val x1179 = x1177._2
          val x1181 = x1179._2
          val x1182 = x1181._1
          val x1184 = (x1176, x1182)
          val x1185 = (x1143, x1184)
          val x1189 = x1139.containsKey(x1185)
          val x1194 = if (x1189) {
            val x1190 = x1139.get(x1185)
            val x1153 = x1152._1
            val x1155 = x1154._1
            val x1186 = 1.0 - x1155
            val x1187 = x1153 * x1186
            val x1192 = x1190 + x1187
            x1192
          } else {
            val x1153 = x1152._1
            val x1155 = x1154._1
            val x1186 = 1.0 - x1155
            val x1187 = x1153 * x1186
            x1187
          }
          val x1195 = x1139.put(x1185, x1194)
          x1195
        } else {
          ()
        }

        x1197
    }
    val x1269 = scala.collection.JavaConverters.asScalaSetConverter(x1139.keySet).asScala.toIterable
    val x1270 = x1269.toArray
    val x1271 = x1270.length
    var x1276 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]](x1271)
    for (x1203 <- 0 until x1271) {
      val x1272 = x1270.apply(x1203)
      val x1273 = x1139.get(x1272)
      val x1274 = (x1272, x1273)
      x1276(x1203) = x1274
    }
    val x1277 = x1276.length
    var x1267_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
    for (x1110 <- 0 until x1250) {
      val x1251 = x1249.apply(x1110)
      val x1252 = x1251._2
      val x1253 = x1252._2
      val x1254 = x1253._2
      val x1255 = x1254._2
      val x1256 = x1255._2
      val x1257 = x1256._2
      val x1258 = x1257._2
      val x1259 = x1258._2
      val x1260 = x1259._2
      val x1261 = x1260._2
      val x1262 = x1261._2
      val x1263 = x1262._1
      val x1264 = x1263 > 19950304
      val x1265 = x1251._1
      if (x1264) x1267_buf += x1251
    }
    val x1267 = x1267_buf.result
    val x1229 = { x1220: (scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]) =>
      val x1222 = x1220._2
      val x1223 = x1222._1
      val x1225 = 0.0 - x1223
      val x1224 = x1222._2
      val x1226 = x1224._1
      val x1228 = (x1225, x1226)
      x1228: scala.Tuple2[Double, Int]
    }
    var x1286 = new Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]](x1277)
    for (x1210 <- 0 until x1277) {
      val x1278 = x1276.apply(x1210)
      val x1279 = x1278._1
      val x1280 = x1278._2
      val x1281 = x1279._1
      val x1282 = x1279._2
      val x1283 = (x1280, x1282)
      val x1284 = (x1281, x1283)
      x1286(x1210) = x1284
    }
    val x1287 = x1286.sortBy(x1229)
    val x1230 = x1287
    println("Elapsed time for selecting " + x1230.length + " records: " + (now - start))   
    x1230
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
