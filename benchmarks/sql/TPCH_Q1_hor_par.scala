/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q1_hor_par extends ((Array[Array[java.lang.String]])=>(Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]])) {
	def apply(x0: Array[Array[java.lang.String]]): Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]] = {
		val x196 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]()
		val x185 = (' ', ' ')
		val x186 = (0.0, 0)
		val x187 = (0.0, x186)
		val x188 = (0.0, x187)
		val x189 = (0.0, x188)
		val x190 = (0.0, x189)
		val x191 = (0.0, x190)
		val x192 = (0.0, x191)
		var x198: Int = 0
		val x201 = while (x198 < 0) {
			val x199 = x196.put(x185, x192)

			x198 = x198 + 1
		}
		val x781 = x0.length
		var x843 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]](x781)
		for (x782 <- 0 until x781) {
			val x783 = x0.apply(x782)
			val x784 = x783.apply(0)
			val x785 = x784.toInt
			val x786 = x783.apply(1)
			val x787 = x786.toInt
			val x788 = x783.apply(2)
			val x789 = x788.toInt
			val x790 = x783.apply(3)
			val x791 = x790.toInt
			val x792 = x783.apply(4)
			val x793 = x792.toDouble
			val x794 = x783.apply(5)
			val x795 = x794.toDouble
			val x796 = x783.apply(6)
			val x797 = x796.toDouble
			val x798 = x783.apply(7)
			val x799 = x798.toDouble
			val x800 = x783.apply(8)
			val x801 = x800.charAt(0)
			val x802 = x783.apply(9)
			val x803 = x802.charAt(0)
			val x804 = x783.apply(10)
			val x805 = x804.substring(0, 4)
			val x806 = x804.substring(5, 7)
			val x807 = x805 + x806
			val x808 = x804.substring(8, 10)
			val x809 = x807 + x808
			val x810 = x809.toInt
			val x811 = x783.apply(11)
			val x812 = x811.substring(0, 4)
			val x813 = x811.substring(5, 7)
			val x814 = x812 + x813
			val x815 = x811.substring(8, 10)
			val x816 = x814 + x815
			val x817 = x816.toInt
			val x818 = x783.apply(12)
			val x819 = x818.substring(0, 4)
			val x820 = x818.substring(5, 7)
			val x821 = x819 + x820
			val x822 = x818.substring(8, 10)
			val x823 = x821 + x822
			val x824 = x823.toInt
			val x825 = x783.apply(13)
			val x826 = x783.apply(14)
			val x827 = x783.apply(15)
			val x828 = (x826, x827)
			val x829 = (x825, x828)
			val x830 = (x824, x829)
			val x831 = (x817, x830)
			val x832 = (x810, x831)
			val x833 = (x803, x832)
			val x834 = (x801, x833)
			val x835 = (x799, x834)
			val x836 = (x797, x835)
			val x837 = (x795, x836)
			val x838 = (x793, x837)
			val x839 = (x791, x838)
			val x840 = (x789, x839)
			val x841 = (x787, x840)
			val x842 = (x785, x841)
			x843(x782) = x842
		}
		val x909 = { x877: (Int) =>
			val x894 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
			val x893 = java.lang.String.valueOf(x877)
			val x895 = (x893, x894)
			var x896: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]] = x895
			var x892_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
			for (x885 <- 0 until x781) {
				val x886 = x843.apply(x885)
				val x887 = x886._1
				val x889 = x887.hashCode
				val x890 = x889 % 4
				val x891 = x890 == x877
				if (x891) x892_buf += x886
			}
			val x892 = x892_buf.result
			val x906 = x892.foreach {
				x897 =>
					val x898 = x896
					val x899 = x898.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]]
					val x901 = x899._1
					val x902 = x899._2
					val x903 = x902 += x897
					x896 = x899

					()
			}
			val x907 = x896
			x907: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
		}
		// generating parallel execute
		val x910 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until 4) yield scala.concurrent.future {
				x909(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}

        java.lang.System.gc()
        println("Start query")
        val start = now

		val x1124 = { x1018: (Int) =>
			val x1019 = x910.apply(x1018)
			val x1020 = x1019._1
			val x1021 = x1019._2
			val x1050 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]()
			val x1022 = x1021.result
			val x1023 = x1022.length
			var x1049_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
			for (x1024 <- 0 until x1023) {
				val x1025 = x1022.apply(x1024)
				val x1027 = x1025._2
				val x1029 = x1027._2
				val x1031 = x1029._2
				val x1033 = x1031._2
				val x1035 = x1033._2
				val x1037 = x1035._2
				val x1039 = x1037._2
				val x1041 = x1039._2
				val x1043 = x1041._2
				val x1045 = x1043._2
				val x1046 = x1045._1
				val x1048 = x1046 <= 19981201
				if (x1048) x1049_buf += x1025
			}
			val x1049 = x1049_buf.result
			val x1122 = x1049.foreach {
				x1051 =>
					val x1053 = x1051._2
					val x1055 = x1053._2
					val x1057 = x1055._2
					val x1059 = x1057._2
					val x1061 = x1059._2
					val x1063 = x1061._2
					val x1065 = x1063._2
					val x1067 = x1065._2
					val x1068 = x1067._1
					val x1069 = x1067._2
					val x1070 = x1069._1
					val x1072 = (x1068, x1070)
					val x1086 = x1050.containsKey(x1072)
					val x1119 = if (x1086) {
						val x1087 = x1050.get(x1072)
						val x1089 = x1087._1
						val x1090 = x1087._2
						val x1060 = x1059._1
						val x1091 = x1089 + x1060
						val x1062 = x1061._1
						val x1092 = x1090._1
						val x1094 = x1092 + x1062
						val x1064 = x1063._1
						val x1073 = 1.0 - x1064
						val x1074 = x1062 * x1073
						val x1093 = x1090._2
						val x1095 = x1093._1
						val x1097 = x1095 + x1074
						val x1066 = x1065._1
						val x1075 = 1.0 + x1066
						val x1076 = x1073 * x1075
						val x1077 = x1062 * x1076
						val x1096 = x1093._2
						val x1098 = x1096._1
						val x1100 = x1098 + x1077
						val x1099 = x1096._2
						val x1101 = x1099._1
						val x1103 = x1101 + x1060
						val x1102 = x1099._2
						val x1104 = x1102._1
						val x1106 = x1104 + x1062
						val x1105 = x1102._2
						val x1107 = x1105._1
						val x1109 = x1107 + x1064
						val x1108 = x1105._2
						val x1110 = x1108 + 1
						val x1111 = (x1109, x1110)
						val x1112 = (x1106, x1111)
						val x1113 = (x1103, x1112)
						val x1114 = (x1100, x1113)
						val x1115 = (x1097, x1114)
						val x1116 = (x1094, x1115)
						val x1117 = (x1091, x1116)
						x1117
					} else {
						val x1060 = x1059._1
						val x1062 = x1061._1
						val x1064 = x1063._1
						val x1073 = 1.0 - x1064
						val x1074 = x1062 * x1073
						val x1066 = x1065._1
						val x1075 = 1.0 + x1066
						val x1076 = x1073 * x1075
						val x1077 = x1062 * x1076
						val x1078 = (x1064, 1)
						val x1079 = (x1062, x1078)
						val x1080 = (x1060, x1079)
						val x1081 = (x1077, x1080)
						val x1082 = (x1074, x1081)
						val x1083 = (x1062, x1082)
						val x1084 = (x1060, x1083)
						x1084
					}
					val x1120 = x1050.put(x1072, x1119)

					x1120
			}
			x1050: java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]
		}
		// generating parallel execute
		val x1125 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until 4) yield scala.concurrent.future {
				x1124(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		var x1126: java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]] = x196
		val x1200 = x1125.foreach {
			x1127 =>
				val x1128 = x1126
				val x1129 = x1128.asInstanceOf[java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]
				val x1131 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]()
				val x1132 = scala.collection.JavaConverters.asScalaSetConverter(x1129.keySet).asScala.toIterable
				val x1187 = x1132.foreach {
					x1133 =>
						val x1134 = x1127.containsKey(x1133)
						val x1184 = if (x1134) {
							val x1135 = x1129.get(x1133)
							val x1138 = x1135._1
							val x1139 = x1135._2
							val x1136 = x1127.get(x1133)
							val x1140 = x1136._1
							val x1142 = x1138 + x1140
							val x1143 = x1139._1
							val x1141 = x1136._2
							val x1145 = x1141._1
							val x1147 = x1143 + x1145
							val x1144 = x1139._2
							val x1148 = x1144._1
							val x1146 = x1141._2
							val x1150 = x1146._1
							val x1152 = x1148 + x1150
							val x1149 = x1144._2
							val x1153 = x1149._1
							val x1151 = x1146._2
							val x1155 = x1151._1
							val x1157 = x1153 + x1155
							val x1154 = x1149._2
							val x1158 = x1154._1
							val x1156 = x1151._2
							val x1160 = x1156._1
							val x1162 = x1158 + x1160
							val x1159 = x1154._2
							val x1163 = x1159._1
							val x1161 = x1156._2
							val x1165 = x1161._1
							val x1167 = x1163 + x1165
							val x1164 = x1159._2
							val x1168 = x1164._1
							val x1166 = x1161._2
							val x1170 = x1166._1
							val x1172 = x1168 + x1170
							val x1169 = x1164._2
							val x1171 = x1166._2
							val x1173 = x1169 + x1171
							val x1174 = (x1172, x1173)
							val x1175 = (x1167, x1174)
							val x1176 = (x1162, x1175)
							val x1177 = (x1157, x1176)
							val x1178 = (x1152, x1177)
							val x1179 = (x1147, x1178)
							val x1180 = (x1142, x1179)
							x1180
						} else {
							val x1182 = x1129.get(x1133)
							x1182
						}
						val x1185 = x1131.put(x1133, x1184)

						x1185
				}
				val x1188 = scala.collection.JavaConverters.asScalaSetConverter(x1127.keySet).asScala.toIterable
				val x1197 = x1188.foreach {
					x1189 =>
						val x1190 = x1129.containsKey(x1189)
						val x1191 = !x1190
						val x1195 = if (x1191) {
							val x1192 = x1127.get(x1189)
							val x1193 = x1131.put(x1189, x1192)
							x1193
						} else {
							()
						}

						x1195
				}
				x1126 = x1131

				()
		}
		val x1201 = x1126
		val x1202 = scala.collection.JavaConverters.asScalaSetConverter(x1201.keySet).asScala.toIterable
		val x1203 = x1202.toArray
		val x1204 = x1203.length
		var x1210 = new Array[scala.Tuple2[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]](x1204)
		for (x1205 <- 0 until x1204) {
			val x1206 = x1203.apply(x1205)
			val x1207 = x1201.get(x1206)
			val x1208 = (x1206, x1207)
			x1210(x1205) = x1208
		}
		val x1211 = x1210.length
		var x1246 = new Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]](x1211)
		for (x1212 <- 0 until x1211) {
			val x1213 = x1210.apply(x1212)
			val x1214 = x1213._1
			val x1215 = x1213._2
			val x1216 = x1214._1
			val x1217 = x1214._2
			val x1218 = x1215._1
			val x1219 = x1215._2
			val x1220 = x1219._1
			val x1221 = x1219._2
			val x1222 = x1221._1
			val x1223 = x1221._2
			val x1224 = x1223._1
			val x1225 = x1223._2
			val x1226 = x1225._1
			val x1227 = x1225._2
			val x1229 = x1227._2
			val x1231 = x1229._2
			val x1232 = x1231.toDouble
			val x1233 = x1226 / x1232
			val x1228 = x1227._1
			val x1234 = x1228 / x1232
			val x1230 = x1229._1
			val x1235 = x1230 / x1232
			val x1236 = (x1235, x1231)
			val x1237 = (x1234, x1236)
			val x1238 = (x1233, x1237)
			val x1239 = (x1224, x1238)
			val x1240 = (x1222, x1239)
			val x1241 = (x1220, x1240)
			val x1242 = (x1218, x1241)
			val x1243 = (x1217, x1242)
			val x1244 = (x1216, x1243)
			x1246(x1212) = x1244
		}
		val x1253 = { x1247: (scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]) =>
			val x1248 = x1247._1
			val x1249 = x1247._2
			val x1250 = x1249._1
			val x1252 = (x1248, x1250)
			x1252: scala.Tuple2[Char, Char]
		}
		val x1254 = x1246.sortBy(x1253)
        println("Elapsed time: " + (now - start))   
		x1254
	}
}
/*****************************************
  End of Generated Code                  
*******************************************/
def now: Long = java.lang.System.currentTimeMillis()
val in = scala.io.Source.fromFile("/home/knizhnik/tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray
val benchmark = new TPCH_Q1_hor_par()
val result = benchmark(in)
