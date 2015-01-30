/*****************************************
  Emitting Generated Code                  
*******************************************/
class sqlMapReduce extends ((scala.Tuple2[Int, Array[scala.Tuple2[Int, java.lang.String]]])=>(Array[scala.Tuple2[java.lang.String, Int]])) {
    def now: Long = java.lang.System.currentTimeMillis()
	def apply(x0: scala.Tuple2[Int, Array[scala.Tuple2[Int, java.lang.String]]]): Array[scala.Tuple2[java.lang.String, Int]] = {
        var start: Long = now
		val x13 = new java.util.HashMap[java.lang.String, Int]()
		var x15: Int = 0
		val x18 = while (x15 < 0) {
			val x16 = x13.put("", 0)

			x15 = x15 + 1
		}
		val x217 = x0._1
		val x218 = x0._2
		val x219 = x218.length
		val x220 = x219 + x217
		val x278 = { x250: (Int) =>
			val x259 = scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, java.lang.String]]()
			val x260 = (x259, x250)
			val x266 = { x261: (scala.Tuple2[scala.Tuple2[scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, java.lang.String]], Int], scala.Tuple2[Int, java.lang.String]]) =>
				val x263 = x261._2
				val x264 = x259 += x263
				x260: scala.Tuple2[scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, java.lang.String]], Int]
			}
			var x267: scala.Tuple2[scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, java.lang.String]], Int] = x260
			val x251 = x220 - x250
			val x252 = x251 - 1
			val x253 = x252 / x217
			var x258 = new Array[scala.Tuple2[Int, java.lang.String]](x253)
			for (x254 <- 0 until x253) {
				val x255 = x254 * x217
				val x256 = x250 + x255
				val x257 = x218.apply(x256)
				x258(x254) = x257
			}
			val x275 = x258.foreach {
				x268 =>
					val x269 = x267
					val x270 = x269.asInstanceOf[scala.Tuple2[scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, java.lang.String]], Int]]
					val x272 = x259 += x268
					x267 = x260

					()
			}
			val x276 = x267
			x276: scala.Tuple2[scala.collection.mutable.ArrayBuffer[scala.Tuple2[Int, java.lang.String]], Int]
		}
		// generating parallel execute
		val x279 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x217) yield scala.concurrent.future {
				x278(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
        println("Elapsed time for insert=" + (now - start))
        var result:Array[scala.Tuple2[java.lang.String, Int]] = Array.empty[scala.Tuple2[java.lang.String, Int]]
        for (i <- 1 until 20) 
        {
        start = now
		val x321 = { x301: (Int) =>
			val x302 = x279.apply(x301)
			val x303 = x302._1
			val x304 = x302._2
			val x306 = new java.util.HashMap[java.lang.String, Int]()
			val x305 = x303.toArray
			val x319 = x305.foreach {
				x307 =>
					val x309 = x307._2
					val x311 = x306.containsKey(x309)
					val x316 = if (x311) {
						val x312 = x306.get(x309)
						val x308 = x307._1
						val x314 = x312 + x308
						x314
					} else {
						val x308 = x307._1
						x308
					}
					val x317 = x306.put(x309, x316)

					x317
			}
			x306: java.util.HashMap[java.lang.String, Int]
		}
		// generating parallel execute
		val x322 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until x217) yield scala.concurrent.future {
				x321(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		var x323: java.util.HashMap[java.lang.String, Int] = x13
		val x355 = x322.foreach {
			x324 =>
				val x325 = x323
				val x326 = x325.asInstanceOf[java.util.HashMap[java.lang.String, Int]]
				val x328 = new java.util.HashMap[java.lang.String, Int]()
				val x329 = scala.collection.JavaConverters.asScalaSetConverter(x326.keySet).asScala.toIterable
				val x342 = x329.foreach {
					x330 =>
						val x331 = x324.containsKey(x330)
						val x339 = if (x331) {
							val x332 = x326.get(x330)
							val x333 = x324.get(x330)
							val x335 = x332 + x333
							x335
						} else {
							val x337 = x326.get(x330)
							x337
						}
						val x340 = x328.put(x330, x339)

						x340
				}
				val x343 = scala.collection.JavaConverters.asScalaSetConverter(x324.keySet).asScala.toIterable
				val x352 = x343.foreach {
					x344 =>
						val x345 = x326.containsKey(x344)
						val x346 = !x345
						val x350 = if (x346) {
							val x347 = x324.get(x344)
							val x348 = x328.put(x344, x347)
							x348
						} else {
							()
						}

						x350
				}
				x323 = x328

				()
		}
		val x356 = x323
		val x357 = scala.collection.JavaConverters.asScalaSetConverter(x356.keySet).asScala.toIterable
		val x358 = x357.toArray
		val x363 = x358.length
		val x364 = new Array[scala.Tuple2[java.lang.String, Int]](x363)
		// workaround for refinedManifest problem
		val x365 = {
			val in = x358
			val out = new Array[scala.Tuple2[java.lang.String, Int]](in.length)
			var i = 0
			while (i < in.length) {
				val x359 = in(i)
				val x360 = x356.get(x359)
				val x361 = (x359, x360)

				out(i) = x361
				i += 1
			}
			out
		}
		val x370 = x365.length
		val x371 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, Int]]](x370)
		// workaround for refinedManifest problem
		val x372 = {
			val in = x365
			val out = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, Int]]](in.length)
			var i = 0
			while (i < in.length) {
				val x366 = in(i)
				val x368 = x366._2
				val x369 = (x368, x366)

				out(i) = x369
				i += 1
			}
			out
		}
		val x373 = {
			val d = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, Int]]](x372.length)
			System.arraycopy(x372, 0, d, 0, x372.length)
			scala.util.Sorting.quickSort(d)
			d
		}
		val x377 = x373.length
		val x378 = new Array[scala.Tuple2[java.lang.String, Int]](x377)
		// workaround for refinedManifest problem
		val x379 = {
			val in = x373
			val out = new Array[scala.Tuple2[java.lang.String, Int]](in.length)
			var i = 0
			while (i < in.length) {
				val x374 = in(i)
				val x376 = x374._2

				out(i) = x376
				i += 1
			}
			out
		}
        println("Elapsed time for map reduce=" + (now - start))
		result = x379
        }
        result
	}
}
/*****************************************
  End of Generated Code                  
*******************************************/
val benchmark = new sqlMapReduce()
val nRecords = 10000000
val inputData = new Array[scala.Tuple2[Int, java.lang.String]](nRecords)
for (i <- 0 until nRecords) {
  inputData.update(i, (i, "http://www.scala-lang.org/some/long/url/" + (i % 100).toString))
}
for (x <- benchmark((1, inputData))) { 
  println(x._1 + "->" + x._2)
}
