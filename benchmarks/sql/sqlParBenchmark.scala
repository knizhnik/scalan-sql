/*****************************************
  Emitting Generated Code                  
*******************************************/
class sqlParBenchmark extends ((Array[scala.Tuple2[Int, java.lang.String]])=>(scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, Int]]])) {
    def now: Long = java.lang.System.currentTimeMillis()
	def apply(x0: Array[scala.Tuple2[Int, java.lang.String]]): scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, Int]]] = {
        val N_TASKS=4
        var start: Long = now
		var x383: Int = 0
		val x274 = x0.length
		val x346 = { x307: (Int) =>
			val x324 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, java.lang.String]]
			val x323 = java.lang.String.valueOf(x307)
			val x325 = (x323, x324)
			val x331 = { x326: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, java.lang.String]]], scala.Tuple2[Int, java.lang.String]]) =>
				val x328 = x326._2
				val x329 = x324 += x328
				x325: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, java.lang.String]]]
			}
			var x332: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, java.lang.String]]] = x325
			var x322_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, java.lang.String]]
			for (x315 <- 0 until x274) {
				val x316 = x0.apply(x315)
				val x317 = x316._1
				val x319 = x317.hashCode
				val x320 = x319 % N_TASKS
				val x321 = x320 == x307
				if (x321) x322_buf += x316
			}
			val x322 = x322_buf.result
			val x340 = x322.foreach {
				x333 =>
					val x334 = x332
					val x335 = x334.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, java.lang.String]]]]
					val x337 = x324 += x333
					x332 = x325

					()
			}
			val x341 = x332
			val x342 = x341._1
			val x343 = x341._2
			val x344 = x343.result
			x344: Array[scala.Tuple2[Int, java.lang.String]]
		}
		// generating parallel execute
		val x347 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until N_TASKS) yield scala.concurrent.future {
				x346(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
        println("Elapsed time for insert=" + (now - start))
        for (i <- 1 until 20){
//        java.lang.System.gc()
        println("-------------------------------")
        start = now

		val x381 = { x365: (Int) =>
			var x367: Int = 0
			val x366 = x347.apply(x365)
			val x378 = x366.foreach {
				x368 =>
					val x370 = x368._2
					val x371 = x370.contains("123")
					val x369 = x368._1
					val x372 = x369 > 0
					val x373 = x371 && x372
					val x376 = if (x373) {
						val x374 = x367 += 1
						()
					} else {
						()
					}

					x376
			}
			val x379 = x367
			x379: Int
		}
		// generating parallel execute
		val x382 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until N_TASKS) yield scala.concurrent.future {
				x381(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x387 = x382.foreach {
			x384 =>
				val x385 = x383 += x384

				()
		}
		val x388 = x383

        println("elapsed time = " + (now - start))
//        java.lang.System.gc()
        start = now

		var x420: Int = 0
		val x418 = { x404: (Int) =>
			var x406: Int = 0
			val x405 = x347.apply(x404)
			val x415 = x405.foreach {
				x407 =>
					val x409 = x407._2
					val x410 = x409.matches(".*1.*2.*3.*")
					val x413 = if (x410) {
						val x411 = x406 += 1
						()
					} else {
						()
					}

					x413
			}
			val x416 = x406
			x416: Int
		}
		// generating parallel execute
		val x419 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until N_TASKS) yield scala.concurrent.future {
				x418(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x424 = x419.foreach {
			x421 =>
				val x422 = x420 += x421

				()
		}
		val x425 = x420

        println("elapsed time = " + (now - start))
//        java.lang.System.gc()
        start = now

		var x451: Int = 0
		val x449 = { x438: (Int) =>
			var x440: Int = 0
			val x439 = x347.apply(x438)
			val x446 = x439.foreach {
				x441 =>
					val x442 = x441._1
					val x444 = x440 += x442

					()
			}
			val x447 = x440
			x447: Int
		}
		// generating parallel execute
		val x450 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until N_TASKS) yield scala.concurrent.future {
				x449(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x455 = x450.foreach {
			x452 =>
				val x453 = x451 += x452

				()
		}
		val x456 = x451

        println("elapsed time = " + (now - start))
//        java.lang.System.gc()
        start = now

		var x494: Int = 0
		val x492 = { x475: (Int) =>
			var x477: Int = 0
			val x476 = x347.apply(x475)
			val x489 = x476.foreach {
				x478 =>
					val x479 = x477
					val x483 = x478._2
					val x484 = x483.startsWith("1")
					val x486 = if (x484) {
						val x480 = x479.asInstanceOf[Int]
						val x482 = x478._1
						val x485 = x480 + x482
						x485
					} else {
						val x480 = x479.asInstanceOf[Int]
						x480
					}
					x477 = x486

					()
			}
			val x490 = x477
			x490: Int
		}
		// generating parallel execute
		val x493 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until N_TASKS) yield scala.concurrent.future {
				x492(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x498 = x493.foreach {
			x495 =>
				val x496 = x494 += x495

				()
		}
        println("elapsed time = " + (now - start))
		val x499 = x494
		val x500 = (x456, x499)
		val x501 = (x425, x500)
		val x502 = (x388, x501)
		x502
        }
        (0, (0, (0, 0)))
	}
}
/*****************************************
  End of Generated Code                  
*******************************************/
val benchmark = new sqlParBenchmark()
val nRecords = 50000000
val inputData = new Array[scala.Tuple2[Int, java.lang.String]](nRecords)
for (i <- 0 until nRecords) {
	inputData.update(i, (i+1, i.toString))
}
println(benchmark(inputData))
