/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q3_ver_par extends ((Array[Array[Array[java.lang.String]]])=>(Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]])) {
	def apply(x0: Array[Array[Array[java.lang.String]]]): Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]] = {
		val x125 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
		val x115 = (0, "")
		val x116 = ("", x115)
		val x117 = ("", x116)
		val x118 = (0, x117)
		val x119 = (0.0, x118)
		val x120 = (' ', x119)
		val x121 = (0, x120)
		val x123 = (0, x121)
		var x127: Int = 0
		val x130 = while (x127 < 0) {
			val x128 = x125 += x123

			x127 = x127 + 1
		}
		val x175 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
		val x158 = ("", "")
		val x159 = ("", x158)
		val x160 = (0, x159)
		val x161 = (0, x160)
		val x162 = (0, x161)
		val x163 = (' ', x162)
		val x164 = (' ', x163)
		val x165 = (0.0, x164)
		val x166 = (0.0, x165)
		val x167 = (0.0, x166)
		val x168 = (0.0, x167)
		val x169 = (0, x168)
		val x170 = (0, x169)
		val x171 = (0, x170)
		val x173 = (0, x171)
		var x176: Int = 0
		val x179 = while (x176 < 0) {
			val x177 = x175 += x173

			x176 = x176 + 1
		}
		val x182 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
		var x183: Int = 0
		val x186 = while (x183 < 0) {
			val x184 = x182 += x173

			x183 = x183 + 1
		}
		val x340 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
		var x341: Int = 0
		val x344 = while (x341 < 0) {
			val x342 = x340 += x123

			x341 = x341 + 1
		}
		val x582 = new java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]()
		val x577 = (0, 0)
		val x578 = (0, x577)
		var x583: Int = 0
		val x586 = while (x583 < 0) {
			val x584 = x582.put(x578, 0.0)

			x583 = x583 + 1
		}
		val x263 = { x253: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]) =>
			val x254 = x253._1
			val x255 = x253._2
			val x257 = x255._2
			val x259 = x257._2
			val x260 = x259._1
			val x262 = (x254, x260)
			x262: scala.Tuple2[Int, Int]
		}
		val x248 = { x245: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]) =>
			val x246 = x245._1
			x246: Int
		}
		val x2965 = x0.apply(2)
		val x2966 = x2965.length
		var x3028 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]](x2966)
		for (x2967 <- 0 until x2966) {
			val x2968 = x2965.apply(x2967)
			val x2969 = x2968.apply(0)
			val x2970 = x2969.toInt
			val x2971 = x2968.apply(1)
			val x2972 = x2971.toInt
			val x2973 = x2968.apply(2)
			val x2974 = x2973.toInt
			val x2975 = x2968.apply(3)
			val x2976 = x2975.toInt
			val x2977 = x2968.apply(4)
			val x2978 = x2977.toDouble
			val x2979 = x2968.apply(5)
			val x2980 = x2979.toDouble
			val x2981 = x2968.apply(6)
			val x2982 = x2981.toDouble
			val x2983 = x2968.apply(7)
			val x2984 = x2983.toDouble
			val x2985 = x2968.apply(8)
			val x2986 = x2985.charAt(0)
			val x2987 = x2968.apply(9)
			val x2988 = x2987.charAt(0)
			val x2989 = x2968.apply(10)
			val x2990 = x2989.substring(0, 4)
			val x2991 = x2989.substring(5, 7)
			val x2992 = x2990 + x2991
			val x2993 = x2989.substring(8, 10)
			val x2994 = x2992 + x2993
			val x2995 = x2994.toInt
			val x2996 = x2968.apply(11)
			val x2997 = x2996.substring(0, 4)
			val x2998 = x2996.substring(5, 7)
			val x2999 = x2997 + x2998
			val x3000 = x2996.substring(8, 10)
			val x3001 = x2999 + x3000
			val x3002 = x3001.toInt
			val x3003 = x2968.apply(12)
			val x3004 = x3003.substring(0, 4)
			val x3005 = x3003.substring(5, 7)
			val x3006 = x3004 + x3005
			val x3007 = x3003.substring(8, 10)
			val x3008 = x3006 + x3007
			val x3009 = x3008.toInt
			val x3010 = x2968.apply(13)
			val x3011 = x2968.apply(14)
			val x3012 = x2968.apply(15)
			val x3013 = (x3011, x3012)
			val x3014 = (x3010, x3013)
			val x3015 = (x3009, x3014)
			val x3016 = (x3002, x3015)
			val x3017 = (x2995, x3016)
			val x3018 = (x2988, x3017)
			val x3019 = (x2986, x3018)
			val x3020 = (x2984, x3019)
			val x3021 = (x2982, x3020)
			val x3022 = (x2980, x3021)
			val x3023 = (x2978, x3022)
			val x3024 = (x2976, x3023)
			val x3025 = (x2974, x3024)
			val x3026 = (x2972, x3025)
			val x3027 = (x2970, x3026)
			x3028(x2967) = x3027
		}
		val x3568 = { x3299: (Int) =>
			val x3364 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3366 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3368 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3370 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3372 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x3374 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x3376 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x3378 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x3380 = scala.collection.mutable.ArrayBuilder.make[Char]
			val x3382 = scala.collection.mutable.ArrayBuilder.make[Char]
			val x3384 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3386 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3388 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3390 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x3392 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x3394 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x3411 = new java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]()
			val x3415 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]()
			val x3315 = java.lang.String.valueOf(x3299)
			val x3316 = x3315 + ".l_orderkey"
			val x3317 = x3316 + "+"
			val x3318 = x3315 + ".l_partkey"
			val x3319 = x3318 + "+"
			val x3320 = x3315 + ".l_suppkey"
			val x3321 = x3320 + "+"
			val x3322 = x3315 + ".l_linenumber"
			val x3323 = x3322 + "+"
			val x3324 = x3315 + ".l_quantity"
			val x3325 = x3324 + "+"
			val x3326 = x3315 + ".l_extendedprice"
			val x3327 = x3326 + "+"
			val x3328 = x3315 + ".l_discount"
			val x3329 = x3328 + "+"
			val x3330 = x3315 + ".l_tax"
			val x3331 = x3330 + "+"
			val x3332 = x3315 + ".l_returnflag"
			val x3333 = x3332 + "+"
			val x3334 = x3315 + ".l_linestatus"
			val x3335 = x3334 + "+"
			val x3336 = x3315 + ".l_shipdate"
			val x3337 = x3336 + "+"
			val x3338 = x3315 + ".l_commitdate"
			val x3339 = x3338 + "+"
			val x3340 = x3315 + ".l_receiptdate"
			val x3341 = x3340 + "+"
			val x3342 = x3315 + ".l_shipinstruct"
			val x3343 = x3342 + "+"
			val x3344 = x3315 + ".l_shipmode"
			val x3345 = x3344 + "+"
			val x3346 = x3315 + ".l_comment"
			val x3347 = x3345 + x3346
			val x3348 = x3343 + x3347
			val x3349 = x3341 + x3348
			val x3350 = x3339 + x3349
			val x3351 = x3337 + x3350
			val x3352 = x3335 + x3351
			val x3353 = x3333 + x3352
			val x3354 = x3331 + x3353
			val x3355 = x3329 + x3354
			val x3356 = x3327 + x3355
			val x3357 = x3325 + x3356
			val x3358 = x3323 + x3357
			val x3359 = x3321 + x3358
			val x3360 = x3319 + x3359
			val x3361 = x3317 + x3360
			val x3362 = x3361 + ".pk"
			val x3363 = x3362 + ".sk"
			val x3365 = (x3316, x3364)
			val x3367 = (x3318, x3366)
			val x3369 = (x3320, x3368)
			val x3371 = (x3322, x3370)
			val x3373 = (x3324, x3372)
			val x3375 = (x3326, x3374)
			val x3377 = (x3328, x3376)
			val x3379 = (x3330, x3378)
			val x3381 = (x3332, x3380)
			val x3383 = (x3334, x3382)
			val x3385 = (x3336, x3384)
			val x3387 = (x3338, x3386)
			val x3389 = (x3340, x3388)
			val x3391 = (x3342, x3390)
			val x3393 = (x3344, x3392)
			val x3395 = (x3346, x3394)
			val x3396 = (x3393, x3395)
			val x3397 = (x3391, x3396)
			val x3398 = (x3389, x3397)
			val x3399 = (x3387, x3398)
			val x3400 = (x3385, x3399)
			val x3401 = (x3383, x3400)
			val x3402 = (x3381, x3401)
			val x3403 = (x3379, x3402)
			val x3404 = (x3377, x3403)
			val x3405 = (x3375, x3404)
			val x3406 = (x3373, x3405)
			val x3407 = (x3371, x3406)
			val x3408 = (x3369, x3407)
			val x3409 = (x3367, x3408)
			val x3410 = (x3365, x3409)
			val x3412 = (x3411, x263)
			val x3413 = (x3410, x3412)
			val x3414 = (x3362, x3413)
			val x3416 = (x3415, x248)
			val x3417 = (x3414, x3416)
			val x3418 = (x3363, x3417)
			val x3490 = { x3419: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]) =>
				val x3421 = x3419._2
				val x3422 = x3421._1
				val x3424 = x3415.containsKey(x3422)
				val x3432 = if (x3424) {
					val x3425 = x3415.get(x3422)
					val x3426 = x3425 += x3421
					x3415
				} else {
					val x3428 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
					val x3429 = x3428 += x3421
					val x3430 = x3415.put(x3422, x3428)
					x3415
				}
				val x3423 = x3421._2
				val x3434 = x3423._2
				val x3436 = x3434._2
				val x3437 = x3436._1
				val x3439 = (x3422, x3437)
				val x3443 = { x3440: (Unit) =>
					val x3441 = x3411.put(x3439, x3421)
					x3411: Unit
				}
				val x3444 = x3411.containsKey(x3439)
				val x3450 = if (x3444) {
					val x3445 = x3411.get(x3439)
					val x3446 = throw new Exception("Unique constraint violation")
					x3446
				} else {
					val x3448 = x3411.put(x3439, x3421)
					x3411
				}
				val x3451 = x3364 += x3422
				val x3433 = x3423._1
				val x3452 = x3366 += x3433
				val x3435 = x3434._1
				val x3453 = x3368 += x3435
				val x3454 = x3370 += x3437
				val x3438 = x3436._2
				val x3455 = x3438._1
				val x3457 = x3372 += x3455
				val x3456 = x3438._2
				val x3458 = x3456._1
				val x3460 = x3374 += x3458
				val x3459 = x3456._2
				val x3461 = x3459._1
				val x3463 = x3376 += x3461
				val x3462 = x3459._2
				val x3464 = x3462._1
				val x3466 = x3378 += x3464
				val x3465 = x3462._2
				val x3467 = x3465._1
				val x3469 = x3380 += x3467
				val x3468 = x3465._2
				val x3470 = x3468._1
				val x3472 = x3382 += x3470
				val x3471 = x3468._2
				val x3473 = x3471._1
				val x3475 = x3384 += x3473
				val x3474 = x3471._2
				val x3476 = x3474._1
				val x3478 = x3386 += x3476
				val x3477 = x3474._2
				val x3479 = x3477._1
				val x3481 = x3388 += x3479
				val x3480 = x3477._2
				val x3482 = x3480._1
				val x3484 = x3390 += x3482
				val x3483 = x3480._2
				val x3485 = x3483._1
				val x3487 = x3392 += x3485
				val x3486 = x3483._2
				val x3488 = x3394 += x3486
				x3418: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]
			}
			var x3491: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]] = x3418
			var x3314_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
			for (x3307 <- 0 until x2966) {
				val x3308 = x3028.apply(x3307)
				val x3309 = x3308._1
				val x3311 = x3309.hashCode
				val x3312 = x3311 % 8
				val x3313 = x3312 == x3299
				if (x3313) x3314_buf += x3308
			}
			val x3314 = x3314_buf.result
			val x3565 = x3314.foreach {
				x3492 =>
					val x3493 = x3491
					val x3494 = x3493.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]]
					val x3496 = x3492._1
					val x3498 = x3415.containsKey(x3496)
					val x3506 = if (x3498) {
						val x3499 = x3415.get(x3496)
						val x3500 = x3499 += x3492
						x3415
					} else {
						val x3502 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
						val x3503 = x3502 += x3492
						val x3504 = x3415.put(x3496, x3502)
						x3415
					}
					val x3497 = x3492._2
					val x3508 = x3497._2
					val x3510 = x3508._2
					val x3511 = x3510._1
					val x3513 = (x3496, x3511)
					val x3517 = { x3514: (Unit) =>
						val x3515 = x3411.put(x3513, x3492)
						x3411: Unit
					}
					val x3518 = x3411.containsKey(x3513)
					val x3524 = if (x3518) {
						val x3519 = x3411.get(x3513)
						val x3520 = throw new Exception("Unique constraint violation")
						x3520
					} else {
						val x3522 = x3411.put(x3513, x3492)
						x3411
					}
					val x3525 = x3364 += x3496
					val x3507 = x3497._1
					val x3526 = x3366 += x3507
					val x3509 = x3508._1
					val x3527 = x3368 += x3509
					val x3528 = x3370 += x3511
					val x3512 = x3510._2
					val x3529 = x3512._1
					val x3531 = x3372 += x3529
					val x3530 = x3512._2
					val x3532 = x3530._1
					val x3534 = x3374 += x3532
					val x3533 = x3530._2
					val x3535 = x3533._1
					val x3537 = x3376 += x3535
					val x3536 = x3533._2
					val x3538 = x3536._1
					val x3540 = x3378 += x3538
					val x3539 = x3536._2
					val x3541 = x3539._1
					val x3543 = x3380 += x3541
					val x3542 = x3539._2
					val x3544 = x3542._1
					val x3546 = x3382 += x3544
					val x3545 = x3542._2
					val x3547 = x3545._1
					val x3549 = x3384 += x3547
					val x3548 = x3545._2
					val x3550 = x3548._1
					val x3552 = x3386 += x3550
					val x3551 = x3548._2
					val x3553 = x3551._1
					val x3555 = x3388 += x3553
					val x3554 = x3551._2
					val x3556 = x3554._1
					val x3558 = x3390 += x3556
					val x3557 = x3554._2
					val x3559 = x3557._1
					val x3561 = x3392 += x3559
					val x3560 = x3557._2
					val x3562 = x3394 += x3560
					x3491 = x3418

					()
			}
			val x3566 = x3491
			x3566: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]]]]]]]]], scala.Tuple2[java.util.HashMap[scala.Tuple2[Int, Int], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], scala.Tuple2[Int, Int]]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]], Int]]]]
		}
		// generating parallel execute
		val x3569 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until 8) yield scala.concurrent.future {
				x3568(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		var x3570: scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]] = x182
		val x3739 = x3569.foreach {
			x3571 =>
				val x3572 = x3570
				val x3573 = x3572.asInstanceOf[scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
				val x3576 = x3571._2
				val x3577 = x3576._1
				val x3580 = x3577._2
				val x3581 = x3580._1
				val x3584 = x3581._2
				val x3586 = x3584._2
				val x3588 = x3586._2
				val x3590 = x3588._2
				val x3592 = x3590._2
				val x3594 = x3592._2
				val x3596 = x3594._2
				val x3598 = x3596._2
				val x3600 = x3598._2
				val x3602 = x3600._2
				val x3604 = x3602._2
				val x3606 = x3604._2
				val x3608 = x3606._2
				val x3609 = x3608._1
				val x3612 = x3609._2
				val x3613 = x3612.result
				val x3614 = x3613.length
				val x3583 = x3581._1
				val x3649 = x3583._2
				val x3650 = x3649.result
				val x3585 = x3584._1
				val x3655 = x3585._2
				val x3656 = x3655.result
				val x3587 = x3586._1
				val x3637 = x3587._2
				val x3638 = x3637.result
				val x3589 = x3588._1
				val x3640 = x3589._2
				val x3641 = x3640.result
				val x3591 = x3590._1
				val x3652 = x3591._2
				val x3653 = x3652.result
				val x3593 = x3592._1
				val x3664 = x3593._2
				val x3665 = x3664.result
				val x3595 = x3594._1
				val x3661 = x3595._2
				val x3662 = x3661.result
				val x3597 = x3596._1
				val x3643 = x3597._2
				val x3644 = x3643.result
				val x3599 = x3598._1
				val x3631 = x3599._2
				val x3632 = x3631.result
				val x3601 = x3600._1
				val x3620 = x3601._2
				val x3621 = x3620.result
				val x3603 = x3602._1
				val x3628 = x3603._2
				val x3629 = x3628.result
				val x3605 = x3604._1
				val x3658 = x3605._2
				val x3659 = x3658.result
				val x3607 = x3606._1
				val x3634 = x3607._2
				val x3635 = x3634.result
				val x3610 = x3608._2
				val x3622 = x3610._1
				val x3646 = x3622._2
				val x3647 = x3646.result
				val x3623 = x3610._2
				val x3625 = x3623._2
				val x3626 = x3625.result
				var x5285 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]](x3614)
				for (x3617 <- 0 until x3614) {
					val x5254 = x3650.apply(x3617)
					val x5255 = x3656.apply(x3617)
					val x5256 = x3638.apply(x3617)
					val x5257 = x3641.apply(x3617)
					val x5258 = x3653.apply(x3617)
					val x5259 = x3665.apply(x3617)
					val x5260 = x3662.apply(x3617)
					val x5261 = x3644.apply(x3617)
					val x5262 = x3632.apply(x3617)
					val x5263 = x3621.apply(x3617)
					val x5264 = x3629.apply(x3617)
					val x5265 = x3659.apply(x3617)
					val x5266 = x3635.apply(x3617)
					val x5267 = x3613.apply(x3617)
					val x5268 = x3647.apply(x3617)
					val x5269 = x3626.apply(x3617)
					val x5270 = (x5268, x5269)
					val x5271 = (x5267, x5270)
					val x5272 = (x5266, x5271)
					val x5273 = (x5265, x5272)
					val x5274 = (x5264, x5273)
					val x5275 = (x5263, x5274)
					val x5276 = (x5262, x5275)
					val x5277 = (x5261, x5276)
					val x5278 = (x5260, x5277)
					val x5279 = (x5259, x5278)
					val x5280 = (x5258, x5279)
					val x5281 = (x5257, x5280)
					val x5282 = (x5256, x5281)
					val x5283 = (x5255, x5282)
					val x5284 = (x5254, x5283)
					x5285(x3617) = x5284
				}
				val x5286 = x5285.foreach {
					x3733 =>
						val x3734 = x3573 += x3733

						x3734
				}
				x3570 = x3573

				()
		}
		val x3740 = x3570
		val x3741 = x3740.result
		val x3742 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]()
		val x3761 = x3741.foreach {
			x3743 =>
				val x3746 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]
				val x3747 = x3746 += x3743
				val x3744 = x3743._1
				val x3749 = x3742.containsKey(x3744)
				val x3758 = if (x3749) {
					val x3750 = x3742.get(x3744)
					val x3752 = x3746.result
					val x3756 = x3752.foreach {
						x3753 =>
							val x3754 = x3750 += x3753

							x3754
					}
					x3750
				} else {
					x3746
				}
				val x3759 = x3742.put(x3744, x3758)

				x3759
		}
		val x665 = { x662: (scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]) =>
			val x663 = x662._1
			x663: Int
		}
		val x3783 = x0.apply(0)
		val x3784 = x3783.length
		var x3805 = new Array[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]](x3784)
		for (x3785 <- 0 until x3784) {
			val x3786 = x3783.apply(x3785)
			val x3787 = x3786.apply(0)
			val x3788 = x3787.toInt
			val x3789 = x3786.apply(1)
			val x3790 = x3786.apply(2)
			val x3791 = x3786.apply(3)
			val x3792 = x3791.toInt
			val x3793 = x3786.apply(4)
			val x3794 = x3786.apply(5)
			val x3795 = x3794.toDouble
			val x3796 = x3786.apply(6)
			val x3797 = x3786.apply(7)
			val x3798 = (x3796, x3797)
			val x3799 = (x3795, x3798)
			val x3800 = (x3793, x3799)
			val x3801 = (x3792, x3800)
			val x3802 = (x3790, x3801)
			val x3803 = (x3789, x3802)
			val x3804 = (x3788, x3803)
			x3805(x3785) = x3804
		}
		val x4103 = { x3955: (Int) =>
			val x3995 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x3997 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x3999 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x4001 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x4003 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x4005 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x4007 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x4009 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x4018 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]()
			val x3971 = java.lang.String.valueOf(x3955)
			val x3972 = x3971 + ".c_custkey"
			val x3973 = x3972 + "+"
			val x3974 = x3971 + ".c_name"
			val x3975 = x3974 + "+"
			val x3976 = x3971 + ".c_address"
			val x3977 = x3976 + "+"
			val x3978 = x3971 + ".c_nationkey"
			val x3979 = x3978 + "+"
			val x3980 = x3971 + ".c_phone"
			val x3981 = x3980 + "+"
			val x3982 = x3971 + ".c_acctbal"
			val x3983 = x3982 + "+"
			val x3984 = x3971 + ".c_mktsegment"
			val x3985 = x3984 + "+"
			val x3986 = x3971 + ".c_comment"
			val x3987 = x3985 + x3986
			val x3988 = x3983 + x3987
			val x3989 = x3981 + x3988
			val x3990 = x3979 + x3989
			val x3991 = x3977 + x3990
			val x3992 = x3975 + x3991
			val x3993 = x3973 + x3992
			val x3994 = x3993 + ".pk"
			val x3996 = (x3972, x3995)
			val x3998 = (x3974, x3997)
			val x4000 = (x3976, x3999)
			val x4002 = (x3978, x4001)
			val x4004 = (x3980, x4003)
			val x4006 = (x3982, x4005)
			val x4008 = (x3984, x4007)
			val x4010 = (x3986, x4009)
			val x4011 = (x4008, x4010)
			val x4012 = (x4006, x4011)
			val x4013 = (x4004, x4012)
			val x4014 = (x4002, x4013)
			val x4015 = (x4000, x4014)
			val x4016 = (x3998, x4015)
			val x4017 = (x3996, x4016)
			val x4019 = (x4018, x665)
			val x4020 = (x4017, x4019)
			val x4021 = (x3994, x4020)
			val x4059 = { x4022: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]) =>
				val x4024 = x4022._2
				val x4025 = x4024._1
				val x4030 = { x4027: (Unit) =>
					val x4028 = x4018.put(x4025, x4024)
					x4018: Unit
				}
				val x4031 = x4018.containsKey(x4025)
				val x4037 = if (x4031) {
					val x4032 = x4018.get(x4025)
					val x4033 = throw new Exception("Unique constraint violation")
					x4033
				} else {
					val x4035 = x4018.put(x4025, x4024)
					x4018
				}
				val x4038 = x3995 += x4025
				val x4026 = x4024._2
				val x4039 = x4026._1
				val x4041 = x3997 += x4039
				val x4040 = x4026._2
				val x4042 = x4040._1
				val x4044 = x3999 += x4042
				val x4043 = x4040._2
				val x4045 = x4043._1
				val x4047 = x4001 += x4045
				val x4046 = x4043._2
				val x4048 = x4046._1
				val x4050 = x4003 += x4048
				val x4049 = x4046._2
				val x4051 = x4049._1
				val x4053 = x4005 += x4051
				val x4052 = x4049._2
				val x4054 = x4052._1
				val x4056 = x4007 += x4054
				val x4055 = x4052._2
				val x4057 = x4009 += x4055
				x4021: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]
			}
			var x4060: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]] = x4021
			var x3970_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]
			for (x3963 <- 0 until x3784) {
				val x3964 = x3805.apply(x3963)
				val x3965 = x3964._1
				val x3967 = x3965.hashCode
				val x3968 = x3967 % 8
				val x3969 = x3968 == x3955
				if (x3969) x3970_buf += x3964
			}
			val x3970 = x3970_buf.result
			val x4100 = x3970.foreach {
				x4061 =>
					val x4062 = x4060
					val x4063 = x4062.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]]
					val x4065 = x4061._1
					val x4070 = { x4067: (Unit) =>
						val x4068 = x4018.put(x4065, x4061)
						x4018: Unit
					}
					val x4071 = x4018.containsKey(x4065)
					val x4077 = if (x4071) {
						val x4072 = x4018.get(x4065)
						val x4073 = throw new Exception("Unique constraint violation")
						x4073
					} else {
						val x4075 = x4018.put(x4065, x4061)
						x4018
					}
					val x4078 = x3995 += x4065
					val x4066 = x4061._2
					val x4079 = x4066._1
					val x4081 = x3997 += x4079
					val x4080 = x4066._2
					val x4082 = x4080._1
					val x4084 = x3999 += x4082
					val x4083 = x4080._2
					val x4085 = x4083._1
					val x4087 = x4001 += x4085
					val x4086 = x4083._2
					val x4088 = x4086._1
					val x4090 = x4003 += x4088
					val x4089 = x4086._2
					val x4091 = x4089._1
					val x4093 = x4005 += x4091
					val x4092 = x4089._2
					val x4094 = x4092._1
					val x4096 = x4007 += x4094
					val x4095 = x4092._2
					val x4097 = x4009 += x4095
					x4060 = x4021

					()
			}
			val x4101 = x4060
			x4101: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], Int]]]]
		}
		// generating parallel execute
		val x4104 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until 8) yield scala.concurrent.future {
				x4103(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		val x252 = { x249: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]) =>
			val x250 = x249._1
			x250: Int
		}
		val x192 = { x187: (scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]) =>
			val x189 = x187._2
			val x190 = x189._1
			x190: Int
		}
		val x4105 = x0.apply(1)
		val x4106 = x4105.length
		var x4137 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]](x4106)
		for (x4107 <- 0 until x4106) {
			val x4108 = x4105.apply(x4107)
			val x4109 = x4108.apply(0)
			val x4110 = x4109.toInt
			val x4111 = x4108.apply(1)
			val x4112 = x4111.toInt
			val x4113 = x4108.apply(2)
			val x4114 = x4113.charAt(0)
			val x4115 = x4108.apply(3)
			val x4116 = x4115.toDouble
			val x4117 = x4108.apply(4)
			val x4118 = x4117.substring(0, 4)
			val x4119 = x4117.substring(5, 7)
			val x4120 = x4118 + x4119
			val x4121 = x4117.substring(8, 10)
			val x4122 = x4120 + x4121
			val x4123 = x4122.toInt
			val x4124 = x4108.apply(5)
			val x4125 = x4108.apply(6)
			val x4126 = x4108.apply(7)
			val x4127 = x4126.toInt
			val x4128 = x4108.apply(8)
			val x4129 = (x4127, x4128)
			val x4130 = (x4125, x4129)
			val x4131 = (x4124, x4130)
			val x4132 = (x4123, x4131)
			val x4133 = (x4116, x4132)
			val x4134 = (x4114, x4133)
			val x4135 = (x4112, x4134)
			val x4136 = (x4110, x4135)
			x4137(x4107) = x4136
		}
		val x4505 = { x4322: (Int) =>
			val x4366 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x4368 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x4370 = scala.collection.mutable.ArrayBuilder.make[Char]
			val x4372 = scala.collection.mutable.ArrayBuilder.make[Double]
			val x4374 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x4376 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x4378 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x4380 = scala.collection.mutable.ArrayBuilder.make[Int]
			val x4382 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
			val x4392 = new java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]()
			val x4396 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]()
			val x4338 = java.lang.String.valueOf(x4322)
			val x4339 = x4338 + ".o_orderkey"
			val x4340 = x4339 + "+"
			val x4341 = x4338 + ".o_custkey"
			val x4342 = x4341 + "+"
			val x4343 = x4338 + ".o_orderstatus"
			val x4344 = x4343 + "+"
			val x4345 = x4338 + ".o_totalprice"
			val x4346 = x4345 + "+"
			val x4347 = x4338 + ".o_orderdate"
			val x4348 = x4347 + "+"
			val x4349 = x4338 + ".o_orderpriority"
			val x4350 = x4349 + "+"
			val x4351 = x4338 + ".o_clerk"
			val x4352 = x4351 + "+"
			val x4353 = x4338 + ".o_shippriority"
			val x4354 = x4353 + "+"
			val x4355 = x4338 + ".o_comment"
			val x4356 = x4354 + x4355
			val x4357 = x4352 + x4356
			val x4358 = x4350 + x4357
			val x4359 = x4348 + x4358
			val x4360 = x4346 + x4359
			val x4361 = x4344 + x4360
			val x4362 = x4342 + x4361
			val x4363 = x4340 + x4362
			val x4364 = x4363 + ".pk"
			val x4365 = x4364 + ".sk"
			val x4367 = (x4339, x4366)
			val x4369 = (x4341, x4368)
			val x4371 = (x4343, x4370)
			val x4373 = (x4345, x4372)
			val x4375 = (x4347, x4374)
			val x4377 = (x4349, x4376)
			val x4379 = (x4351, x4378)
			val x4381 = (x4353, x4380)
			val x4383 = (x4355, x4382)
			val x4384 = (x4381, x4383)
			val x4385 = (x4379, x4384)
			val x4386 = (x4377, x4385)
			val x4387 = (x4375, x4386)
			val x4388 = (x4373, x4387)
			val x4389 = (x4371, x4388)
			val x4390 = (x4369, x4389)
			val x4391 = (x4367, x4390)
			val x4393 = (x4392, x252)
			val x4394 = (x4391, x4393)
			val x4395 = (x4364, x4394)
			val x4397 = (x4396, x192)
			val x4398 = (x4395, x4397)
			val x4399 = (x4365, x4398)
			val x4449 = { x4400: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]) =>
				val x4402 = x4400._2
				val x4404 = x4402._2
				val x4405 = x4404._1
				val x4407 = x4396.containsKey(x4405)
				val x4415 = if (x4407) {
					val x4408 = x4396.get(x4405)
					val x4409 = x4408 += x4402
					x4396
				} else {
					val x4411 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
					val x4412 = x4411 += x4402
					val x4413 = x4396.put(x4405, x4411)
					x4396
				}
				val x4403 = x4402._1
				val x4419 = { x4416: (Unit) =>
					val x4417 = x4392.put(x4403, x4402)
					x4392: Unit
				}
				val x4420 = x4392.containsKey(x4403)
				val x4426 = if (x4420) {
					val x4421 = x4392.get(x4403)
					val x4422 = throw new Exception("Unique constraint violation")
					x4422
				} else {
					val x4424 = x4392.put(x4403, x4402)
					x4392
				}
				val x4427 = x4366 += x4403
				val x4428 = x4368 += x4405
				val x4406 = x4404._2
				val x4429 = x4406._1
				val x4431 = x4370 += x4429
				val x4430 = x4406._2
				val x4432 = x4430._1
				val x4434 = x4372 += x4432
				val x4433 = x4430._2
				val x4435 = x4433._1
				val x4437 = x4374 += x4435
				val x4436 = x4433._2
				val x4438 = x4436._1
				val x4440 = x4376 += x4438
				val x4439 = x4436._2
				val x4441 = x4439._1
				val x4443 = x4378 += x4441
				val x4442 = x4439._2
				val x4444 = x4442._1
				val x4446 = x4380 += x4444
				val x4445 = x4442._2
				val x4447 = x4382 += x4445
				x4399: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]
			}
			var x4450: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]] = x4399
			var x4337_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
			for (x4330 <- 0 until x4106) {
				val x4331 = x4137.apply(x4330)
				val x4332 = x4331._1
				val x4334 = x4332.hashCode
				val x4335 = x4334 % 8
				val x4336 = x4335 == x4322
				if (x4336) x4337_buf += x4331
			}
			val x4337 = x4337_buf.result
			val x4502 = x4337.foreach {
				x4451 =>
					val x4452 = x4450
					val x4453 = x4452.asInstanceOf[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]]
					val x4456 = x4451._2
					val x4457 = x4456._1
					val x4459 = x4396.containsKey(x4457)
					val x4467 = if (x4459) {
						val x4460 = x4396.get(x4457)
						val x4461 = x4460 += x4451
						x4396
					} else {
						val x4463 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
						val x4464 = x4463 += x4451
						val x4465 = x4396.put(x4457, x4463)
						x4396
					}
					val x4455 = x4451._1
					val x4471 = { x4468: (Unit) =>
						val x4469 = x4392.put(x4455, x4451)
						x4392: Unit
					}
					val x4472 = x4392.containsKey(x4455)
					val x4478 = if (x4472) {
						val x4473 = x4392.get(x4455)
						val x4474 = throw new Exception("Unique constraint violation")
						x4474
					} else {
						val x4476 = x4392.put(x4455, x4451)
						x4392
					}
					val x4479 = x4366 += x4455
					val x4480 = x4368 += x4457
					val x4458 = x4456._2
					val x4481 = x4458._1
					val x4483 = x4370 += x4481
					val x4482 = x4458._2
					val x4484 = x4482._1
					val x4486 = x4372 += x4484
					val x4485 = x4482._2
					val x4487 = x4485._1
					val x4489 = x4374 += x4487
					val x4488 = x4485._2
					val x4490 = x4488._1
					val x4492 = x4376 += x4490
					val x4491 = x4488._2
					val x4493 = x4491._1
					val x4495 = x4378 += x4493
					val x4494 = x4491._2
					val x4496 = x4494._1
					val x4498 = x4380 += x4496
					val x4497 = x4494._2
					val x4499 = x4382 += x4497
					x4450 = x4399

					()
			}
			val x4503 = x4450
			x4503: scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[java.lang.String, scala.Tuple2[scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]]]]]]]], scala.Tuple2[java.util.HashMap[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]], scala.Tuple2[java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]], scala.Function1[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]], Int]]]]
		}
		// generating parallel execute
		val x4506 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until 8) yield scala.concurrent.future {
				x4505(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		var x4507: scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]] = x125
		val x4613 = x4506.foreach {
			x4508 =>
				val x4509 = x4507
				val x4510 = x4509.asInstanceOf[scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
				val x4513 = x4508._2
				val x4514 = x4513._1
				val x4517 = x4514._2
				val x4518 = x4517._1
				val x4521 = x4518._2
				val x4523 = x4521._2
				val x4525 = x4523._2
				val x4527 = x4525._2
				val x4529 = x4527._2
				val x4531 = x4529._2
				val x4532 = x4531._1
				val x4535 = x4532._2
				val x4536 = x4535.result
				val x4537 = x4536.length
				val x4520 = x4518._1
				val x4554 = x4520._2
				val x4555 = x4554.result
				val x4522 = x4521._1
				val x4543 = x4522._2
				val x4544 = x4543.result
				val x4524 = x4523._1
				val x4560 = x4524._2
				val x4561 = x4560.result
				val x4526 = x4525._1
				val x4566 = x4526._2
				val x4567 = x4566.result
				val x4528 = x4527._1
				val x4557 = x4528._2
				val x4558 = x4557.result
				val x4530 = x4529._1
				val x4551 = x4530._2
				val x4552 = x4551.result
				val x4533 = x4531._2
				val x4545 = x4533._1
				val x4563 = x4545._2
				val x4564 = x4563.result
				val x4546 = x4533._2
				val x4548 = x4546._2
				val x4549 = x4548.result
				var x5305 = new Array[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]](x4537)
				for (x4540 <- 0 until x4537) {
					val x5288 = x4555.apply(x4540)
					val x5289 = x4544.apply(x4540)
					val x5290 = x4561.apply(x4540)
					val x5291 = x4567.apply(x4540)
					val x5292 = x4558.apply(x4540)
					val x5293 = x4552.apply(x4540)
					val x5294 = x4536.apply(x4540)
					val x5295 = x4564.apply(x4540)
					val x5296 = x4549.apply(x4540)
					val x5297 = (x5295, x5296)
					val x5298 = (x5294, x5297)
					val x5299 = (x5293, x5298)
					val x5300 = (x5292, x5299)
					val x5301 = (x5291, x5300)
					val x5302 = (x5290, x5301)
					val x5303 = (x5289, x5302)
					val x5304 = (x5288, x5303)
					x5305(x4540) = x5304
				}
				val x5306 = x5305.foreach {
					x4607 =>
						val x4608 = x4510 += x4607

						x4608
				}
				x4507 = x4510

				()
		}
		val x4614 = x4507
		val x4615 = x4614.result

        java.lang.System.gc()
        println("Start query")
        val start = now

		val x4616 = new java.util.HashMap[Int, scala.collection.mutable.ArrayBuilder[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]()
		val x4637 = x4615.foreach {
			x4617 =>
				val x4622 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]
				val x4623 = x4622 += x4617
				val x4619 = x4617._2
				val x4620 = x4619._1
				val x4625 = x4616.containsKey(x4620)
				val x4634 = if (x4625) {
					val x4626 = x4616.get(x4620)
					val x4628 = x4622.result
					val x4632 = x4628.foreach {
						x4629 =>
							val x4630 = x4626 += x4629

							x4630
					}
					x4626
				} else {
					x4622
				}
				val x4635 = x4616.put(x4620, x4634)

				x4635
		}
		val x5186 = { x4922: (Int) =>
			val x4923 = x4104.apply(x4922)
			val x4924 = x4923._1
			val x4925 = x4923._2
			val x5024 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
			val x4926 = x4925._1
			val x4929 = x4926._2
			val x4931 = x4929._2
			val x4933 = x4931._2
			val x4935 = x4933._2
			val x4937 = x4935._2
			val x4938 = x4937._1
			val x4941 = x4938._2
			val x4942 = x4941.result
			val x4943 = x4942.length
			val x4928 = x4926._1
			val x4969 = x4928._2
			val x4970 = x4969.result
			val x4930 = x4929._1
			val x4960 = x4930._2
			val x4961 = x4960.result
			val x4932 = x4931._1
			val x4957 = x4932._2
			val x4958 = x4957.result
			val x4934 = x4933._1
			val x4963 = x4934._2
			val x4964 = x4963.result
			val x4936 = x4935._1
			val x4949 = x4936._2
			val x4950 = x4949.result
			val x4939 = x4937._2
			val x4952 = x4939._2
			val x4954 = x4952._2
			val x4955 = x4954.result
			val x4951 = x4939._1
			val x4966 = x4951._2
			val x4967 = x4966.result
			var x5371_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]
			for (x4946 <- 0 until x4943) {
				val x5308 = x4970.apply(x4946)
				val x5309 = x4961.apply(x4946)
				val x5310 = x4958.apply(x4946)
				val x5311 = x4964.apply(x4946)
				val x5312 = x4950.apply(x4946)
				val x5313 = x4942.apply(x4946)
				val x5315 = x4955.apply(x4946)
				val x5314 = x4967.apply(x4946)
				val x5316 = (x5314, x5315)
				val x5317 = (x5313, x5316)
				val x5318 = (x5312, x5317)
				val x5319 = (x5311, x5318)
				val x5320 = (x5310, x5319)
				val x5321 = (x5309, x5320)
				val x5322 = (x5308, x5321)
				val x5370 = x5314 == "HOUSEHOLD"
				if (x5370) x5371_buf += x5322
			}
			val x5371 = x5371_buf.result
			val x5372 = x5371.foreach {
				x5025 =>
					val x5026 = x5025._1
					val x5028 = x4616.containsKey(x5026)
					val x5031 = if (x5028) {
						val x5029 = x4616.get(x5026)
						x5029
					} else {
						x340
					}
					val x5032 = x5031.result
					val x5036 = x5032.length
					var x5041 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]](x5036)
					for (x5037 <- 0 until x5036) {
						val x5038 = x5032.apply(x5037)
						val x5039 = (x5025, x5038)
						x5041(x5037) = x5039
					}
					val x5045 = x5041.foreach {
						x5042 =>
							val x5043 = x5024 += x5042

							x5043
					}

					x5045
			}
			val x5373 = x5024.result
			val x5374 = x5373.length
			val x5067 = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
			var x5385_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]]]
			for (x5050 <- 0 until x5374) {
				val x5375 = x5373.apply(x5050)
				val x5376 = x5375._2
				val x5377 = x5376._2
				val x5378 = x5377._2
				val x5379 = x5378._2
				val x5380 = x5379._2
				val x5381 = x5380._1
				val x5382 = x5381 < 19950304
				val x5383 = x5375._1
				if (x5382) x5385_buf += x5375
			}
			val x5385 = x5385_buf.result
			val x5386 = x5385.foreach {
				x5068 =>
					val x5070 = x5068._2
					val x5071 = x5070._1
					val x5073 = x3742.containsKey(x5071)
					val x5076 = if (x5073) {
						val x5074 = x3742.get(x5071)
						x5074
					} else {
						x175
					}
					val x5077 = x5076.result
					val x5081 = x5077.length
					var x5086 = new Array[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]](x5081)
					for (x5082 <- 0 until x5081) {
						val x5083 = x5077.apply(x5082)
						val x5084 = (x5068, x5083)
						x5086(x5082) = x5084
					}
					val x5090 = x5086.foreach {
						x5087 =>
							val x5088 = x5067 += x5087

							x5088
					}

					x5090
			}
			val x5387 = x5067.result
			val x5388 = x5387.length
			val x5124 = new java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]()
			val x5389 = x5387.foreach {
				x5125 =>
					val x5127 = x5125._2
					val x5129 = x5127._2
					val x5131 = x5129._2
					val x5133 = x5131._2
					val x5135 = x5133._2
					val x5137 = x5135._2
					val x5139 = x5137._2
					val x5141 = x5139._2
					val x5143 = x5141._2
					val x5145 = x5143._2
					val x5147 = x5145._2
					val x5148 = x5147._1
					val x5150 = x5148 > 19950304
					val x5182 = if (x5150) {
						val x5128 = x5127._1
						val x5126 = x5125._1
						val x5152 = x5126._2
						val x5154 = x5152._2
						val x5156 = x5154._2
						val x5158 = x5156._2
						val x5160 = x5158._2
						val x5161 = x5160._1
						val x5162 = x5160._2
						val x5164 = x5162._2
						val x5166 = x5164._2
						val x5167 = x5166._1
						val x5169 = (x5161, x5167)
						val x5170 = (x5128, x5169)
						val x5174 = x5124.containsKey(x5170)
						val x5179 = if (x5174) {
							val x5175 = x5124.get(x5170)
							val x5138 = x5137._1
							val x5140 = x5139._1
							val x5171 = 1.0 - x5140
							val x5172 = x5138 * x5171
							val x5177 = x5175 + x5172
							x5177
						} else {
							val x5138 = x5137._1
							val x5140 = x5139._1
							val x5171 = 1.0 - x5140
							val x5172 = x5138 * x5171
							x5172
						}
						val x5180 = x5124.put(x5170, x5179)
						x5180
					} else {
						()
					}

					x5182
			}
			var x5406_buf = scala.collection.mutable.ArrayBuilder.make[scala.Tuple2[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[Double, scala.Tuple2[java.lang.String, java.lang.String]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, scala.Tuple2[Int, java.lang.String]]]]]]]]], scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[Int, scala.Tuple2[java.lang.String, scala.Tuple2[java.lang.String, java.lang.String]]]]]]]]]]]]]]]]]
			for (x5095 <- 0 until x5388) {
				val x5390 = x5387.apply(x5095)
				val x5391 = x5390._2
				val x5392 = x5391._2
				val x5393 = x5392._2
				val x5394 = x5393._2
				val x5395 = x5394._2
				val x5396 = x5395._2
				val x5397 = x5396._2
				val x5398 = x5397._2
				val x5399 = x5398._2
				val x5400 = x5399._2
				val x5401 = x5400._2
				val x5402 = x5401._1
				val x5403 = x5402 > 19950304
				val x5404 = x5390._1
				if (x5403) x5406_buf += x5390
			}
			val x5406 = x5406_buf.result
			x5124: java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]
		}
		// generating parallel execute
		val x5187 = {
			import scala.concurrent.ExecutionContext.Implicits.global
			val tasks = for (i <- 0 until 8) yield scala.concurrent.future {
				x5186(i)
			}
			scala.concurrent.Await.result(scala.concurrent.Future.sequence(tasks), scala.concurrent.duration.Duration.Inf).toArray
		}
		var x5188: java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double] = x582
		val x5220 = x5187.foreach {
			x5189 =>
				val x5190 = x5188
				val x5191 = x5190.asInstanceOf[java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]]
				val x5193 = new java.util.HashMap[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]()
				val x5194 = scala.collection.JavaConverters.asScalaSetConverter(x5191.keySet).asScala.toIterable
				val x5207 = x5194.foreach {
					x5195 =>
						val x5196 = x5189.containsKey(x5195)
						val x5204 = if (x5196) {
							val x5197 = x5191.get(x5195)
							val x5198 = x5189.get(x5195)
							val x5200 = x5197 + x5198
							x5200
						} else {
							val x5202 = x5191.get(x5195)
							x5202
						}
						val x5205 = x5193.put(x5195, x5204)

						x5205
				}
				val x5208 = scala.collection.JavaConverters.asScalaSetConverter(x5189.keySet).asScala.toIterable
				val x5217 = x5208.foreach {
					x5209 =>
						val x5210 = x5191.containsKey(x5209)
						val x5211 = !x5210
						val x5215 = if (x5211) {
							val x5212 = x5189.get(x5209)
							val x5213 = x5193.put(x5209, x5212)
							x5213
						} else {
							()
						}

						x5215
				}
				x5188 = x5193

				()
		}
		val x5221 = x5188
		val x5222 = scala.collection.JavaConverters.asScalaSetConverter(x5221.keySet).asScala.toIterable
		val x5223 = x5222.toArray
		val x5224 = x5223.length
		var x5230 = new Array[scala.Tuple2[scala.Tuple2[Int, scala.Tuple2[Int, Int]], Double]](x5224)
		for (x5225 <- 0 until x5224) {
			val x5226 = x5223.apply(x5225)
			val x5227 = x5221.get(x5226)
			val x5228 = (x5226, x5227)
			x5230(x5225) = x5228
		}
		val x5231 = x5230.length
		var x5241 = new Array[scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]](x5231)
		for (x5232 <- 0 until x5231) {
			val x5233 = x5230.apply(x5232)
			val x5234 = x5233._1
			val x5235 = x5233._2
			val x5236 = x5234._1
			val x5237 = x5234._2
			val x5238 = (x5235, x5237)
			val x5239 = (x5236, x5238)
			x5241(x5232) = x5239
		}
		val x5251 = { x5242: (scala.Tuple2[Int, scala.Tuple2[Double, scala.Tuple2[Int, Int]]]) =>
			val x5244 = x5242._2
			val x5245 = x5244._1
			val x5247 = 0.0 - x5245
			val x5246 = x5244._2
			val x5248 = x5246._1
			val x5250 = (x5247, x5248)
			x5250: scala.Tuple2[Double, Int]
		}
		val x5252 = x5241.sortBy(x5251)
        println("Elapsed time: " + (now - start))   
		x5252
	}
}
/*****************************************
  End of Generated Code                  
*******************************************/
def now: Long = java.lang.System.currentTimeMillis()
val in = Array(scala.io.Source.fromFile("~/tpch-data/sf1/customer.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("~/tpch-data/sf1/orders.tbl").getLines().map(s => s.split("\\|")).toArray,
    scala.io.Source.fromFile("~/tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray)    
val benchmark = new TPCH_Q3_ver_par()
val result = benchmark(in)
