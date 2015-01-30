/*****************************************
  Emitting Generated Code                  
*******************************************/
class TPCH_Q1_ver_seq extends ((Array[Array[java.lang.String]])=>(Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]])) {
  def apply(x0: Array[Array[java.lang.String]]): Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]] = {
    val x12 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x13 = ("Lineitem.l_discount", x12)
    val x19 = { x14: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], Double]) =>
      val x16 = x14._2
      val x17 = x12 += x16
      x13: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]
    }
    val x20 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x22 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x23 = ("Lineitem.l_suppkey", x22)
    val x29 = { x24: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
      val x26 = x24._2
      val x27 = x22 += x26
      x23: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
    }
    val x30 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x32 = scala.collection.mutable.ArrayBuilder.make[Char]
    val x33 = ("Lineitem.l_linestatus", x32)
    val x39 = { x34: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], Char]) =>
      val x36 = x34._2
      val x37 = x32 += x36
      x33: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]]
    }
    val x40 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x41 = ("Lineitem.l_linenumber", x40)
    val x47 = { x42: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
      val x44 = x42._2
      val x45 = x40 += x44
      x41: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
    }
    val x48 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x49 = ("Lineitem.l_shipdate", x48)
    val x64 = { x59: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
      val x61 = x59._2
      val x62 = x48 += x61
      x49: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
    }
    val x123 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x124 = ("Lineitem.l_extendedprice", x123)
    val x130 = { x125: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], Double]) =>
      val x127 = x125._2
      val x128 = x123 += x127
      x124: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]
    }
    val x196 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x198 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x21 = ("Lineitem.l_receiptdate", x20)
    val x209 = { x204: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
      val x206 = x204._2
      val x207 = x20 += x206
      x21: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
    }
    val x31 = ("Lineitem.l_orderkey", x30)
    val x218 = { x213: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
      val x215 = x213._2
      val x216 = x30 += x215
      x31: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
    }
    val x219 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x225 = scala.collection.mutable.ArrayBuilder.make[Double]
    val x226 = ("Lineitem.l_tax", x225)
    val x232 = { x227: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], Double]) =>
      val x229 = x227._2
      val x230 = x225 += x229
      x226: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]
    }
    val x266 = scala.collection.mutable.ArrayBuilder.make[Int]
    val x199 = ("Lineitem.l_quantity", x198)
    val x273 = { x268: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]], Double]) =>
      val x270 = x268._2
      val x271 = x198 += x270
      x199: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]
    }
    val x283 = scala.collection.mutable.ArrayBuilder.make[Char]
    val x284 = ("Lineitem.l_returnflag", x283)
    val x298 = { x293: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]], Char]) =>
      val x295 = x293._2
      val x296 = x283 += x295
      x284: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]]
    }
    val x299 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x300 = ("Lineitem.l_shipinstruct", x299)
    val x309 = { x304: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], java.lang.String]) =>
      val x306 = x304._2
      val x307 = x299 += x306
      x300: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]
    }
    val x197 = ("Lineitem.l_commitdate", x196)
    val x315 = { x310: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
      val x312 = x310._2
      val x313 = x196 += x312
      x197: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
    }
    val x220 = ("Lineitem.l_comment", x219)
    val x321 = { x316: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], java.lang.String]) =>
      val x318 = x316._2
      val x319 = x219 += x318
      x220: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]
    }
    val x267 = ("Lineitem.l_partkey", x266)
    val x327 = { x322: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]], Int]) =>
      val x324 = x322._2
      val x325 = x266 += x324
      x267: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]
    }
    val x328 = scala.collection.mutable.ArrayBuilder.make[java.lang.String]
    val x329 = ("Lineitem.l_shipmode", x328)
    val x348 = { x343: (scala.Tuple2[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]], java.lang.String]) =>
      val x345 = x343._2
      val x346 = x328 += x345
      x329: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]
    }
    var x901: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]] = x300
    val x896 = x0.length
    var x900 = new Array[java.lang.String](x896)
    var x1439 = new Array[Double](x896)
    var x1451 = new Array[Int](x896)
    var x1458 = new Array[Double](x896)
    var x1465 = new Array[Int](x896)
    var x1477 = new Array[Int](x896)
    var x1484 = new Array[Char](x896)
    var x1490 = new Array[java.lang.String](x896)
    var x1496 = new Array[java.lang.String](x896)
    var x1503 = new Array[Int](x896)
    var x1510 = new Array[Char](x896)
    var x1517 = new Array[Int](x896)
    var x1524 = new Array[Int](x896)
    var x1531 = new Array[Double](x896)
    var x1538 = new Array[Double](x896)
    var x1550 = new Array[Int](x896)
    for (x897 <- 0 until x896) {
      val x898 = x0.apply(x897)
      val x899 = x898.apply(13)
      val x1437 = x898.apply(5)
      val x1438 = x1437.toDouble
      val x1444 = x898.apply(12)
      val x1445 = x1444.substring(0, 4)
      val x1446 = x1444.substring(5, 7)
      val x1447 = x1445 + x1446
      val x1448 = x1444.substring(8, 10)
      val x1449 = x1447 + x1448
      val x1450 = x1449.toInt
      val x1456 = x898.apply(4)
      val x1457 = x1456.toDouble
      val x1463 = x898.apply(3)
      val x1464 = x1463.toInt
      val x1470 = x898.apply(11)
      val x1471 = x1470.substring(0, 4)
      val x1472 = x1470.substring(5, 7)
      val x1473 = x1471 + x1472
      val x1474 = x1470.substring(8, 10)
      val x1475 = x1473 + x1474
      val x1476 = x1475.toInt
      val x1482 = x898.apply(9)
      val x1483 = x1482.charAt(0)
      val x1489 = x898.apply(15)
      val x1495 = x898.apply(14)
      val x1501 = x898.apply(0)
      val x1502 = x1501.toInt
      val x1508 = x898.apply(8)
      val x1509 = x1508.charAt(0)
      val x1515 = x898.apply(2)
      val x1516 = x1515.toInt
      val x1522 = x898.apply(1)
      val x1523 = x1522.toInt
      val x1529 = x898.apply(7)
      val x1530 = x1529.toDouble
      val x1536 = x898.apply(6)
      val x1537 = x1536.toDouble
      val x1543 = x898.apply(10)
      val x1544 = x1543.substring(0, 4)
      val x1545 = x1543.substring(5, 7)
      val x1546 = x1544 + x1545
      val x1547 = x1543.substring(8, 10)
      val x1548 = x1546 + x1547
      val x1549 = x1548.toInt
      x900(x897) = x899
      x1439(x897) = x1438
      x1451(x897) = x1450
      x1458(x897) = x1457
      x1465(x897) = x1464
      x1477(x897) = x1476
      x1484(x897) = x1483
      x1490(x897) = x1489
      x1496(x897) = x1495
      x1503(x897) = x1502
      x1510(x897) = x1509
      x1517(x897) = x1516
      x1524(x897) = x1523
      x1531(x897) = x1530
      x1538(x897) = x1537
      x1550(x897) = x1549
    }
    val x909 = x900.foreach {
      x902 =>
        val x903 = x901
        val x904 = x903.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]
        val x906 = x299 += x902
        x901 = x300

        ()
    }
    val x910 = x901
    val x911 = x910._1
    val x912 = x910._2
    var x924: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]] = x124
    val x1440 = x1439.foreach {
      x925 =>
        val x926 = x924
        val x927 = x926.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]]
        val x929 = x123 += x925
        x924 = x124

        ()
    }
    val x1441 = x924
    val x1442 = x1441._1
    val x1443 = x1441._2
    var x947: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x21
    val x1452 = x1451.foreach {
      x948 =>
        val x949 = x947
        val x950 = x949.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
        val x952 = x20 += x948
        x947 = x21

        ()
    }
    val x1453 = x947
    val x1454 = x1453._1
    val x1455 = x1453._2
    var x965: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]] = x199
    val x1459 = x1458.foreach {
      x966 =>
        val x967 = x965
        val x968 = x967.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]]
        val x970 = x198 += x966
        x965 = x199

        ()
    }
    val x1460 = x965
    val x1461 = x1460._1
    val x1462 = x1460._2
    var x983: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x41
    val x1466 = x1465.foreach {
      x984 =>
        val x985 = x983
        val x986 = x985.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
        val x988 = x40 += x984
        x983 = x41

        ()
    }
    val x1467 = x983
    val x1468 = x1467._1
    val x1469 = x1467._2
    var x1006: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x197
    val x1478 = x1477.foreach {
      x1007 =>
        val x1008 = x1006
        val x1009 = x1008.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
        val x1011 = x196 += x1007
        x1006 = x197

        ()
    }
    val x1479 = x1006
    val x1480 = x1479._1
    val x1481 = x1479._2
    var x1024: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]] = x33
    val x1485 = x1484.foreach {
      x1025 =>
        val x1026 = x1024
        val x1027 = x1026.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]]]
        val x1029 = x32 += x1025
        x1024 = x33

        ()
    }
    val x1486 = x1024
    val x1487 = x1486._1
    val x1488 = x1486._2
    var x1041: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]] = x220
    val x1491 = x1490.foreach {
      x1042 =>
        val x1043 = x1041
        val x1044 = x1043.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]
        val x1046 = x219 += x1042
        x1041 = x220

        ()
    }
    val x1492 = x1041
    val x1493 = x1492._1
    val x1494 = x1492._2
    var x1058: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]] = x329
    val x1497 = x1496.foreach {
      x1059 =>
        val x1060 = x1058
        val x1061 = x1060.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[java.lang.String]]]
        val x1063 = x328 += x1059
        x1058 = x329

        ()
    }
    val x1498 = x1058
    val x1499 = x1498._1
    val x1500 = x1498._2
    var x1076: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x31
    val x1504 = x1503.foreach {
      x1077 =>
        val x1078 = x1076
        val x1079 = x1078.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
        val x1081 = x30 += x1077
        x1076 = x31

        ()
    }
    val x1505 = x1076
    val x1506 = x1505._1
    val x1507 = x1505._2
    var x1094: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]] = x284
    val x1511 = x1510.foreach {
      x1095 =>
        val x1096 = x1094
        val x1097 = x1096.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Char]]]
        val x1099 = x283 += x1095
        x1094 = x284

        ()
    }
    val x1512 = x1094
    val x1513 = x1512._1
    val x1514 = x1512._2
    var x1112: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x23
    val x1518 = x1517.foreach {
      x1113 =>
        val x1114 = x1112
        val x1115 = x1114.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
        val x1117 = x22 += x1113
        x1112 = x23

        ()
    }
    val x1519 = x1112
    val x1520 = x1519._1
    val x1521 = x1519._2
    var x1130: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x267
    val x1525 = x1524.foreach {
      x1131 =>
        val x1132 = x1130
        val x1133 = x1132.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
        val x1135 = x266 += x1131
        x1130 = x267

        ()
    }
    val x1526 = x1130
    val x1527 = x1526._1
    val x1528 = x1526._2
    var x1148: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]] = x226
    val x1532 = x1531.foreach {
      x1149 =>
        val x1150 = x1148
        val x1151 = x1150.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]]
        val x1153 = x225 += x1149
        x1148 = x226

        ()
    }
    val x1533 = x1148
    val x1534 = x1533._1
    val x1535 = x1533._2
    var x1166: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]] = x13
    val x1539 = x1538.foreach {
      x1167 =>
        val x1168 = x1166
        val x1169 = x1168.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Double]]]
        val x1171 = x12 += x1167
        x1166 = x13

        ()
    }
    val x1540 = x1166
    val x1541 = x1540._1
    val x1542 = x1540._2
    var x1189: scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]] = x49
    val x1551 = x1550.foreach {
      x1190 =>
        val x1191 = x1189
        val x1192 = x1191.asInstanceOf[scala.Tuple2[java.lang.String, scala.collection.mutable.ArrayBuilder[Int]]]
        val x1194 = x48 += x1190
        x1189 = x49

        ()
    }

    java.lang.System.gc()
    println("Start query")
    val start = now

    val x1552 = x1189
    val x1553 = x1552._1
    val x1554 = x1552._2
    val x1295 = new java.util.HashMap[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]()
    val x913 = x912.result
    val x914 = x913.length
    val x1555 = x1554.result
    val x1558 = x1514.result
    val x1560 = x1488.result
    val x1564 = x1462.result
    val x1566 = x1443.result
    val x1568 = x1542.result
    val x1572 = x1535.result
    var x1297: Int = 0
    val x1620 = while (x1297 < x914) {
      val x1556 = x1555.apply(x1297)
      val x1557 = x1556 <= 19981201
      val x1618 = if (x1557) {
        val x1559 = x1558.apply(x1297)
        val x1561 = x1560.apply(x1297)
        val x1562 = (x1559, x1561)
        val x1563 = x1295.containsKey(x1562)
        val x1615 = if (x1563) {
          val x1584 = x1295.get(x1562)
          val x1585 = x1584._1
          val x1586 = x1584._2
          val x1565 = x1564.apply(x1297)
          val x1587 = x1585 + x1565
          val x1567 = x1566.apply(x1297)
          val x1588 = x1586._1
          val x1589 = x1588 + x1567
          val x1569 = x1568.apply(x1297)
          val x1570 = 1.0 - x1569
          val x1571 = x1567 * x1570
          val x1590 = x1586._2
          val x1591 = x1590._1
          val x1592 = x1591 + x1571
          val x1573 = x1572.apply(x1297)
          val x1574 = 1.0 + x1573
          val x1575 = x1570 * x1574
          val x1576 = x1567 * x1575
          val x1593 = x1590._2
          val x1594 = x1593._1
          val x1595 = x1594 + x1576
          val x1596 = x1593._2
          val x1597 = x1596._1
          val x1598 = x1597 + x1565
          val x1599 = x1596._2
          val x1600 = x1599._1
          val x1601 = x1600 + x1567
          val x1602 = x1599._2
          val x1603 = x1602._1
          val x1604 = x1603 + x1569
          val x1605 = x1602._2
          val x1606 = x1605 + 1
          val x1607 = (x1604, x1606)
          val x1608 = (x1601, x1607)
          val x1609 = (x1598, x1608)
          val x1610 = (x1595, x1609)
          val x1611 = (x1592, x1610)
          val x1612 = (x1589, x1611)
          val x1613 = (x1587, x1612)
          x1613
        } else {
          val x1565 = x1564.apply(x1297)
          val x1567 = x1566.apply(x1297)
          val x1569 = x1568.apply(x1297)
          val x1570 = 1.0 - x1569
          val x1571 = x1567 * x1570
          val x1573 = x1572.apply(x1297)
          val x1574 = 1.0 + x1573
          val x1575 = x1570 * x1574
          val x1576 = x1567 * x1575
          val x1577 = (x1569, 1)
          val x1578 = (x1567, x1577)
          val x1579 = (x1565, x1578)
          val x1580 = (x1576, x1579)
          val x1581 = (x1571, x1580)
          val x1582 = (x1567, x1581)
          val x1583 = (x1565, x1582)
          x1583
        }
        val x1616 = x1295.put(x1562, x1615)
        x1616
      } else {
        ()
      }

      x1297 = x1297 + 1
    }
    val x1621 = scala.collection.JavaConverters.asScalaSetConverter(x1295.keySet).asScala.toIterable
    val x1622 = x1621.toArray
    val x1623 = x1622.length
    var x1628 = new Array[scala.Tuple2[scala.Tuple2[Char, Char], scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]](x1623)
    for (x1386 <- 0 until x1623) {
      val x1624 = x1622.apply(x1386)
      val x1625 = x1295.get(x1624)
      val x1626 = (x1624, x1625)
      x1628(x1386) = x1626
    }
    val x1629 = x1628.length
    val x1434 = { x1428: (scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]) =>
      val x1429 = x1428._1
      val x1430 = x1428._2
      val x1431 = x1430._1
      val x1433 = (x1429, x1431)
      x1433: scala.Tuple2[Char, Char]
    }
    var x1663 = new Array[scala.Tuple2[Char, scala.Tuple2[Char, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, scala.Tuple2[Double, Int]]]]]]]]]](x1629)
    for (x1393 <- 0 until x1629) {
      val x1630 = x1628.apply(x1393)
      val x1631 = x1630._1
      val x1632 = x1630._2
      val x1633 = x1631._1
      val x1634 = x1631._2
      val x1635 = x1632._1
      val x1636 = x1632._2
      val x1637 = x1636._1
      val x1638 = x1636._2
      val x1639 = x1638._1
      val x1640 = x1638._2
      val x1641 = x1640._1
      val x1642 = x1640._2
      val x1643 = x1642._1
      val x1644 = x1642._2
      val x1645 = x1644._2
      val x1646 = x1645._2
      val x1647 = x1646.toDouble
      val x1648 = x1643 / x1647
      val x1649 = x1644._1
      val x1650 = x1649 / x1647
      val x1651 = x1645._1
      val x1652 = x1651 / x1647
      val x1653 = (x1652, x1646)
      val x1654 = (x1650, x1653)
      val x1655 = (x1648, x1654)
      val x1656 = (x1641, x1655)
      val x1657 = (x1639, x1656)
      val x1658 = (x1637, x1657)
      val x1659 = (x1635, x1658)
      val x1660 = (x1634, x1659)
      val x1661 = (x1633, x1660)
      x1663(x1393) = x1661
    }
    val x1664 = x1663.sortBy(x1434)
    val x1435 = x1664
    println("Elapsed time: " + (now - start))   
    x1435
  }
}
/*****************************************
  End of Generated Code                  
*******************************************/
def now: Long = java.lang.System.currentTimeMillis()
val in = scala.io.Source.fromFile("/home/knizhnik/tpch-data/sf1/lineitem.tbl").getLines().map(s => s.split("\\|")).toArray
val benchmark = new TPCH_Q1_ver_seq()
val result = benchmark(in)
