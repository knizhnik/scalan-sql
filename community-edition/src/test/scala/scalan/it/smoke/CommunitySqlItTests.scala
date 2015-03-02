package scalan.it.smoke

import scalan.{ScalanCommunity, ScalanCtxSeq}
//import scalan.community._
import scalan.collections._
import scalan.sql._

/**
 *  Tests that very simple examples are run correctly
 */
//abstract class CommunitySqlItTests extends SmokeItTests {
//
//  trait ProgCommunity extends Prog with ScalanCommunity with MultiMapsDsl with SqlDsl with TablesDsl with Queries  {
//    lazy val selectUsingIndex = fun { in: Rep[Array[(Int, Double)]] =>
//      val table = Table.create[(Int, Double)]("t1").primaryKey(r => r._1).secondaryKey(r => r._2).insertFrom(in)
//      table.where(r => ((r._1 === 3) && (r._2 > 0.0))).select(r => (r._1, r._2 * 2.0)).orderBy(r => r._2)
//    }
//
//    lazy val innerJoin = fun { in: Rep[(Array[(Int, Double)], Array[(String, Int)])] =>
//      val outer = Table.create[(Int, Double)]("outer").primaryKey(_._1).secondaryKey(_._2).insertFrom(in._1)
//      val inner = Table.create[(String, Int)]("inner").primaryKey(_._2).insertFrom(in._2)
//      outer.where(r => (r._2 === 1.0)).join(inner)(_._1, _._2).orderBy(_._3)
//    }
//
//    lazy val hashJoin = fun { in: Rep[(Array[(Int, Double)], Array[(String, Int)])] =>
//      val outer = Table.create[(Int, Double)]("outer").primaryKey(_._1).secondaryKey(_._2).insertFrom(in._1)
//      val inner = Table.create[(String, Int)]("inner").primaryKey(_._1).insertFrom(in._2)
//      outer.where(r => (r._2 === 1.0)).join(inner)(_._1, _._2).orderBy(_._2)
//    }
//
//    lazy val selectCount = fun { in: Rep[Array[(Int, Double)]] =>
//      val table = Table.create[(Int, Double)]("t1").primaryKey(_._1).insertFrom(in)
//      table.where(_._2 >= 0.0).count
//    }
//
//    lazy val groupBy = fun { in: Arr[(Int, Double)] =>
//      Table.create[(Int, Double)]("t1").insertFrom(in).groupBy(_._1 % 2).reduce(grp => grp.map(_._2).sum).toArray.sortBy(fun { r => r._1})
//    }
///*
//    lazy val tpchQ1 =  fun { in: Arr[Array[String]]  =>
//      val data = Array.tabulate(in.length)(i => LINEITEM.create(in(i)))
//      val lineitem = ReadOnlyTable(data)
//      lineitem.where(r => r.l_shipdate <= 19980811).mapReduce[(Char,Char),(Double,(Double,(Double,(Double,(Double,Int)))))](
//        r => Pair(Pair(r.l_returnflag,r.l_linestatus),
//                  Pair(r.l_quantity,
//                  Pair(r.l_extendedprice,
//                  Pair(r.l_extendedprice*(toRep(1.0) - r.l_discount),
//                  Pair(r.l_extendedprice*(toRep(1.0) - r.l_discount)*(r.l_tax + 1.0),
//                  Pair(r.l_discount,
//                  toRep(1))))))),
//        (s1,s2)  => (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3, s1._4 + s2._4, s1._5 + s2._5, s1._6 + s2._6)).toArray.sortBy(fun { r => r.head })
//        .map(r => Pair(r.head._1, Pair(r.head._2, Pair(r.tail._1, Pair(r.tail._2, Pair(r.tail._3, Pair(r.tail._4, Pair(r.tail._1/r.tail._6.toDouble,
//        Pair(r.tail._2/r.tail._6.toDouble, Pair(r.tail._5/r.tail._6.toDouble, r.tail._6))))))))))
//    }
//*/
//    lazy val tpchQ1 =  fun { in: Arr[Array[String]]  =>
//      val data = SArray.tabulate(in.length)(i => LINEITEM.create(in(i)))
//      val lineitem = ReadOnlyTable(data)
//      lineitem.where(r => r.l_shipdate <= 19980811).mapReduce(
//        r => Pair(Pair(r.l_returnflag,r.l_linestatus),
//                  Pair(r.l_quantity,
//                  Pair(r.l_extendedprice,
//                  Pair(r.l_extendedprice*(toRep(1.0) - r.l_discount),
//                  Pair(r.l_extendedprice*(toRep(1.0) - r.l_discount)*(r.l_tax + 1.0),
//                  Pair(r.l_discount,
//                  toRep(1))))))),
//        (s1:Rep[(Double,(Double,(Double,(Double,(Double,Int)))))],s2:Rep[(Double,(Double,(Double,(Double,(Double,Int)))))])  => (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3, s1._4 + s2._4, s1._5 + s2._5, s1._6 + s2._6)).toArray.sortBy(fun { r => r.head })
//        .map(r => Pair(r.head._1, Pair(r.head._2, Pair(r.tail._1, Pair(r.tail._2, Pair(r.tail._3, Pair(r.tail._4, Pair(r.tail._1/r.tail._6.toDouble,
//        Pair(r.tail._2/r.tail._6.toDouble, Pair(r.tail._5/r.tail._6.toDouble, r.tail._6))))))))))
//    }
//
//    lazy val TPCH_Q1_hor_seq = fun { in: Arr[Array[String]] =>
//      val data = in.map(r => parseLineitem(r))
//      val lineitem = ReadOnlyTable(data)
//      Q1(lineitem)
//    }
//
//    lazy val TPCH_Q1_ver_seq = fun { in: Arr[Array[String]] =>
//      val data = in.map(r => parseLineitem(r))
//      val lineitem = createLineitem("Lineitem").insertFrom(data)
//      Q1(lineitem)
//    }
//
//    lazy val TPCH_Q1_hor_par = fun { in: Arr[Array[String]] =>
//      val nJobs = 4
//      val data = in.map(r => parseLineitem(r))
//      val lineitem = ShardedTable.create[Lineitem]("dist", nJobs, (node: Rep[Int]) => Table.createShard[Lineitem](node), (r: Rep[Lineitem]) => r._1.hashcode).insertFrom(data)
//      Q1(lineitem)
//    }
//
//    lazy val TPCH_Q1_ver_par = fun { in: Arr[Array[String]] =>
//      val nJobs = 4
//      val data = in.map(r => parseLineitem(r))
//      val lineitem = ShardedTable.create[Lineitem]("dist", nJobs, (node: Rep[Int]) => createLineitem(node.toStr), (r: Rep[Lineitem]) => r._1.hashcode).insertFrom(data)
//      Q1(lineitem)
//    }
//
//   lazy val TPCH_Q3_hor_seq = fun { in: Arr[Array[Array[String]]] =>
//      val customerData = in(0).map(r => parseCustomer(r))
//      val ordersData = in(1).map(r => parseOrders(r))
//      val lineitemData = in(2).map(r => parseLineitem(r))
//      val customer = Table.create[Customer]("Customer").primaryKey(customer_pk).insertFrom(customerData)
//      val orders = Table.create[Orders]("Orders").primaryKey(orders_pk).secondaryKey(orders_cust_fk).insertFrom(ordersData)
//      val lineitem = Table.create[Lineitem]("Lineitem").primaryKey(lineitem_pk).insertFrom(lineitemData)
//      //val customer = ReadOnlyTable(customerData).primaryKey(customer_pk)
//      //val orders = ReadOnlyTable(ordersData).secondaryKey(orders_cust_fk)
//      //val lineitem = ReadOnlyTable(lineitemData).primaryKey(lineitem_pk).secondaryKey(lineitem_order_fk)
//      Q3(customer, orders, lineitem)
//    }
//
//    lazy val TPCH_Q3_ver_seq = fun { in: Arr[Array[Array[String]]] =>
//      val customerData = in(0).map(r => parseCustomer(r))
//      val ordersData = in(1).map(r => parseOrders(r))
//      val lineitemData = in(2).map(r => parseLineitem(r))
//      val customer = createCustomer("Customer").primaryKey(customer_pk).insertFrom(customerData)
//      val orders = createOrders("Orders").primaryKey(orders_pk).secondaryKey(orders_cust_fk).insertFrom(ordersData)
//      val lineitem = createLineitem("Lineitem").primaryKey(lineitem_pk).secondaryKey(lineitem_order_fk).insertFrom(lineitemData)
//      Q3(customer, orders, lineitem)
//   }
//
//    lazy val TPCH_Q3_hor_par = fun { in: Arr[Array[Array[String]]] =>
//      val customerData = in(0).map(r => parseCustomer(r))
//      val ordersData = in(1).map(r => parseOrders(r))
//      val lineitemData = in(2).map(r => parseLineitem(r))
//      val nJobs = 8
//      val customer = ShardedTable.create[Customer]("CustomerHDist", nJobs, (node: Rep[Int]) => Table.createShard[Customer](node).primaryKey(customer_pk), (r: Rep[Customer]) => r._1.hashcode).insertFrom(customerData)
//      val orders = ShardedTable.create[Orders]("OrdersHDist", nJobs, (node: Rep[Int]) => Table.createShard[Orders](node).primaryKey(orders_pk).secondaryKey(orders_cust_fk), (r: Rep[Orders]) => r._1.hashcode).insertFrom(ordersData)
//      val lineitem = ShardedTable.create[Lineitem]("LineitemHDist", nJobs, (node: Rep[Int]) => Table.createShard[Lineitem](node).primaryKey(lineitem_pk).secondaryKey(lineitem_order_fk), (r: Rep[Lineitem]) => r._1.hashcode).insertFrom(lineitemData)
//      Q3(customer, orders, lineitem)
//    }
//
//    lazy val TPCH_Q3_ver_par = fun { in: Arr[Array[Array[String]]] =>
//      val customerData = in(0).map(r => parseCustomer(r))
//      val ordersData = in(1).map(r => parseOrders(r))
//      val lineitemData = in(2).map(r => parseLineitem(r))
//      val nJobs = 8
//      val customer = ShardedTable.create[Customer]("CustomerVDist", nJobs, (node: Rep[Int]) => createCustomer(node.toStr).primaryKey(customer_pk), (r: Rep[Customer]) => r._1.hashcode).insertFrom(customerData)
//      val orders = ShardedTable.create[Orders]("OrdersVDist", nJobs, (node: Rep[Int]) => createOrders(node.toStr).primaryKey(orders_pk).secondaryKey(orders_cust_fk), (r: Rep[Orders]) => r._1.hashcode).insertFrom(ordersData)
//      val lineitem = ShardedTable.create[Lineitem]("LineitemVDist", nJobs, (node: Rep[Int]) => createLineitem(node.toStr).primaryKey(lineitem_pk).secondaryKey(lineitem_order_fk), (r: Rep[Lineitem]) => r._1.hashcode).insertFrom(lineitemData)
//      Q3(customer, orders, lineitem)
//    }
//
//   lazy val TPCH_Q10_hor_seq = fun { in: Arr[Array[Array[String]]] =>
//      val customerData = in(0).map(r => parseCustomer(r))
//      val ordersData = in(1).map(r => parseOrders(r))
//      val lineitemData = in(2).map(r => parseLineitem(r))
//      val nationData = in(3).map(r => parseNation(r))
//      val customer = Table.create[Customer]("Customer").primaryKey(customer_pk).insertFrom(customerData)
//      val orders = Table.create[Orders]("Orders").insertFrom(ordersData)
//      val lineitem = Table.create[Lineitem]("Lineitem").secondaryKey(lineitem_order_fk).insertFrom(lineitemData)
//      val nation = Table.create[Nation]("Nation").primaryKey(nation_pk).insertFrom(nationData)
//      Q10(orders, customer, lineitem, nation)
//    }
//
//    lazy val TPCH_Q10_ver_seq = fun { in: Arr[Array[Array[String]]] =>
//      val customerData = in(0).map(r => parseCustomer(r))
//      val ordersData = in(1).map(r => parseOrders(r))
//      val lineitemData = in(2).map(r => parseLineitem(r))
//      val nationData = in(3).map(r => parseNation(r))
//      val customer = createCustomer("Customer").primaryKey(customer_pk).insertFrom(customerData)
//      val orders = createOrders("Orders").primaryKey(orders_pk).insertFrom(ordersData)
//      val lineitem = createLineitem("Lineitem").secondaryKey(lineitem_order_fk).insertFrom(lineitemData)
//      val nation = createNation("Nation").primaryKey(nation_pk).insertFrom(nationData)
//      Q10(orders, customer, lineitem, nation)
//   }
//
//    lazy val TPCH_Q10_hor_par = fun { in: Arr[Array[Array[String]]] =>
//      val customerData = in(0).map(r => parseCustomer(r))
//      val ordersData = in(1).map(r => parseOrders(r))
//      val lineitemData = in(2).map(r => parseLineitem(r))
//      val nationData = in(3).map(r => parseNation(r))
//      val nJobs = 8
//      val customer = ShardedTable.create[Customer]("CustomerHDist", nJobs, (node: Rep[Int]) => Table.createShard[Customer](node).primaryKey(customer_pk), (r: Rep[Customer]) => r._1.hashcode).insertFrom(customerData)
//      val orders = ShardedTable.create[Orders]("OrdersHDist", nJobs, (node: Rep[Int]) => Table.createShard[Orders](node).primaryKey(orders_pk), (r: Rep[Orders]) => r._1.hashcode).insertFrom(ordersData)
//      val lineitem = ShardedTable.create[Lineitem]("LineitemHDist", nJobs, (node: Rep[Int]) => Table.createShard[Lineitem](node).secondaryKey(lineitem_order_fk), (r: Rep[Lineitem]) => r._1.hashcode).insertFrom(lineitemData)
//      val nation = ShardedTable.create[Nation]("NationHDist", nJobs, (node: Rep[Int]) => Table.createShard[Nation](node).primaryKey(nation_pk), (r: Rep[Nation]) => r._1.hashcode).insertFrom(nationData)
//      Q10(orders, customer, lineitem, nation)
//    }
//
//    lazy val TPCH_Q10_ver_par = fun { in: Arr[Array[Array[String]]] =>
//      val customerData = in(0).map(r => parseCustomer(r))
//      val ordersData = in(1).map(r => parseOrders(r))
//      val lineitemData = in(2).map(r => parseLineitem(r))
//      val nationData = in(3).map(r => parseNation(r))
//      val nJobs = 8
//      val customer = ShardedTable.create[Customer]("CustomerVDist", nJobs, (node: Rep[Int]) => createCustomer(node.toStr).primaryKey(customer_pk), (r: Rep[Customer]) => r._1.hashcode).insertFrom(customerData)
//      val orders = ShardedTable.create[Orders]("OrdersVDist", nJobs, (node: Rep[Int]) => createOrders(node.toStr).primaryKey(orders_pk), (r: Rep[Orders]) => r._1.hashcode).insertFrom(ordersData)
//      val lineitem = ShardedTable.create[Lineitem]("LineitemVDist", nJobs, (node: Rep[Int]) => createLineitem(node.toStr).secondaryKey(lineitem_order_fk), (r: Rep[Lineitem]) => r._1.hashcode).insertFrom(lineitemData)
//      val nation = ShardedTable.create[Nation]("NationVDist", nJobs, (node: Rep[Int]) => createNation(node.toStr).primaryKey(nation_pk), (r: Rep[Nation]) => r._1.hashcode).insertFrom(nationData)
//      Q10(orders, customer, lineitem, nation)
//   }
//
//   type Record = (Int, String)
//
//    lazy val sqlDsl = fun { n: Rep[Int] =>
//      val input = SArray.tabulate(n)(i => Detail.create(i, "Some detail", 1.0))
//      val table = ReadOnlyTable(input).primaryKey(_.id)
//      table.where(r => r.id === 1).select(r => r.desc).toArray
//    }
//
//    // Doesn't work: wrong transformation
//    lazy val sqlDsl2 = fun { n: Rep[Int] =>
//      val input = SArray.tabulate(n)(i => Detail.create(i, "Some detail", 1.0))
//      val table = Table.create[Detail]("detail").insertFrom(input)
////      val table = Table.create[Detail]("detail").primaryKey(_.id).insertFrom(input)
//      table.where(r => r.id === 1).select(r => r.desc).toArray
//    }
//
//    // Doesn't work: wrong transformation
//    lazy val sqlDsl3 = fun { n: Rep[Int] =>
//      val table = Table.create[Detail]("detail").insert(Detail.create(1, "Some detail", 1.0))
//      table.where(r => r.id === 1).select(r => r.desc).toArray
//    }
//
//    // Doesn't work: generates wrong code
//    lazy val sqlDsl4 = fun { n: Rep[Int] =>
//      val input = SArray.tabulate(n)(i => Detail.create(i, "Some detail", 1.0))
//      input.update(1, Detail.create(1, "Another detail", 2.0)).map(_.desc)
//    }
//
//    lazy val sqlBenchmark = fun { in: Arr[Record] =>
//      val table = Table.create[Record]("t1"). /*primaryKey(r => r._1).*/ insertFrom(in)
//      (table.where(r => r._2.contains("123") && r._1 > 0).count,
//        table.sum(_._1),
//        table.where(_._2.startsWith("1")).sum(_._1))
//    }
//
//    lazy val columnarStore = fun { in: Arr[(Int, String)] =>
//      val table = PairTable.create(Table.create[Int]("c1"), Table.create[String]("c2")).primaryKey(r => r._1).insertFrom(in)
//      table.sum(_._1)
//    }
//
//    lazy val columnarStoreR3 = fun { in: Arr[(String, (Int, Double))] =>
//      val table = PairTable.create(Table.create[String]("c1"), PairTable.create(Table.create[Int]("c2"), Table.create[Double]("c3"))).primaryKey(r => r._1).insertFrom(in)
//      table.mapReduce(x => (x._1, x._3), (s1:Rep[Double], s2:Rep[Double]) => s1 + s2).toArray.sortBy(fun { p => p._1 })
//    }
//
//    def doQueries(table: Rep[Table[Record]]) = {
//      (table.where(r => r._2.contains("123") && r._1 > 0).count,
//        table.where(r => r._2.matches(".*1.*2.*3.*")).count,
//        table.sum(_._1),
//        table.where(_._2.startsWith("1")).sum(_._1))
//    }
//
//    lazy val sqlParBenchmark = fun { in: Arr[Record] =>
//      val table = ShardedTable.create[Record]("dist", 4, (node: Rep[Int]) => Table.createShard[Record](node), (r: Rep[Record]) => r._1.hashcode).insertFrom(in)
//      doQueries(table)
//    }
//
//    lazy val sqlColumnarStoreBenchmark = fun { in: Rep[(Int, Array[Record])] =>
//      val table = ShardedTable.create[Record]("dist", in._1, (node: Rep[Int]) => PairTable.create[Int,String](Table.create[Int](toRep("left") + node.toStr),
//                                                                                                              Table.create[String](toRep("right") + node.toStr)),
//                                              (r: Rep[Record]) => r._1.hashcode).insertFrom(in._2)
//      (table.where(r => r._2.contains("123") && r._1 > 0).count,
//        table.where(r => r._2.contains("123")).count,
//        table.sum(_._1),
//        table.where(_._2.startsWith("1")).sum(_._1))
//    }
//
//
//    lazy val sqlIndexBenchmark = fun { in: Arr[Record] =>
//      val n_records = in.length
//      val table = Table.create[Record]("t1").primaryKey(r => r._1).insertFrom(in)
//      loopUntil2(1, 0)(
//      { (i, sum) => i > n_records}, { (i, sum) => (i + 1, sum + table.where(_._1 === i).singleton._1)}
//      )
//    }
//
//    lazy val sqlGroupBy = fun { in: Arr[Record] =>
//      val table = ShardedTable.create[Record]("dist", 4, (node: Rep[Int]) => Table.createShard[Record](node), (r: Rep[Record]) => r._1.hashcode).insertFrom(in)
//      table.groupBy(_._2).reduce(grp => grp.sumBy(fun { r => r._1})).toArray.sortBy(fun { r => r._2})
//    }
//
//    lazy val sqlMapReduce = fun { in: Rep[(Int, Array[Record])] =>
//      val table = ShardedTable.create[Record]("dist", in._1, (node: Rep[Int]) => Table.createShard[Record](node), (r: Rep[Record]) => r._1.hashcode).insertFrom(in._2)
//      table.mapReduce[String, Int](r => (r._2, r._1), (s1,s2) => s1 + s2 ).toArray.sortBy(fun { r => r._2})
//    }
//
//    lazy val sqlParallelJoin = fun { in: Rep[(Array[Record], (Array[Record], Array[Record]))] =>
//      val t1 = ShardedTable.create[Record]("t1", 4, (node: Rep[Int]) => Table.createShard[Record](node).primaryKey(_._1), (r: Rep[Record]) => r._1.hashcode).insertFrom(in._1)
//      val t2 = ShardedTable.create[Record]("t2", 4, (node: Rep[Int]) => Table.createShard[Record](node).primaryKey(_._1), (r: Rep[Record]) => r._1.hashcode).insertFrom(in._2)
//      val t3 = ShardedTable.create[Record]("t3", 4, (node: Rep[Int]) => Table.createShard[Record](node).primaryKey(_._1), (r: Rep[Record]) => r._1.hashcode).insertFrom(in._3)
//      /*
//      (t1.join(t2)(_._1, _._1).join(t3)(_._1._1, _._1).singleton,
//       t1.where(r => r._1 === 2).join(t2)(_._1, _._1).join(t3)(_._1._1, _._1).singleton,
//       t1.where(r => r._2 matches("1.*")).join(t2)(_._1, _._1).join(t3)(_._1._1, _._1).select(r => (r._1._1._1, r._1._1._2 + "-" + r._1._2._2 + "-" + r._2._2)))
//*/
//      (t1.join(t2)(_.head, _.head).join(t3)(_.head.head, _.head).singleton,
//        t1.where(r => r.head === 2).join(t2)(_.head, _.head).join(t3)(_.head.head, _.head).singleton,
//        t1.where(r => r.tail matches (".*ee.*")).join(t2)(_.head, _.head).join(t3)(_.head.head, _.head).select(r => (r.head.head.head, r.head.head.tail + "-" + r.head.tail.tail + "-" + r.tail.tail)).singleton)
//    }
//
//    lazy val sqlIndexJoin = fun { in: Rep[(Array[Record], Array[Record])] =>
//      val outer = Table.create[Record]("outer").primaryKey(_.head).insertFrom(in.head)
//      val inner = Table.create[Record]("inner").secondaryKey(_.head).insertFrom(in.tail)
//      outer.join(inner)(_.head, _.head).orderBy(_.tail.tail)
//    }
//
//
//    type Relation = (Int, Int)
//
//    lazy val sqlAggJoin = fun { in: Rep[(Array[Relation], (Array[Relation], (Array[Relation], (Array[Relation], (Array[Relation], Int)))))] =>
//      val t1 = ShardedTable.create[Relation]("t1", in._6, (node: Rep[Int]) => Table.createShard[Relation](node), (r: Rep[Relation]) => r._1.hashcode).insertFrom(in._1)
//      val t2 = Table.create[Relation]("t2").primaryKey(_._1).insertFrom(in._2)
//      val t3 = Table.create[Relation]("t3").primaryKey(_._1).insertFrom(in._3)
//      val t4 = Table.create[Relation]("t4").primaryKey(_._1).insertFrom(in._4)
//      val t5 = Table.create[Relation]("t5").primaryKey(_._1).insertFrom(in._5)
//      t1.where(r => r.head % 3 === 0).join(t2)(_.tail, _.head).join(t3)(_.tail.tail, _.head).join(t4)(_.tail.tail, _.head).join(t5)(_.tail.tail, _.head).where(r => r.tail.tail % 3 === 0).sum(_.tail.tail)
//    }
//
//
//    lazy val testTuple = fun { in: Rep[(Array[Record], Array[Record])] =>
//      in._1.zip(in._2).map(_.head.head)
//    }
//  }
//
//  class ProgCommunitySeq extends ProgCommunity with ScalanCommunityDslSeq with MultiMapsDslSeq with SqlDslSeq with TablesDslSeq {
//  }
//
//  override val progSeq: ProgCommunitySeq = new ProgCommunitySeq
//}
