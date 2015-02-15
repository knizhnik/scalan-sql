package scalan.sql
import scalan._

trait Queries extends ScalanDsl with SqlDsl {

implicit class StringFormatter(str: Rep[String]) {
  def toDate: Rep[Int] = (str.substring(0, 4) + str.substring(5, 7) + str.substring(8, 10)).toInt
  def toChar: Rep[Char] = str(0)
}

type Lineitem = (Int, (Int, (Int, (Int, (Double, (Double, (Double, (Double, (Char, (Char, (Int, (Int, (Int, (String, (String, String)))))))))))))))

def createLineitem(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".l_orderkey"), PairTable.create(Table.create[Int](tableName + ".l_partkey"), PairTable.create(Table.create[Int](tableName + ".l_suppkey"), PairTable.create(Table.create[Int](tableName + ".l_linenumber"), PairTable.create(Table.create[Double](tableName + ".l_quantity"), PairTable.create(Table.create[Double](tableName + ".l_extendedprice"), PairTable.create(Table.create[Double](tableName + ".l_discount"), PairTable.create(Table.create[Double](tableName + ".l_tax"), PairTable.create(Table.create[Char](tableName + ".l_returnflag"), PairTable.create(Table.create[Char](tableName + ".l_linestatus"), PairTable.create(Table.create[Int](tableName + ".l_shipdate"), PairTable.create(Table.create[Int](tableName + ".l_commitdate"), PairTable.create(Table.create[Int](tableName + ".l_receiptdate"), PairTable.create(Table.create[String](tableName + ".l_shipinstruct"), PairTable.create(Table.create[String](tableName + ".l_shipmode"), Table.create[String](tableName + ".l_comment"))))))))))))))))

def parseLineitem(c: Arr[String]): Rep[Lineitem] = Pair(c(0).toInt, Pair(c(1).toInt, Pair(c(2).toInt, Pair(c(3).toInt, Pair(c(4).toDouble, Pair(c(5).toDouble, Pair(c(6).toDouble, Pair(c(7).toDouble, Pair(c(8).toChar, Pair(c(9).toChar, Pair(c(10).toDate, Pair(c(11).toDate, Pair(c(12).toDate, Pair(c(13), Pair(c(14), c(15))))))))))))))))

implicit class Lineitem_class(self: Rep[Lineitem]) {
  def l_orderkey = self._1
  def l_partkey = self._2
  def l_suppkey = self._3
  def l_linenumber = self._4
  def l_quantity = self._5
  def l_extendedprice = self._6
  def l_discount = self._7
  def l_tax = self.tail.tail.tail.tail.tail.tail.tail.head
  def l_returnflag = self.tail.tail.tail.tail.tail.tail.tail.tail.head
  def l_linestatus = self.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  def l_shipdate = self.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  def l_commitdate = self.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  def l_receiptdate = self.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  def l_shipinstruct = self.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  def l_shipmode = self.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  def l_comment = self.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail
}



type Orders = (Int, (Int, (Char, (Double, (Int, (String, (String, (Int, String))))))))

def createOrders(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".o_orderkey"), PairTable.create(Table.create[Int](tableName + ".o_custkey"), PairTable.create(Table.create[Char](tableName + ".o_orderstatus"), PairTable.create(Table.create[Double](tableName + ".o_totalprice"), PairTable.create(Table.create[Int](tableName + ".o_orderdate"), PairTable.create(Table.create[String](tableName + ".o_orderpriority"), PairTable.create(Table.create[String](tableName + ".o_clerk"), PairTable.create(Table.create[Int](tableName + ".o_shippriority"), Table.create[String](tableName + ".o_comment")))))))))

def parseOrders(c: Arr[String]): Rep[Orders] = Pair(c(0).toInt, Pair(c(1).toInt, Pair(c(2).toChar, Pair(c(3).toDouble, Pair(c(4).toDate, Pair(c(5), Pair(c(6), Pair(c(7).toInt, c(8)))))))))

implicit class Orders_class(self: Rep[Orders]) {
  def o_orderkey = self._1
  def o_custkey = self._2
  def o_orderstatus = self._3
  def o_totalprice = self._4
  def o_orderdate = self._5
  def o_orderpriority = self._6
  def o_clerk = self._7
  def o_shippriority = self.tail.tail.tail.tail.tail.tail.tail.head
  def o_comment = self.tail.tail.tail.tail.tail.tail.tail.tail
}



type Customer = (Int, (String, (String, (Int, (String, (Double, (String, String)))))))

def createCustomer(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".c_custkey"), PairTable.create(Table.create[String](tableName + ".c_name"), PairTable.create(Table.create[String](tableName + ".c_address"), PairTable.create(Table.create[Int](tableName + ".c_nationkey"), PairTable.create(Table.create[String](tableName + ".c_phone"), PairTable.create(Table.create[Double](tableName + ".c_acctbal"), PairTable.create(Table.create[String](tableName + ".c_mktsegment"), Table.create[String](tableName + ".c_comment"))))))))

def parseCustomer(c: Arr[String]): Rep[Customer] = Pair(c(0).toInt, Pair(c(1), Pair(c(2), Pair(c(3).toInt, Pair(c(4), Pair(c(5).toDouble, Pair(c(6), c(7))))))))

implicit class Customer_class(self: Rep[Customer]) {
  def c_custkey = self._1
  def c_name = self._2
  def c_address = self._3
  def c_nationkey = self._4
  def c_phone = self._5
  def c_acctbal = self._6
  def c_mktsegment = self._7
  def c_comment = self._8
}



type Supplier = (Int, (String, (String, (Int, (String, (Double, String))))))

def createSupplier(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".s_suppkey"), PairTable.create(Table.create[String](tableName + ".s_name"), PairTable.create(Table.create[String](tableName + ".s_address"), PairTable.create(Table.create[Int](tableName + ".s_nationkey"), PairTable.create(Table.create[String](tableName + ".s_phone"), PairTable.create(Table.create[Double](tableName + ".s_acctbal"), Table.create[String](tableName + ".s_comment")))))))

def parseSupplier(c: Arr[String]): Rep[Supplier] = Pair(c(0).toInt, Pair(c(1), Pair(c(2), Pair(c(3).toInt, Pair(c(4), Pair(c(5).toDouble, c(6)))))))

implicit class Supplier_class(self: Rep[Supplier]) {
  def s_suppkey = self._1
  def s_name = self._2
  def s_address = self._3
  def s_nationkey = self._4
  def s_phone = self._5
  def s_acctbal = self._6
  def s_comment = self._7
}



type Partsupp = (Int, (Int, (Int, (Double, String))))

def createPartsupp(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".ps_partkey"), PairTable.create(Table.create[Int](tableName + ".ps_suppkey"), PairTable.create(Table.create[Int](tableName + ".ps_availqty"), PairTable.create(Table.create[Double](tableName + ".ps_supplycost"), Table.create[String](tableName + ".ps_comment")))))

def parsePartsupp(c: Arr[String]): Rep[Partsupp] = Pair(c(0).toInt, Pair(c(1).toInt, Pair(c(2).toInt, Pair(c(3).toDouble, c(4)))))

implicit class Partsupp_class(self: Rep[Partsupp]) {
  def ps_partkey = self._1
  def ps_suppkey = self._2
  def ps_availqty = self._3
  def ps_supplycost = self._4
  def ps_comment = self._5
}



type Region = (Int, (String, String))

def createRegion(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".r_regionkey"), PairTable.create(Table.create[String](tableName + ".r_name"), Table.create[String](tableName + ".r_comment")))

def parseRegion(c: Arr[String]): Rep[Region] = Pair(c(0).toInt, Pair(c(1), c(2)))

implicit class Region_class(self: Rep[Region]) {
  def r_regionkey = self._1
  def r_name = self._2
  def r_comment = self._3
}



type Nation = (Int, (String, (Int, String)))

def createNation(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".n_nationkey"), PairTable.create(Table.create[String](tableName + ".n_name"), PairTable.create(Table.create[Int](tableName + ".n_regionkey"), Table.create[String](tableName + ".n_comment"))))

def parseNation(c: Arr[String]): Rep[Nation] = Pair(c(0).toInt, Pair(c(1), Pair(c(2).toInt, c(3))))

implicit class Nation_class(self: Rep[Nation]) {
  def n_nationkey = self._1
  def n_name = self._2
  def n_regionkey = self._3
  def n_comment = self._4
}



type Part = (Int, (String, (String, (String, (String, (Int, (String, (Double, String))))))))

def createPart(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".p_partkey"), PairTable.create(Table.create[String](tableName + ".p_name"), PairTable.create(Table.create[String](tableName + ".p_mfgr"), PairTable.create(Table.create[String](tableName + ".p_brand"), PairTable.create(Table.create[String](tableName + ".p_type"), PairTable.create(Table.create[Int](tableName + ".p_size"), PairTable.create(Table.create[String](tableName + ".p_container"), PairTable.create(Table.create[Double](tableName + ".p_retailprice"), Table.create[String](tableName + ".p_comment")))))))))

def parsePart(c: Arr[String]): Rep[Part] = Pair(c(0).toInt, Pair(c(1), Pair(c(2), Pair(c(3), Pair(c(4), Pair(c(5).toInt, Pair(c(6), Pair(c(7).toDouble, c(8)))))))))

implicit class Part_class(self: Rep[Part]) {
  def p_partkey = self._1
  def p_name = self._2
  def p_mfgr = self._3
  def p_brand = self._4
  def p_type = self._5
  def p_size = self._6
  def p_container = self._7
  def p_retailprice = self.tail.tail.tail.tail.tail.tail.tail.head
  def p_comment = self.tail.tail.tail.tail.tail.tail.tail.tail
}



def lineitem_pk(r: Rep[Lineitem]) = Pair(r.l_orderkey, r.l_linenumber)

def lineitem_order_fk(r: Rep[Lineitem]) = r.l_orderkey

def lineitem_supp_fk(r: Rep[Lineitem]) = r.l_suppkey

def lineitem_part_fk(r: Rep[Lineitem]) = r.l_partkey

def lineitem_ps_fk(r: Rep[Lineitem]) = Pair(r.l_partkey, r.l_suppkey)

def part_pk(r: Rep[Part]) = r.p_partkey

def supplier_pk(r: Rep[Supplier]) = r.s_suppkey

def partsupp_pk(r: Rep[Partsupp]) = Pair(r.ps_partkey, r.ps_suppkey)

def partsupp_supp_fk(r: Rep[Partsupp]) = r.ps_suppkey

def partsupp_part_fk(r: Rep[Partsupp]) = r.ps_partkey

def customer_pk(r: Rep[Customer]) = r.c_custkey

def customer_nation_fk(r: Rep[Customer]) = r.c_nationkey

def orders_pk(r: Rep[Orders]) = r.o_orderkey

def orders_cust_fk(r: Rep[Orders]) = r.o_custkey

def nation_pk(r: Rep[Nation]) = r.n_nationkey

def nation_region_fk(r: Rep[Nation]) = r.n_regionkey

def region_pk(r: Rep[Region]) = r.r_regionkey

def Q1(lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(lineitem
	.where(r1 => (r1.l_shipdate <= toRep(19981201)))
	.mapReduce(r => Pair(Pair(r.l_returnflag, r.l_linestatus), Pair(r.l_quantity, Pair(r.l_extendedprice, Pair((r.l_extendedprice * (toRep(1.0) - r.l_discount)), Pair(((r.l_extendedprice * (toRep(1.0) - r.l_discount)) * (toRep(1.0) + r.l_tax)), Pair(r.l_quantity, Pair(r.l_extendedprice, Pair(r.l_discount, 1)))))))),
		(s1: Rep[(Double, (Double, (Double, (Double, (Double, (Double, (Double, Int)))))))], s2: Rep[(Double, (Double, (Double, (Double, (Double, (Double, (Double, Int)))))))]) => (s1._1 + s2._1,s1._2 + s2._2,s1._3 + s2._3,s1._4 + s2._4,s1._5 + s2._5,s1._6 + s2._6,s1._7 + s2._7,s1._8 + s2._8)).toArray.map(r => Pair(r.head._1, Pair(r.head._2, Pair(r.tail._1, Pair(r.tail._2, Pair(r.tail._3, Pair(r.tail._4, Pair((r.tail._5.toDouble / r.tail._8.toDouble), Pair((r.tail._6.toDouble / r.tail._8.toDouble), Pair((r.tail._7.toDouble / r.tail._8.toDouble), r.tail._8)))))))))))
	.orderBy(r => Pair(r._1, r._2))

def Q2(region: Rep[Table[Region]], part: Rep[Table[Part]], supplier: Rep[Table[Supplier]], nation: Rep[Table[Nation]], partsupp: Rep[Table[Partsupp]]) = part
	.where(r => ((r.p_size === toRep(43)) && r.p_type.startsWith("TIN")))
	.join(partsupp)(r => r.p_partkey, r => r.ps_partkey)
	.join(supplier)(r => r.tail.ps_suppkey, r => r.s_suppkey)
	.join(nation)(r => r.tail.s_nationkey, r => r.n_nationkey)
	.join(region)(r => r.tail.n_regionkey, r => r.r_regionkey)
	.where(r => ((r.tail.r_name === toRep("AFRICA")) && (r.head.head.head.tail.ps_supplycost === ({ val result = partsupp
		.where(r2 => (r.head.head.head.head.p_partkey === r2.ps_partkey))
		.join(supplier)(r2 => r2.ps_partkey, r2 => r2.s_suppkey)
		.join(nation)(r2 => r2.tail.s_nationkey, r2 => r2.n_nationkey)
		.join(region)(r2 => r2.tail.n_regionkey, r2 => r2.r_regionkey)
		.where(r2 => (r2.tail.r_name === toRep("AFRICA"))) ; (result.min(r1 => r1.head.head.head.ps_supplycost)) }))))
	.select(r => Pair(r.head.head.tail.s_acctbal, Pair(r.head.head.tail.s_name, Pair(r.head.tail.n_name, Pair(r.head.head.head.head.p_partkey, Pair(r.head.head.head.head.p_mfgr, Pair(r.head.head.tail.s_address, Pair(r.head.head.tail.s_phone, r.head.head.tail.s_comment))))))))
	.orderBy(r => Pair(r._1, Pair(r._3, Pair(r._2, r._4))))

def Q3(customer: Rep[Table[Customer]], orders: Rep[Table[Orders]], lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(customer
	.where(r1 => (r1.c_mktsegment === toRep("HOUSEHOLD")))
	.join(orders)(r1 => r1.c_custkey, r1 => r1.o_custkey)
	.where(r1 => (r1.tail.o_orderdate < toRep(19950304)))
	.join(lineitem)(r1 => r1.tail.o_orderkey, r1 => r1.l_orderkey)
	.where(r1 => (r1.tail.l_shipdate > toRep(19950304)))
	.mapReduce(r => Pair(Pair(r.tail.l_orderkey, Pair(r.head.tail.o_orderdate, r.head.tail.o_shippriority)), (r.tail.l_extendedprice * (toRep(1.0) - r.tail.l_discount))),
		(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r => Pair(r.head._1, Pair(r.tail, Pair(r.head._2, r.head._3)))))
	.orderBy(r => Pair(-r._2, r._3))

def Q4(orders: Rep[Table[Orders]], lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(orders
	.where(r1 => ((r1.o_orderdate >= toRep(19930801)) && (r1.o_orderdate < toRep(19931101))))
	.join(lineitem)(r1 => r1.o_orderkey, r1 => r1.l_orderkey)
	.where(r1 => (r1.tail.l_commitdate < r1.tail.l_receiptdate))
	.mapReduce(r => Pair(r.head.o_orderpriority, 1),
		(s1: Rep[Int], s2: Rep[Int]) => (s1 + s2)).toArray.map(r => Pair(r.head, r.tail)))
	.orderBy(r => r._1)

def Q5(region: Rep[Table[Region]], customer: Rep[Table[Customer]], lineitem: Rep[Table[Lineitem]], supplier: Rep[Table[Supplier]], nation: Rep[Table[Nation]], orders: Rep[Table[Orders]]) = ReadOnlyTable(customer
	.join(orders)(r1 => r1.c_custkey, r1 => r1.o_custkey)
	.where(r1 => (r1.tail.o_orderdate < toRep(19970101)))
	.join(lineitem)(r1 => r1.tail.o_orderkey, r1 => r1.l_orderkey)
	.join(supplier)(r1 => r1.tail.l_suppkey, r1 => r1.s_suppkey)
	.join(nation)(r1 => r1.head.head.head.c_nationkey, r1 => r1.n_nationkey)
	.where(r1 => (r1.head.head.head.tail.o_orderdate >= toRep(19960101)))
	.where(r1 => (r1.head.head.head.head.c_nationkey === r1.head.tail.s_nationkey))
	.join(region)(r1 => r1.tail.n_regionkey, r1 => r1.r_regionkey)
	.where(r1 => (r1.tail.r_name === toRep("ASIA")))
	.mapReduce(r => Pair(r.head.tail.n_name, (r.head.head.head.tail.l_extendedprice * (toRep(1.0) - r.head.head.head.tail.l_discount))),
		(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r => Pair(r.head, r.tail)))
	.orderBy(r => -r._2)

def Q6(lineitem: Rep[Table[Lineitem]]) = { val result = lineitem
	.where(r1 => ((((r1.l_shipdate >= toRep(19960101)) && (r1.l_shipdate <= toRep(19970101))) && ((r1.l_discount >= toRep(0.08)) && (r1.l_discount <= toRep(0.1)))) && (r1.l_quantity < toRep(24.0)))) ; (result.sum(r => (r.l_extendedprice * r.l_discount))) }

def Q7(customer: Rep[Table[Customer]], lineitem: Rep[Table[Lineitem]], supplier: Rep[Table[Supplier]], nation: Rep[Table[Nation]], orders: Rep[Table[Orders]]) = ReadOnlyTable(supplier
		.join(lineitem)(r1 => r1.s_suppkey, r1 => r1.l_suppkey)
		.where(r1 => ((r1.tail.l_shipdate >= toRep(19950101)) && (r1.tail.l_shipdate <= toRep(19961231))))
		.join(orders)(r1 => r1.tail.l_orderkey, r1 => r1.o_orderkey)
		.join(customer)(r1 => r1.tail.o_custkey, r1 => r1.c_custkey)
		.join(nation)(r1 => r1.head.head.head.s_nationkey, r1 => r1.n_nationkey)
		.join(nation)(r1 => r1.head.tail.c_nationkey, r1 => r1.n_nationkey)
		.where(r1 => (((r1.head.tail.n_name === toRep("UNITED STATES")) && (r1.tail.n_name === toRep("INDONESIA"))) || ((r1.head.tail.n_name === toRep("INDONESIA")) && (r1.tail.n_name === toRep("UNITED STATES")))))
		.select(r1 => Pair(r1.head.tail.n_name, Pair(r1.tail.n_name, Pair((r1.head.head.head.head.tail.l_shipdate/!toRep(10000)), (r1.head.head.head.head.tail.l_extendedprice * (toRep(1.0) - r1.head.head.head.head.tail.l_discount))))))
	.mapReduce(r => Pair(Pair(r._1, Pair(r._2, r._3)), r._4),
		(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r => Pair(r.head._1, Pair(r.head._2, Pair(r.head._3, r.tail)))))
	.orderBy(r => Pair(r._1, Pair(r._2, r._3)))

def Q8(region: Rep[Table[Region]], customer: Rep[Table[Customer]], lineitem: Rep[Table[Lineitem]], part: Rep[Table[Part]], supplier: Rep[Table[Supplier]], nation: Rep[Table[Nation]], orders: Rep[Table[Orders]]) = ReadOnlyTable(part
			.where(r1 => (r1.p_type === toRep("MEDIUM ANODIZED NICKEL")))
			.join(lineitem)(r1 => r1.p_partkey, r1 => r1.l_partkey)
			.join(supplier)(r1 => r1.tail.l_suppkey, r1 => r1.s_suppkey)
			.join(orders)(r1 => r1.head.tail.l_orderkey, r1 => r1.o_orderkey)
			.join(customer)(r1 => r1.tail.o_custkey, r1 => r1.c_custkey)
			.join(nation)(r1 => r1.tail.c_nationkey, r1 => r1.n_nationkey)
			.join(nation)(r1 => r1.head.head.head.tail.s_nationkey, r1 => r1.n_nationkey)
			.where(r1 => ((r1.head.head.head.tail.o_orderdate >= toRep(19950101)) && (r1.head.head.head.tail.o_orderdate <= toRep(19961231))))
			.join(region)(r1 => r1.head.tail.n_regionkey, r1 => r1.r_regionkey)
			.where(r1 => (r1.tail.r_name === toRep("ASIA")))
			.select(r1 => Pair((r1.head.head.head.head.tail.o_orderdate/!toRep(10000)), Pair((r1.head.head.head.head.head.head.tail.l_extendedprice * (toRep(1.0) - r1.head.head.head.head.head.head.tail.l_discount)), r1.head.tail.n_name)))
		.mapReduce(r => Pair(r._1, Pair(IF ((r._3 === toRep("INDONESIA"))) THEN (r._2) ELSE (toRep(0)), r._2)),
			(s1: Rep[(Double, Double)], s2: Rep[(Double, Double)]) => (s1._1 + s2._1,s1._2 + s2._2)).toArray.map(r => Pair(r.head, Pair(r.tail._1, r.tail._2))))
	.select(r => Pair(r._1, (r._2/r._3)))
	.orderBy(r => r._1)

def Q9(lineitem: Rep[Table[Lineitem]], part: Rep[Table[Part]], supplier: Rep[Table[Supplier]], nation: Rep[Table[Nation]], orders: Rep[Table[Orders]], partsupp: Rep[Table[Partsupp]]) = ReadOnlyTable(lineitem
		.join(supplier)(r1 => r1.l_suppkey, r1 => r1.s_suppkey)
		.join(part)(r1 => r1.head.l_partkey, r1 => r1.p_partkey)
		.where(r1 => r1.tail.p_name.contains("ghost"))
		.join(partsupp)(r1 => Pair(r1.head.head.l_partkey, r1.head.head.l_suppkey), r1 => Pair(r1.ps_partkey, r1.ps_suppkey))
		.join(orders)(r1 => r1.head.head.head.l_orderkey, r1 => r1.o_orderkey)
		.join(nation)(r1 => r1.head.head.head.tail.s_nationkey, r1 => r1.n_nationkey)
		.select(r1 => Pair(r1.tail.n_name, Pair((r1.head.tail.o_orderdate/!toRep(10000)), ((r1.head.head.head.head.head.l_extendedprice * (toRep(1.0) - r1.head.head.head.head.head.l_discount)) - (r1.head.head.tail.ps_supplycost * r1.head.head.head.head.head.l_quantity)))))
	.mapReduce(r => Pair(Pair(r._1, r._2), r._3),
		(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r => Pair(r.head._1, Pair(r.head._2, r.tail))))
	.orderBy(r => Pair(r._1, -r._2))

def Q10(orders: Rep[Table[Orders]], customer: Rep[Table[Customer]], lineitem: Rep[Table[Lineitem]], nation: Rep[Table[Nation]]) = ReadOnlyTable(orders
	.where(r1 => ((r1.o_orderdate >= toRep(19941101)) && (r1.o_orderdate < toRep(19950201))))
	.join(customer)(r1 => r1.o_custkey, r1 => r1.c_custkey)
	.join(lineitem)(r1 => r1.head.o_orderkey, r1 => r1.l_orderkey)
	.where(r1 => (r1.tail.l_returnflag.toStr === toRep("R")))
	.join(nation)(r1 => r1.head.tail.c_nationkey, r1 => r1.n_nationkey)
	.mapReduce(r => Pair(Pair(r.head.head.tail.c_custkey, Pair(r.head.head.tail.c_name, Pair(r.head.head.tail.c_acctbal, Pair(r.head.head.tail.c_phone, Pair(r.tail.n_name, Pair(r.head.head.tail.c_address, r.head.head.tail.c_comment)))))), (r.head.tail.l_extendedprice * (toRep(1.0) - r.head.tail.l_discount))),
		(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r => Pair(r.head._1, Pair(r.head._2, Pair(r.tail, Pair(r.head._3, Pair(r.head._5, Pair(r.head._6, Pair(r.head._4, r.head._7)))))))))
	.orderBy(r => -r._3)

def Q11(partsupp: Rep[Table[Partsupp]], supplier: Rep[Table[Supplier]], nation: Rep[Table[Nation]]) = ReadOnlyTable(partsupp
	.join(supplier)(r1 => r1.ps_suppkey, r1 => r1.s_suppkey)
	.join(nation)(r1 => r1.tail.s_nationkey, r1 => r1.n_nationkey)
	.where(r1 => (r1.tail.n_name === toRep("UNITED KINGDOM")))
	.mapReduce(r => Pair(r.head.head.ps_partkey, (r.head.head.ps_supplycost * r.head.head.ps_availqty.toDouble)),
		(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r => Pair(r.head, r.tail)))
	.where(r => (r._2 > (toRep(1.0E-4) * ({ val result = partsupp
		.join(supplier)(r2 => r2.ps_suppkey, r2 => r2.s_suppkey)
		.join(nation)(r2 => r2.tail.s_nationkey, r2 => r2.n_nationkey)
		.where(r2 => (r2.tail.n_name === toRep("UNITED KINGDOM"))) ; (result.sum(r1 => (r1.head.head.ps_supplycost * r1.head.head.ps_availqty.toDouble))) }))))
	.orderBy(r => -r._2)

def Q12(orders: Rep[Table[Orders]], lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(orders
	.join(lineitem)(r1 => r1.o_orderkey, r1 => r1.l_orderkey)
	.where(r1 => (((((r1.tail.l_shipmode === toRep("MAIL") || r1.tail.l_shipmode === toRep("SHIP")) && (r1.tail.l_commitdate < r1.tail.l_receiptdate)) && (r1.tail.l_shipdate < r1.tail.l_commitdate)) && (r1.tail.l_receiptdate >= toRep(19940101))) && (r1.tail.l_receiptdate < toRep(19950101))))
	.mapReduce(r => Pair(r.tail.l_shipmode, Pair(IF (((r.head.o_orderpriority === toRep("1-URGENT")) || (r.head.o_orderpriority === toRep("2-HIGH")))) THEN (toRep(1)) ELSE (toRep(0)), IF (((r.head.o_orderpriority !== toRep("1-URGENT")) && (r.head.o_orderpriority !== toRep("2-HIGH")))) THEN (toRep(1)) ELSE (toRep(0)))),
		(s1: Rep[(Int, Int)], s2: Rep[(Int, Int)]) => (s1._1 + s2._1,s1._2 + s2._2)).toArray.map(r => Pair(r.head, Pair(r.tail._1, r.tail._2))))
	.orderBy(r => r._1)

def Q13(customer: Rep[Table[Customer]], orders: Rep[Table[Orders]]) = ReadOnlyTable(ReadOnlyTable(customer
		.join(orders)(r2 => r2.c_custkey, r2 => r2.o_custkey)
		.where(r2 => !r2.tail.o_comment.matches(".*unusual.*packages.*"))
		.mapReduce(r1 => Pair(r1.head.c_custkey, 1),
			(s1: Rep[Int], s2: Rep[Int]) => (s1 + s2)).toArray.map(r1 => Pair(r1.head, r1.tail)))
	.mapReduce(r => Pair(r._2, 1),
		(s1: Rep[Int], s2: Rep[Int]) => (s1 + s2)).toArray.map(r => Pair(r.head, r.tail)))
	.orderBy(r => Pair(-r._2, -r._1))

def Q14(lineitem: Rep[Table[Lineitem]], part: Rep[Table[Part]]) = { val result = lineitem
	.where(r1 => ((r1.l_shipdate >= toRep(19940301)) && (r1.l_shipdate < toRep(19940401))))
	.join(part)(r1 => r1.l_partkey, r1 => r1.p_partkey) ; (((toRep(100.0) * result.sum(r => IF (r.tail.p_type.endsWith("PROMO")) THEN ((r.head.l_extendedprice * (toRep(1.0) - r.head.l_discount))) ELSE (toRep(0))))/result.sum(r => (r.head.l_extendedprice * (toRep(1.0) - r.head.l_discount))))) }

def Q15(supplier: Rep[Table[Supplier]], lineitem: Rep[Table[Lineitem]]) = supplier
	.join(ReadOnlyTable(lineitem
		.where(r1 => ((r1.l_shipdate >= toRep(19930901)) && (r1.l_shipdate < toRep(19931201))))
		.mapReduce(r => Pair(r.l_suppkey, (r.l_extendedprice * (toRep(1.0) - r.l_discount))),
			(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r => Pair(r.head, r.tail))))(r => r.s_suppkey, r => r._1)
	.where(r => (r.tail._2 === ({ val result = ReadOnlyTable(lineitem
			.where(r3 => ((r3.l_shipdate >= toRep(19930901)) && (r3.l_shipdate < toRep(19931201))))
			.mapReduce(r2 => Pair(r2.l_suppkey, (r2.l_extendedprice * (toRep(1.0) - r2.l_discount))),
				(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r2 => Pair(r2.head, r2.tail))) ; (result.max(r1 => r1._2)) })))
	.select(r => Pair(r.head.s_suppkey, Pair(r.head.s_name, Pair(r.head.s_address, Pair(r.head.s_phone, r.tail._2)))))
	.orderBy(r => r._1)

def Q16(partsupp: Rep[Table[Partsupp]], part: Rep[Table[Part]], supplier: Rep[Table[Supplier]]) = ReadOnlyTable(partsupp
	.where(r1 => !(supplier
		.where(r2 => r2.s_comment.matches(".*Customer.*Complaints.*"))
		.select(r2 => r2.s_suppkey).where(e => e == r1.ps_suppkey).count !== 0))
	.join(part)(r1 => r1.ps_partkey, r1 => r1.p_partkey)
	.where(r1 => (((r1.tail.p_brand !== toRep("Brand#21")) && !r1.tail.p_type.endsWith("PROMO PLATED")) && (r1.tail.p_size === toRep(23) || r1.tail.p_size === toRep(3) || r1.tail.p_size === toRep(33) || r1.tail.p_size === toRep(29) || r1.tail.p_size === toRep(40) || r1.tail.p_size === toRep(27) || r1.tail.p_size === toRep(22) || r1.tail.p_size === toRep(4))))
	.mapReduce(r => Pair(Pair(r.tail.p_brand, Pair(r.tail.p_type, r.tail.p_size)), 1),
		(s1: Rep[Int], s2: Rep[Int]) => (s1 + s2)).toArray.map(r => Pair(r.head._1, Pair(r.head._2, Pair(r.head._3, r.tail)))))
	.orderBy(r => Pair(-r._4, Pair(r._1, Pair(r._2, r._3))))

def Q17(lineitem: Rep[Table[Lineitem]], part: Rep[Table[Part]]) = { val result = lineitem
	.join(part)(r1 => r1.l_partkey, r1 => r1.p_partkey)
	.where(r1 => (((r1.tail.p_brand === toRep("Brand#15")) && (r1.tail.p_container === toRep("MED BAG"))) && (r1.head.l_quantity < (toRep(0.2) * ({ val result = lineitem
		.where(r3 => (r3.l_partkey === r1.tail.p_partkey)) ; (result.avg(r2 => r2.l_quantity)) }))))) ; ((result.sum(r => r.head.l_extendedprice)/toRep(7.0))) }

def Q18(customer: Rep[Table[Customer]], orders: Rep[Table[Orders]], lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(customer
	.join(orders)(r1 => r1.c_custkey, r1 => r1.o_custkey)
	.join(lineitem)(r1 => r1.tail.o_orderkey, r1 => r1.l_orderkey)
	.where(r1 => (ReadOnlyTable(lineitem
			.mapReduce(r2 => Pair(r2.l_orderkey, r2.l_quantity),
				(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r2 => Pair(r2.head, r2.tail)))
			.where(r2 => (r2._2 > toRep(300.0)))
		.select(r2 => r2._1).where(e => e == r1.head.tail.o_orderkey).count !== 0))
	.mapReduce(r => Pair(Pair(r.head.head.c_name, Pair(r.head.head.c_custkey, Pair(r.head.tail.o_orderkey, Pair(r.head.tail.o_orderdate, r.head.tail.o_totalprice)))), r.tail.l_quantity),
		(s1: Rep[Double], s2: Rep[Double]) => (s1 + s2)).toArray.map(r => Pair(r.head._1, Pair(r.head._2, Pair(r.head._3, Pair(r.head._4, Pair(r.head._5, r.tail)))))))
	.orderBy(r => Pair(-r._5, r._4))

def Q19(lineitem: Rep[Table[Lineitem]], part: Rep[Table[Part]]) = { val result = lineitem
	.join(part)(r1 => r1.l_partkey, r1 => r1.p_partkey)
	.where(r1 => (((((((((r1.tail.p_brand === toRep("Brand#31")) && (r1.tail.p_container === toRep("SM CASE") || r1.tail.p_container === toRep("SM BOX") || r1.tail.p_container === toRep("SM PACK") || r1.tail.p_container === toRep("SM PKG"))) && (r1.head.l_quantity >= toRep(26.0))) && (r1.head.l_quantity <= toRep(36.0))) && ((r1.tail.p_size >= toRep(1)) && (r1.tail.p_size <= toRep(5)))) && (r1.head.l_shipmode === toRep("AIR") || r1.head.l_shipmode === toRep("AIR REG"))) && (r1.head.l_shipinstruct === toRep("DELIVER IN PERSON"))) || (((((((r1.tail.p_brand === toRep("Brand#43")) && (r1.tail.p_container === toRep("MED BAG") || r1.tail.p_container === toRep("MED BOX") || r1.tail.p_container === toRep("MED PKG") || r1.tail.p_container === toRep("MED PACK"))) && (r1.head.l_quantity >= toRep(15.0))) && (r1.head.l_quantity <= toRep(25.0))) && ((r1.tail.p_size >= toRep(1)) && (r1.tail.p_size <= toRep(10)))) && (r1.head.l_shipmode === toRep("AIR") || r1.head.l_shipmode === toRep("AIR REG"))) && (r1.head.l_shipinstruct === toRep("DELIVER IN PERSON")))) || (((((((r1.tail.p_brand === toRep("Brand#43")) && (r1.tail.p_container === toRep("LG CASE") || r1.tail.p_container === toRep("LG BOX") || r1.tail.p_container === toRep("LG PACK") || r1.tail.p_container === toRep("LG PKG"))) && (r1.head.l_quantity >= toRep(4.0))) && (r1.head.l_quantity <= toRep(14.0))) && ((r1.tail.p_size >= toRep(1)) && (r1.tail.p_size <= toRep(15)))) && (r1.head.l_shipmode === toRep("AIR") || r1.head.l_shipmode === toRep("AIR REG"))) && (r1.head.l_shipinstruct === toRep("DELIVER IN PERSON"))))) ; (result.sum(r => (r.head.l_extendedprice * (toRep(1.0) - r.head.l_discount)))) }

def Q20(lineitem: Rep[Table[Lineitem]], part: Rep[Table[Part]], supplier: Rep[Table[Supplier]], nation: Rep[Table[Nation]], partsupp: Rep[Table[Partsupp]]) = supplier
	.where(r => (partsupp
		.where(r1 => ((part
			.where(r2 => r2.p_name.endsWith("azure"))
			.select(r2 => r2.p_partkey).where(e => e == r1.ps_partkey).count !== 0) && (r1.ps_availqty.toDouble > (toRep(0.5) * ({ val result = lineitem
			.where(r3 => ((((r3.l_partkey === r1.ps_partkey) && (r3.l_suppkey === r.s_suppkey)) && (r3.l_shipdate >= toRep(19960101))) && (r3.l_shipdate < toRep(19970101)))) ; (result.sum(r2 => r2.l_quantity)) })))))
		.select(r1 => r1.ps_suppkey).where(e => e == r.s_suppkey).count !== 0))
	.join(nation)(r => r.s_nationkey, r => r.n_nationkey)
	.where(r => (r.tail.n_name === toRep("JORDAN")))
	.select(r => Pair(r.head.s_name, r.head.s_address))
	.orderBy(r => r._1)

def Q21(supplier: Rep[Table[Supplier]], lineitem: Rep[Table[Lineitem]], orders: Rep[Table[Orders]], nation: Rep[Table[Nation]]) = ReadOnlyTable(supplier
	.join(lineitem)(r1 => r1.s_suppkey, r1 => r1.l_suppkey)
	.where(r1 => !((lineitem
		.where(r2 => (((r2.l_orderkey === r1.tail.l_orderkey) && (r2.l_suppkey !== r1.tail.l_suppkey)) && (r2.l_receiptdate > r2.l_commitdate)))).count !== 0))
	.where(r1 => ((lineitem
		.where(r2 => ((r2.l_orderkey === r1.tail.l_orderkey) && (r2.l_suppkey !== r1.tail.l_suppkey)))).count !== 0))
	.where(r1 => (r1.tail.l_receiptdate > r1.tail.l_commitdate))
	.join(orders)(r1 => r1.tail.l_orderkey, r1 => r1.o_orderkey)
	.where(r1 => (r1.tail.o_orderstatus.toStr === toRep("F")))
	.join(nation)(r1 => r1.head.head.s_nationkey, r1 => r1.n_nationkey)
	.where(r1 => (r1.tail.n_name === toRep("MOROCCO")))
	.mapReduce(r => Pair(r.head.head.head.s_name, 1),
		(s1: Rep[Int], s2: Rep[Int]) => (s1 + s2)).toArray.map(r => Pair(r.head, r.tail)))
	.orderBy(r => Pair(-r._2, r._1))

def Q22(customer: Rep[Table[Customer]], orders: Rep[Table[Orders]]) = ReadOnlyTable(customer
		.where(r1 => (((r1.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I1]") || r1.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I2]") || r1.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I3]") || r1.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I4]") || r1.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I5]") || r1.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I6]") || r1.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I7]")) && (r1.c_acctbal > ({ val result = customer
			.where(r3 => ((r3.c_acctbal > toRep(0.0)) && (r3.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I1]") || r3.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I2]") || r3.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I3]") || r3.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I4]") || r3.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I5]") || r3.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I6]") || r3.c_phone.substring(toRep(1), toRep(1) + toRep(2)) === toRep("[I7]")))) ; (result.avg(r2 => r2.c_acctbal)) }))) && !((orders
			.where(r2 => (r2.o_custkey === r1.c_custkey))).count !== 0)))
		.select(r1 => Pair(r1.c_phone.substring(toRep(1), toRep(1) + toRep(2)), r1.c_acctbal))
	.mapReduce(r => Pair(r._1, Pair(1, r._2)),
		(s1: Rep[(Int, Double)], s2: Rep[(Int, Double)]) => (s1._1 + s2._1,s1._2 + s2._2)).toArray.map(r => Pair(r.head, Pair(r.tail._1, r.tail._2))))
	.orderBy(r => r._1)

}