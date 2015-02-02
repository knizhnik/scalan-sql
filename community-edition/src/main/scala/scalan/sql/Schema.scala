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


type Orders = (Int, (Int, (Char, (Double, (Int, (String, (String, (Int, String))))))))

def createOrders(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".o_orderkey"), PairTable.create(Table.create[Int](tableName + ".o_custkey"), PairTable.create(Table.create[Char](tableName + ".O_ORDERSTATUS"), PairTable.create(Table.create[Double](tableName + ".o_totalprice"), PairTable.create(Table.create[Int](tableName + ".o_orderdate"), PairTable.create(Table.create[String](tableName + ".o_orderpriority"), PairTable.create(Table.create[String](tableName + ".o_clerk"), PairTable.create(Table.create[Int](tableName + ".o_shippriority"), Table.create[String](tableName + ".o_comment")))))))))

def parseOrders(c: Arr[String]): Rep[Orders] = Pair(c(0).toInt, Pair(c(1).toInt, Pair(c(2).toChar, Pair(c(3).toDouble, Pair(c(4).toInt, Pair(c(5), Pair(c(6), Pair(c(7).toInt, c(8)))))))))


type Customer = (Int, (String, (String, (Int, (String, (Double, (String, String)))))))

def createCustomer(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".c_custkey"), PairTable.create(Table.create[String](tableName + ".c_name"), PairTable.create(Table.create[String](tableName + ".c_address"), PairTable.create(Table.create[Int](tableName + ".c_nationkey"), PairTable.create(Table.create[String](tableName + ".c_phone"), PairTable.create(Table.create[Double](tableName + ".c_acctbal"), PairTable.create(Table.create[String](tableName + ".c_mktsegment"), Table.create[String](tableName + ".c_comment"))))))))

def parseCustomer(c: Arr[String]): Rep[Customer] = Pair(c(0).toInt, Pair(c(1), Pair(c(2), Pair(c(3).toInt, Pair(c(4), Pair(c(5).toDouble, Pair(c(6), c(7))))))))


type Supplier = (Int, (String, (String, (Int, (String, (Double, String))))))

def createSupplier(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".s_suppkey"), PairTable.create(Table.create[String](tableName + ".s_name"), PairTable.create(Table.create[String](tableName + ".s_address"), PairTable.create(Table.create[Int](tableName + ".s_nationkey"), PairTable.create(Table.create[String](tableName + ".s_phone"), PairTable.create(Table.create[Double](tableName + ".s_acctbal"), Table.create[String](tableName + ".s_comment")))))))

def parseSupplier(c: Arr[String]): Rep[Supplier] = Pair(c(0).toInt, Pair(c(1), Pair(c(2), Pair(c(3).toInt, Pair(c(4), Pair(c(5).toDouble, c(6)))))))


type Partsupp = (Int, (Int, (Int, (Double, String))))

def createPartsupp(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".ps_partkey"), PairTable.create(Table.create[Int](tableName + ".ps_suppkey"), PairTable.create(Table.create[Int](tableName + ".ps_availqty"), PairTable.create(Table.create[Double](tableName + ".ps_supplycost"), Table.create[String](tableName + ".ps_comment")))))

def parsePartsupp(c: Arr[String]): Rep[Partsupp] = Pair(c(0).toInt, Pair(c(1).toInt, Pair(c(2).toInt, Pair(c(3).toDouble, c(4)))))


type Region = (Int, (String, String))

def createRegion(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".r_regionkey"), PairTable.create(Table.create[String](tableName + ".r_name"), Table.create[String](tableName + ".r_comment")))

def parseRegion(c: Arr[String]): Rep[Region] = Pair(c(0).toInt, Pair(c(1), c(2)))


type Nation = (Int, (String, (Int, String)))

def createNation(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".n_nationkey"), PairTable.create(Table.create[String](tableName + ".n_name"), PairTable.create(Table.create[Int](tableName + ".n_regionkey"), Table.create[String](tableName + ".n_comment"))))

def parseNation(c: Arr[String]): Rep[Nation] = Pair(c(0).toInt, Pair(c(1), Pair(c(2).toInt, c(3))))


type Part = (Int, (String, (String, (String, (String, (Int, (String, (Double, String))))))))

def createPart(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + ".p_partkey"), PairTable.create(Table.create[String](tableName + ".p_name"), PairTable.create(Table.create[String](tableName + ".p_mfgr"), PairTable.create(Table.create[String](tableName + ".p_brand"), PairTable.create(Table.create[String](tableName + ".p_type"), PairTable.create(Table.create[Int](tableName + ".p_size"), PairTable.create(Table.create[String](tableName + ".p_container"), PairTable.create(Table.create[Double](tableName + ".p_retailprice"), Table.create[String](tableName + ".p_comment")))))))))

def parsePart(c: Arr[String]): Rep[Part] = Pair(c(0).toInt, Pair(c(1), Pair(c(2), Pair(c(3), Pair(c(4), Pair(c(5).toInt, Pair(c(6), Pair(c(7).toDouble, c(8)))))))))


def lineitem_pk(r: Rep[Lineitem]) = Pair(r.head, r.tail.tail.tail.head)

def lineitem_order_fk(r: Rep[Lineitem]) = r.head

def lineitem_supp_fk(r: Rep[Lineitem]) = r.tail.tail.head

def lineitem_part_fk(r: Rep[Lineitem]) = r.tail.head

def lineitem_ps_fk(r: Rep[Lineitem]) = Pair(r.tail.head, r.tail.tail.head)

def part_pk(r: Rep[Part]) = r.head

def supplier_pk(r: Rep[Supplier]) = r.head

def partsupp_pk(r: Rep[Partsupp]) = Pair(r.head, r.tail.head)

def partsupp_supp_fk(r: Rep[Partsupp]) = r.tail.head

def partsupp_part_fk(r: Rep[Partsupp]) = r.head

def customer_pk(r: Rep[Customer]) = r.head

def customer_nation_fk(r: Rep[Customer]) = r.tail.tail.tail.head

def orders_pk(r: Rep[Orders]) = r.head

def orders_cust_fk(r: Rep[Orders]) = r.tail.head

def nation_pk(r: Rep[Nation]) = r.head

def nation_region_fk(r: Rep[Nation]) = r.tail.tail.head

def region_pk(r: Rep[Region]) = r.head

def Q1(lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(lineitem.where(r => r.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head <= toRep(19981201)).mapReduce(r => Pair(Pair(r.tail.tail.tail.tail.tail.tail.tail.tail.head, r.tail.tail.tail.tail.tail.tail.tail.tail.tail.head), Pair(r.tail.tail.tail.tail.head, Pair(r.tail.tail.tail.tail.tail.head, Pair((r.tail.tail.tail.tail.tail.head * (toRep(1).toDouble - r.tail.tail.tail.tail.tail.tail.head)), Pair(((r.tail.tail.tail.tail.tail.head * (toRep(1).toDouble - r.tail.tail.tail.tail.tail.tail.head)) * (toRep(1).toDouble + r.tail.tail.tail.tail.tail.tail.tail.head)), Pair(r.tail.tail.tail.tail.head, Pair(r.tail.tail.tail.tail.tail.head, Pair(r.tail.tail.tail.tail.tail.tail.head, 1)))))))),
 (s1: Rep[(Double, (Double, (Double, (Double, (Double, (Double, (Double, Int)))))))], s2: Rep[(Double, (Double, (Double, (Double, (Double, (Double, (Double, Int)))))))]) => (s1._1 + s2._1,s1._2 + s2._2,s1._3 + s2._3,s1._4 + s2._4,s1._5 + s2._5,s1._6 + s2._6,s1._7 + s2._7,s1._8 + s2._8)).toArray.map(r => Pair(r.head.head, Pair(r.head.tail, Pair(r.tail.head, Pair(r.tail.tail.head, Pair(r.tail.tail.tail.head, Pair(r.tail.tail.tail.tail.head, Pair((r.tail.tail.tail.tail.tail.head.toDouble / r.tail.tail.tail.tail.tail.tail.tail.tail.toDouble), Pair((r.tail.tail.tail.tail.tail.tail.head.toDouble / r.tail.tail.tail.tail.tail.tail.tail.tail.toDouble), Pair((r.tail.tail.tail.tail.tail.tail.tail.head.toDouble / r.tail.tail.tail.tail.tail.tail.tail.tail.toDouble), r.tail.tail.tail.tail.tail.tail.tail.tail))))))))))).orderBy(r => Pair(r.head, r.tail.head))

def Q2(customer: Rep[Table[Customer]], orders: Rep[Table[Orders]], lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(customer.join(orders)(r => r.head, r => r.tail.head).join(lineitem)(r => r.tail.head, r => r.head).where(r => r.head.head.tail.tail.tail.tail.tail.tail.head === toRep("HOUSEHOLD") && r.head.tail.tail.tail.tail.tail.head < toRep(19950304) && r.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head > toRep(19950304)).mapReduce(r => Pair(Pair(r.tail.head, Pair(r.head.tail.tail.tail.tail.tail.head, r.head.tail.tail.tail.tail.tail.tail.tail.tail.head)), (r.tail.tail.tail.tail.tail.tail.head * (toRep(1).toDouble - r.tail.tail.tail.tail.tail.tail.tail.head))),
 (s1: Rep[Double], s2: Rep[Double]) => (s1._1 + s2._1)).toArray.map(r => Pair(r.head.head, Pair(r.tail, Pair(r.head.tail.head, r.head.tail.tail))))).orderBy(r => Pair(r.tail.head, r.tail.tail.head))

def Q3(orders: Rep[Table[Orders]], lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(orders.join(lineitem)(r => r.head, r => r.head).where(r => r.head.tail.tail.tail.tail.head >= toRep(19931101) && r.head.tail.tail.tail.tail.head < toRep(19930801) && r.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head < r.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head).mapReduce(r => Pair(r.head.tail.tail.tail.tail.tail.head, 1),
 (s1: Rep[Int], s2: Rep[Int]) => (s1._1 + s2._1)).toArray.map(r => Pair(r.head, r.tail))).orderBy(r => r.head)

def Q4(customer: Rep[Table[Customer]], orders: Rep[Table[Orders]], lineitem: Rep[Table[Lineitem]], supplier: Rep[Table[Supplier]], nation: Rep[Table[Nation]], region: Rep[Table[Region]]) = ReadOnlyTable(customer.join(orders)(r => r.head, r => r.tail.head).join(lineitem)(r => r.tail.head, r => r.head).join(supplier)(r => r.tail.tail.tail.head, r => r.head).join(nation)(r => r.head.head.head.tail.tail.tail.head, r => r.head).join(region)(r => r.tail.tail.tail.head, r => r.head).where(r => r.head.head.head.head.head.tail.tail.tail.head === r.head.head.tail.tail.tail.tail.head && r.tail.tail.head === toRep("ASIA") && r.head.head.head.head.tail.tail.tail.tail.tail.head >= toRep(19960101) && r.head.head.head.head.tail.tail.tail.tail.tail.head < toRep(19970101)).mapReduce(r => Pair(r.head.tail.tail.head, (r.head.head.head.tail.tail.tail.tail.tail.tail.head * (toRep(1).toDouble - r.head.head.head.tail.tail.tail.tail.tail.tail.tail.head))),
 (s1: Rep[Double], s2: Rep[Double]) => (s1._1 + s2._1)).toArray.map(r => Pair(r.head, r.tail))).orderBy(r => r.tail)

def Q5(lineitem: Rep[Table[Lineitem]]) = lineitem.where(r => r.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head >= toRep(19960101) && r.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head <= toRep(19970101) && r.tail.tail.tail.tail.tail.tail.head >= toRep(0.08) && r.tail.tail.tail.tail.tail.tail.head <= toRep(0.1) && r.tail.tail.tail.tail.head < toRep(24).toDouble)..toArray

def Q6(supplier: Rep[Table[Supplier]], lineitem: Rep[Table[Lineitem]], orders: Rep[Table[Orders]], customer: Rep[Table[Customer]], n1: Rep[Table[Nation]], n2: Rep[Table[Nation]]) = supplier.join(lineitem)(r => r.head, r => r.tail.tail.head).join(orders)(r => r.tail.head, r => r.head).join(customer)(r => r.tail.tail.head, r => r.head).join(n1)(r => r.head.head.head.tail.tail.tail.head, r => r.head).join(n2)(r => r.head.tail.tail.tail.tail.head, r => r.head).where(r => (r.head.tail.tail.head === toRep("UNITED STATES") && r.tail.tail.head === toRep("INDONESIA") || r.head.tail.tail.head === toRep("INDONESIA") && r.tail.tail.head === toRep("UNITED STATES")) && r.head.head.head.head.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head >= toRep(19950101) && r.head.head.head.head.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head <= toRep(19961231)).select(r => Pair(r.head.tail.tail.head, Pair(r.tail.tail.head, Pair((r.head.head.head.head.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head/!toRep(10000)), (r.head.head.head.head.tail.tail.tail.tail.tail.tail.head * (toRep(1).toDouble - r.head.head.head.head.tail.tail.tail.tail.tail.tail.tail.head)))))).toArray

def Q7(supplier: Rep[Table[Supplier]], lineitem: Rep[Table[Lineitem]], orders: Rep[Table[Orders]], customer: Rep[Table[Customer]], n1: Rep[Table[Nation]], n2: Rep[Table[Nation]]) = ReadOnlyTable(shipping.mapReduce(r => Pair(Pair(r.head, Pair(r.tail.head, r.tail.tail.head)), r.tail.tail.tail),
 (s1: Rep[Double], s2: Rep[Double]) => (s1._1 + s2._1)).toArray.map(r => Pair(r.head.head, Pair(r.head.tail.head, Pair(r.head.tail.tail, r.tail))))).orderBy(r => Pair(r.head, Pair(r.tail.head, r.tail.tail.head)))

def Q8(lineitem: Rep[Table[Lineitem]], supplier: Rep[Table[Supplier]], part: Rep[Table[Part]], partsupp: Rep[Table[Partsupp]], orders: Rep[Table[Orders]], nation: Rep[Table[Nation]]) = ReadOnlyTable(profit.mapReduce(r => Pair(Pair(r.head, r.tail.head), r.tail.tail),
 (s1: Rep[Double], s2: Rep[Double]) => (s1._1 + s2._1)).toArray.map(r => Pair(r.head.head, Pair(r.head.tail, r.tail)))).orderBy(r => Pair(r.head, r.tail.head))

}