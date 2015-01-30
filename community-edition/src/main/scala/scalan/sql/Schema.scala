package scalan.sql
import scalan._

trait Queries extends ScalanDsl with SqlDsl {

implicit class StringFormatter(str: Rep[String]) {
  def toDate: Rep[Int] = (str.substring(0, 4) + str.substring(5, 7) + str.substring(8, 10)).toInt
  def toChar: Rep[Char] = str(0)
}

type Lineitem = (Int, (Int, (Int, (Int, (Double, (Double, (Double, (Double, (Char, (Char, (Int, (Int, (Int, (String, (String, String)))))))))))))))

def createLineitem(tableName: Rep[String]) = PairTable.create(Table.create[Int](tableName + toRep(".l_orderkey")), PairTable.create(Table.create[Int](tableName + toRep(".l_partkey")), PairTable.create(Table.create[Int](tableName + toRep(".l_suppkey")), PairTable.create(Table.create[Int](tableName + toRep(".l_linenumber")), PairTable.create(Table.create[Double](tableName + toRep(".l_quantity")), PairTable.create(Table.create[Double](tableName + toRep(".l_extendedprice")), PairTable.create(Table.create[Double](tableName + toRep(".l_discount")), PairTable.create(Table.create[Double](tableName + toRep(".l_tax")), PairTable.create(Table.create[Char](tableName + toRep(".l_returnflag")), PairTable.create(Table.create[Char](tableName + toRep(".l_linestatus")), PairTable.create(Table.create[Int](tableName + toRep(".l_shipdate")), PairTable.create(Table.create[Int](tableName + toRep(".l_commitdate")), PairTable.create(Table.create[Int](tableName + toRep(".l_receiptdate")), PairTable.create(Table.create[String](tableName + toRep(".l_shipinstruct")), PairTable.create(Table.create[String](tableName + toRep(".l_shipmode")), Table.create[String](tableName + toRep(".l_comment")))))))))))))))))

def parseLineitem(c: Arr[String]): Rep[Lineitem] = Pair(c(0).toInt, Pair(c(1).toInt, Pair(c(2).toInt, Pair(c(3).toInt, Pair(c(4).toDouble, Pair(c(5).toDouble, Pair(c(6).toDouble, Pair(c(7).toDouble, Pair(c(8).toChar, Pair(c(9).toChar, Pair(c(10).toDate, Pair(c(11).toDate, Pair(c(12).toDate, Pair(c(13), Pair(c(14), c(15))))))))))))))))


def lineitem_pk(r: Rep[Lineitem]) = Pair(r.head, r.tail.tail.tail.head)

def Q1(lineitem: Rep[Table[Lineitem]]) = ReadOnlyTable(lineitem.where(r => r.tail.tail.tail.tail.tail.tail.tail.tail.tail.tail.head <= toRep(19981201)).mapReduce(r => Pair(Pair(r.tail.tail.tail.tail.tail.tail.tail.tail.head, r.tail.tail.tail.tail.tail.tail.tail.tail.tail.head), Pair(r.tail.tail.tail.tail.head, Pair(r.tail.tail.tail.tail.tail.head, Pair((r.tail.tail.tail.tail.tail.head * (toRep(1).toDouble - r.tail.tail.tail.tail.tail.tail.head)), Pair((r.tail.tail.tail.tail.tail.head * ((toRep(1).toDouble - r.tail.tail.tail.tail.tail.tail.head) * (toRep(1).toDouble + r.tail.tail.tail.tail.tail.tail.tail.head))), Pair(r.tail.tail.tail.tail.head, Pair(r.tail.tail.tail.tail.tail.head, Pair(r.tail.tail.tail.tail.tail.tail.head, 1)))))))),
 (s1: Rep[(Double, (Double, (Double, (Double, (Double, (Double, (Double, Int)))))))], s2: Rep[(Double, (Double, (Double, (Double, (Double, (Double, (Double, Int)))))))]) => (s1._1 + s2._1,s1._2 + s2._2,s1._3 + s2._3,s1._4 + s2._4,s1._5 + s2._5,s1._6 + s2._6,s1._7 + s2._7,s1._8 + s2._8)).toArray.map(r => Pair(r.head.head, Pair(r.head.tail, Pair(r.tail.head, Pair(r.tail.tail.head, Pair(r.tail.tail.tail.head, Pair(r.tail.tail.tail.tail.head, Pair((r.tail.tail.tail.tail.tail.head.toDouble / r.tail.tail.tail.tail.tail.tail.tail.tail.toDouble), Pair((r.tail.tail.tail.tail.tail.tail.head.toDouble / r.tail.tail.tail.tail.tail.tail.tail.tail.toDouble), Pair((r.tail.tail.tail.tail.tail.tail.tail.head.toDouble / r.tail.tail.tail.tail.tail.tail.tail.tail.toDouble), r.tail.tail.tail.tail.tail.tail.tail.tail))))))))))).orderBy(r => Pair(r.head, r.tail.head))

}