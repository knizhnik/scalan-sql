package scalan.sql

import scalan._
import scalan.common.Default


trait Tables extends Base {
  self: TablesDsl =>

  trait TableRecord extends Reifiable[TableRecord] {
  }

  trait TableRecordCompanion {
  }

  // Supplier-Order-Detail tables
  abstract class Detail(val id: Rep[Int], val desc: Rep[String], val weight: Rep[Double]) extends TableRecord

  trait DetailCompanion extends ConcreteClass0[Detail] with TableRecordCompanion {
    def defaultOf = Default.defaultVal(Detail(0, "", 0.0))

    def create(id: Rep[Int], desc: Rep[String], weight: Rep[Double]) = Detail(id, desc, weight)
  }


  abstract class Supplier(val id:Rep[Int], val company: Rep[String], val address: Rep[String]) extends TableRecord

  trait SupplierCompanion extends ConcreteClass0[Supplier] with TableRecordCompanion {
    def defaultOf = Default.defaultVal(Supplier(0, "", ""))

    def create(id: Rep[Int], company: Rep[String], address: Rep[String]) = Supplier(id, company, address)
  }


  abstract class Order(val detail: Rep[Int], val supplier: Rep[Int], val amount: Rep[Int], val price: Rep[Double], val delivery: Rep[Int]) extends TableRecord

  trait OrderCompanion extends ConcreteClass0[Order] with TableRecordCompanion {
    def defaultOf = Default.defaultVal(Order(0, 0, 0, 0.0, 0))

    def create(detail: Rep[Int], supplier: Rep[Int], amount: Rep[Int], price: Rep[Double], delivery: Rep[Int]) = Order(detail, supplier, amount, price, delivery)
  }

  // TPC-H tables
  abstract class LINEITEM(
                           val l_orderkey: Rep[Int],
                           val l_partkey: Rep[Int],
                           val l_suppkey: Rep[Int],
                           val l_linenumber: Rep[Int],
                           val l_quantity: Rep[Double],
                           val l_extendedprice: Rep[Double],
                           val l_discount: Rep[Double],
                           val l_tax: Rep[Double],
                           val l_returnflag: Rep[Char],
                           val l_linestatus: Rep[Char],
                           val l_shipdate: Rep[Int],
                           val l_commitdate: Rep[Int],
                           val l_receiptdate: Rep[Int],
                           val l_shipinstruct: Rep[String],
                           val l_shipmode: Rep[String],
                           val l_comment: Rep[String]) extends TableRecord

  abstract class ORDERS(
                         val o_orderkey: Rep[Int],
                         val o_custkey: Rep[Int],
                         val o_orderstatus: Rep[Char],
                         val o_totalprice: Rep[Double],
                         val o_orderdate: Rep[Int],
                         val o_orderpriority: Rep[String],
                         val o_clerk: Rep[String],
                         val o_shippriority: Rep[Int],
                         val o_comment: Rep[String]) extends TableRecord

  abstract class CUSTOMER(
                           val c_custkey: Rep[Int],
                           val c_name: Rep[String],
                           val c_address: Rep[String],
                           val c_nationkey: Rep[Int],
                           val c_phone: Rep[String],
                           val c_acctbal: Rep[Double],
                           val c_mktsegment: Rep[String],
                           val c_comment: Rep[String]) extends TableRecord

  abstract class SUPPLIER(
                           val s_suppkey: Rep[Int],
                           val s_name: Rep[String],
                           val s_address: Rep[String],
                           val s_nationkey: Rep[Int],
                           val s_phone: Rep[String],
                           val s_acctbal: Rep[Double],
                           val s_comment: Rep[String]) extends TableRecord

  abstract class PARTSUPP(
                           val ps_partkey: Rep[Int],
                           val ps_suppkey: Rep[Int],
                           val ps_availqty: Rep[Int],
                           val ps_supplycost: Rep[Double],
                           val ps_comment: Rep[String]) extends TableRecord

  abstract class REGION(
                         val r_regionkey: Rep[Int],
                         val r_name: Rep[String],
                         val r_comment: Rep[String]) extends TableRecord

  abstract class NATION(
                         val n_nationkey: Rep[Int],
                         val n_name: Rep[String],
                         val n_regionkey: Rep[Int],
                         val n_comment: Rep[String]) extends TableRecord

  abstract class PART(
                       val p_partkey: Rep[Int],
                       val p_name: Rep[String],
                       val p_mfgr: Rep[String],
                       val p_brand: Rep[String],
                       val p_type: Rep[String],
                       val p_size: Rep[Int],
                       val p_container: Rep[String],
                       val p_retailprice: Rep[Double],
                       val p_comment: Rep[String]) extends TableRecord

  def parseDate(d: Rep[String]): Rep[Int] = (d.substring(0, 4) + d.substring(5, 7) + d.substring(8, 10)).toInt

  trait LINEITEMCompanion extends ConcreteClass0[LINEITEM] with TableRecordCompanion
  {
    def defaultOf = Default.defaultVal(LINEITEM(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, '\0', '\0', 0, 0, 0, "", "", ""))
    def create(in: Arr[String]): Rep[LINEITEM] =
      LINEITEM(in(0).toInt, in(1).toInt, in(2).toInt, in(3).toInt, in(4).toDouble, in(5).toDouble, in(6).toDouble, in(7).toDouble, in(8)(0), in(9)(0), parseDate(in(10)), parseDate(in(11)), parseDate(in(12)), in(13), in(14), in(15))
  }

  trait ORDERSCompanion extends ConcreteClass0[ORDERS] with TableRecordCompanion
  {
    def defaultOf = Default.defaultVal(ORDERS(0, 0, '\0', 0.0, 0, "", "", 0, ""))
  }

  trait CUSTOMERCompanion extends ConcreteClass0[CUSTOMER] with TableRecordCompanion
  {
    def defaultOf = Default.defaultVal(CUSTOMER(0, "", "", 0, "", 0.0, "", ""))
  }

  trait SUPPLIERCompanion extends ConcreteClass0[SUPPLIER] with TableRecordCompanion
  {
    def defaultOf = Default.defaultVal(SUPPLIER(0, "", "", 0, "", 0.0, ""))
  }

  trait PARTSUPPCompanion extends ConcreteClass0[PARTSUPP] with TableRecordCompanion
  {
    def defaultOf = Default.defaultVal(PARTSUPP(0, 0, 0, 0.0, ""))
  }

  trait REGIONCompanion extends ConcreteClass0[REGION] with TableRecordCompanion
  {
    def defaultOf = Default.defaultVal(REGION(0, "", ""))
  }

  trait NATIONCompanion extends ConcreteClass0[NATION] with TableRecordCompanion
  {
    def defaultOf = Default.defaultVal(NATION(0, "", 0, ""))
  }

  trait PARTCompanion extends ConcreteClass0[PART] with TableRecordCompanion
  {
    def defaultOf = Default.defaultVal(PART(0, "", "", "", "", 0, "", 0.0, ""))
  }
}

trait TablesDsl extends ScalanDsl with impl.TablesAbs with Tables

trait TablesDslSeq extends TablesDsl with impl.TablesSeq with ScalanSeq

trait TablesDslExp extends TablesDsl with impl.TablesExp with ScalanExp
