package scalan.sql
package impl

import scalan._
import scalan.collections._
import scalan.common.Default
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait TablesAbs extends Scalan with Tables
{ self: TablesDsl =>
  // single proxy for each type family
  implicit def proxyTableRecord(p: Rep[TableRecord]): TableRecord =
    proxyOps[TableRecord](p)



  abstract class TableRecordElem[From, To <: TableRecord](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait TableRecordCompanionElem extends CompanionElem[TableRecordCompanionAbs]
  implicit lazy val TableRecordCompanionElem: TableRecordCompanionElem = new TableRecordCompanionElem {
    lazy val tag = typeTag[TableRecordCompanionAbs]
    protected def getDefaultRep = TableRecord
  }

  abstract class TableRecordCompanionAbs extends CompanionBase[TableRecordCompanionAbs] with TableRecordCompanion {
    override def toString = "TableRecord"
    
  }
  def TableRecord: Rep[TableRecordCompanionAbs]
  implicit def proxyTableRecordCompanion(p: Rep[TableRecordCompanion]): TableRecordCompanion = {
    proxyOps[TableRecordCompanion](p)
  }

  //default wrapper implementation
  
  // elem for concrete class
  class DetailElem(iso: Iso[DetailData, Detail]) extends TableRecordElem[DetailData, Detail](iso)

  // state representation type
  type DetailData = (Int, (String, Double))

  // 3) Iso for concrete class
  class DetailIso
    extends Iso[DetailData, Detail] {
    override def from(p: Rep[Detail]) =
      unmkDetail(p) match {
        case Some((id, desc, weight)) => Pair(id, Pair(desc, weight))
        case None => !!!
      }
    override def to(p: Rep[(Int, (String, Double))]) = {
      val Pair(id, Pair(desc, weight)) = p
      Detail(id, desc, weight)
    }
    lazy val tag = {
      weakTypeTag[Detail]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[Detail]](Detail(0, "", 0.0))
    lazy val eTo = new DetailElem(this)
  }
  // 4) constructor and deconstructor
  abstract class DetailCompanionAbs extends CompanionBase[DetailCompanionAbs] with DetailCompanion {
    override def toString = "Detail"
    def apply(p: Rep[DetailData]): Rep[Detail] =
      isoDetail.to(p)
    def apply(id: Rep[Int], desc: Rep[String], weight: Rep[Double]): Rep[Detail] =
      mkDetail(id, desc, weight)
    def unapply(p: Rep[Detail]) = unmkDetail(p)
  }
  def Detail: Rep[DetailCompanionAbs]
  implicit def proxyDetailCompanion(p: Rep[DetailCompanionAbs]): DetailCompanionAbs = {
    proxyOps[DetailCompanionAbs](p)
  }

  class DetailCompanionElem extends CompanionElem[DetailCompanionAbs] {
    lazy val tag = typeTag[DetailCompanionAbs]
    protected def getDefaultRep = Detail
  }
  implicit lazy val DetailCompanionElem: DetailCompanionElem = new DetailCompanionElem

  implicit def proxyDetail(p: Rep[Detail]): Detail =
    proxyOps[Detail](p)

  implicit class ExtendedDetail(p: Rep[Detail]) {
    def toData: Rep[DetailData] = isoDetail.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoDetail: Iso[DetailData, Detail] =
    new DetailIso

  // 6) smart constructor and deconstructor
  def mkDetail(id: Rep[Int], desc: Rep[String], weight: Rep[Double]): Rep[Detail]
  def unmkDetail(p: Rep[Detail]): Option[(Rep[Int], Rep[String], Rep[Double])]

  //default wrapper implementation
  
  // elem for concrete class
  class SupplierElem(iso: Iso[SupplierData, Supplier]) extends TableRecordElem[SupplierData, Supplier](iso)

  // state representation type
  type SupplierData = (Int, (String, String))

  // 3) Iso for concrete class
  class SupplierIso
    extends Iso[SupplierData, Supplier] {
    override def from(p: Rep[Supplier]) =
      unmkSupplier(p) match {
        case Some((id, company, address)) => Pair(id, Pair(company, address))
        case None => !!!
      }
    override def to(p: Rep[(Int, (String, String))]) = {
      val Pair(id, Pair(company, address)) = p
      Supplier(id, company, address)
    }
    lazy val tag = {
      weakTypeTag[Supplier]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[Supplier]](Supplier(0, "", ""))
    lazy val eTo = new SupplierElem(this)
  }
  // 4) constructor and deconstructor
  abstract class SupplierCompanionAbs extends CompanionBase[SupplierCompanionAbs] with SupplierCompanion {
    override def toString = "Supplier"
    def apply(p: Rep[SupplierData]): Rep[Supplier] =
      isoSupplier.to(p)
    def apply(id: Rep[Int], company: Rep[String], address: Rep[String]): Rep[Supplier] =
      mkSupplier(id, company, address)
    def unapply(p: Rep[Supplier]) = unmkSupplier(p)
  }
  def Supplier: Rep[SupplierCompanionAbs]
  implicit def proxySupplierCompanion(p: Rep[SupplierCompanionAbs]): SupplierCompanionAbs = {
    proxyOps[SupplierCompanionAbs](p)
  }

  class SupplierCompanionElem extends CompanionElem[SupplierCompanionAbs] {
    lazy val tag = typeTag[SupplierCompanionAbs]
    protected def getDefaultRep = Supplier
  }
  implicit lazy val SupplierCompanionElem: SupplierCompanionElem = new SupplierCompanionElem

  implicit def proxySupplier(p: Rep[Supplier]): Supplier =
    proxyOps[Supplier](p)

  implicit class ExtendedSupplier(p: Rep[Supplier]) {
    def toData: Rep[SupplierData] = isoSupplier.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSupplier: Iso[SupplierData, Supplier] =
    new SupplierIso

  // 6) smart constructor and deconstructor
  def mkSupplier(id: Rep[Int], company: Rep[String], address: Rep[String]): Rep[Supplier]
  def unmkSupplier(p: Rep[Supplier]): Option[(Rep[Int], Rep[String], Rep[String])]

  //default wrapper implementation
  
  // elem for concrete class
  class OrderElem(iso: Iso[OrderData, Order]) extends TableRecordElem[OrderData, Order](iso)

  // state representation type
  type OrderData = (Int, (Int, (Int, (Double, Int))))

  // 3) Iso for concrete class
  class OrderIso
    extends Iso[OrderData, Order] {
    override def from(p: Rep[Order]) =
      unmkOrder(p) match {
        case Some((detail, supplier, amount, price, delivery)) => Pair(detail, Pair(supplier, Pair(amount, Pair(price, delivery))))
        case None => !!!
      }
    override def to(p: Rep[(Int, (Int, (Int, (Double, Int))))]) = {
      val Pair(detail, Pair(supplier, Pair(amount, Pair(price, delivery)))) = p
      Order(detail, supplier, amount, price, delivery)
    }
    lazy val tag = {
      weakTypeTag[Order]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[Order]](Order(0, 0, 0, 0.0, 0))
    lazy val eTo = new OrderElem(this)
  }
  // 4) constructor and deconstructor
  abstract class OrderCompanionAbs extends CompanionBase[OrderCompanionAbs] with OrderCompanion {
    override def toString = "Order"
    def apply(p: Rep[OrderData]): Rep[Order] =
      isoOrder.to(p)
    def apply(detail: Rep[Int], supplier: Rep[Int], amount: Rep[Int], price: Rep[Double], delivery: Rep[Int]): Rep[Order] =
      mkOrder(detail, supplier, amount, price, delivery)
    def unapply(p: Rep[Order]) = unmkOrder(p)
  }
  def Order: Rep[OrderCompanionAbs]
  implicit def proxyOrderCompanion(p: Rep[OrderCompanionAbs]): OrderCompanionAbs = {
    proxyOps[OrderCompanionAbs](p)
  }

  class OrderCompanionElem extends CompanionElem[OrderCompanionAbs] {
    lazy val tag = typeTag[OrderCompanionAbs]
    protected def getDefaultRep = Order
  }
  implicit lazy val OrderCompanionElem: OrderCompanionElem = new OrderCompanionElem

  implicit def proxyOrder(p: Rep[Order]): Order =
    proxyOps[Order](p)

  implicit class ExtendedOrder(p: Rep[Order]) {
    def toData: Rep[OrderData] = isoOrder.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoOrder: Iso[OrderData, Order] =
    new OrderIso

  // 6) smart constructor and deconstructor
  def mkOrder(detail: Rep[Int], supplier: Rep[Int], amount: Rep[Int], price: Rep[Double], delivery: Rep[Int]): Rep[Order]
  def unmkOrder(p: Rep[Order]): Option[(Rep[Int], Rep[Int], Rep[Int], Rep[Double], Rep[Int])]

  //default wrapper implementation
  
  // elem for concrete class
  class LINEITEMElem(iso: Iso[LINEITEMData, LINEITEM]) extends TableRecordElem[LINEITEMData, LINEITEM](iso)

  // state representation type
  type LINEITEMData = (Int, (Int, (Int, (Int, (Double, (Double, (Double, (Double, (Char, (Char, (Int, (Int, (Int, (String, (String, String)))))))))))))))

  // 3) Iso for concrete class
  class LINEITEMIso
    extends Iso[LINEITEMData, LINEITEM] {
    override def from(p: Rep[LINEITEM]) =
      unmkLINEITEM(p) match {
        case Some((l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)) => Pair(l_orderkey, Pair(l_partkey, Pair(l_suppkey, Pair(l_linenumber, Pair(l_quantity, Pair(l_extendedprice, Pair(l_discount, Pair(l_tax, Pair(l_returnflag, Pair(l_linestatus, Pair(l_shipdate, Pair(l_commitdate, Pair(l_receiptdate, Pair(l_shipinstruct, Pair(l_shipmode, l_comment)))))))))))))))
        case None => !!!
      }
    override def to(p: Rep[(Int, (Int, (Int, (Int, (Double, (Double, (Double, (Double, (Char, (Char, (Int, (Int, (Int, (String, (String, String)))))))))))))))]) = {
      val Pair(l_orderkey, Pair(l_partkey, Pair(l_suppkey, Pair(l_linenumber, Pair(l_quantity, Pair(l_extendedprice, Pair(l_discount, Pair(l_tax, Pair(l_returnflag, Pair(l_linestatus, Pair(l_shipdate, Pair(l_commitdate, Pair(l_receiptdate, Pair(l_shipinstruct, Pair(l_shipmode, l_comment))))))))))))))) = p
      LINEITEM(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
    }
    lazy val tag = {
      weakTypeTag[LINEITEM]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[LINEITEM]](LINEITEM(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, element[Char].defaultRepValue, element[Char].defaultRepValue, 0, 0, 0, "", "", ""))
    lazy val eTo = new LINEITEMElem(this)
  }
  // 4) constructor and deconstructor
  abstract class LINEITEMCompanionAbs extends CompanionBase[LINEITEMCompanionAbs] with LINEITEMCompanion {
    override def toString = "LINEITEM"
    def apply(p: Rep[LINEITEMData]): Rep[LINEITEM] =
      isoLINEITEM.to(p)
    def apply(l_orderkey: Rep[Int], l_partkey: Rep[Int], l_suppkey: Rep[Int], l_linenumber: Rep[Int], l_quantity: Rep[Double], l_extendedprice: Rep[Double], l_discount: Rep[Double], l_tax: Rep[Double], l_returnflag: Rep[Char], l_linestatus: Rep[Char], l_shipdate: Rep[Int], l_commitdate: Rep[Int], l_receiptdate: Rep[Int], l_shipinstruct: Rep[String], l_shipmode: Rep[String], l_comment: Rep[String]): Rep[LINEITEM] =
      mkLINEITEM(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
    def unapply(p: Rep[LINEITEM]) = unmkLINEITEM(p)
  }
  def LINEITEM: Rep[LINEITEMCompanionAbs]
  implicit def proxyLINEITEMCompanion(p: Rep[LINEITEMCompanionAbs]): LINEITEMCompanionAbs = {
    proxyOps[LINEITEMCompanionAbs](p)
  }

  class LINEITEMCompanionElem extends CompanionElem[LINEITEMCompanionAbs] {
    lazy val tag = typeTag[LINEITEMCompanionAbs]
    protected def getDefaultRep = LINEITEM
  }
  implicit lazy val LINEITEMCompanionElem: LINEITEMCompanionElem = new LINEITEMCompanionElem

  implicit def proxyLINEITEM(p: Rep[LINEITEM]): LINEITEM =
    proxyOps[LINEITEM](p)

  implicit class ExtendedLINEITEM(p: Rep[LINEITEM]) {
    def toData: Rep[LINEITEMData] = isoLINEITEM.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoLINEITEM: Iso[LINEITEMData, LINEITEM] =
    new LINEITEMIso

  // 6) smart constructor and deconstructor
  def mkLINEITEM(l_orderkey: Rep[Int], l_partkey: Rep[Int], l_suppkey: Rep[Int], l_linenumber: Rep[Int], l_quantity: Rep[Double], l_extendedprice: Rep[Double], l_discount: Rep[Double], l_tax: Rep[Double], l_returnflag: Rep[Char], l_linestatus: Rep[Char], l_shipdate: Rep[Int], l_commitdate: Rep[Int], l_receiptdate: Rep[Int], l_shipinstruct: Rep[String], l_shipmode: Rep[String], l_comment: Rep[String]): Rep[LINEITEM]
  def unmkLINEITEM(p: Rep[LINEITEM]): Option[(Rep[Int], Rep[Int], Rep[Int], Rep[Int], Rep[Double], Rep[Double], Rep[Double], Rep[Double], Rep[Char], Rep[Char], Rep[Int], Rep[Int], Rep[Int], Rep[String], Rep[String], Rep[String])]

  //default wrapper implementation
  
  // elem for concrete class
  class ORDERSElem(iso: Iso[ORDERSData, ORDERS]) extends TableRecordElem[ORDERSData, ORDERS](iso)

  // state representation type
  type ORDERSData = (Int, (Int, (Char, (Double, (Int, (String, (String, (Int, String))))))))

  // 3) Iso for concrete class
  class ORDERSIso
    extends Iso[ORDERSData, ORDERS] {
    override def from(p: Rep[ORDERS]) =
      unmkORDERS(p) match {
        case Some((o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)) => Pair(o_orderkey, Pair(o_custkey, Pair(o_orderstatus, Pair(o_totalprice, Pair(o_orderdate, Pair(o_orderpriority, Pair(o_clerk, Pair(o_shippriority, o_comment))))))))
        case None => !!!
      }
    override def to(p: Rep[(Int, (Int, (Char, (Double, (Int, (String, (String, (Int, String))))))))]) = {
      val Pair(o_orderkey, Pair(o_custkey, Pair(o_orderstatus, Pair(o_totalprice, Pair(o_orderdate, Pair(o_orderpriority, Pair(o_clerk, Pair(o_shippriority, o_comment)))))))) = p
      ORDERS(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
    }
    lazy val tag = {
      weakTypeTag[ORDERS]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[ORDERS]](ORDERS(0, 0, element[Char].defaultRepValue, 0.0, 0, "", "", 0, ""))
    lazy val eTo = new ORDERSElem(this)
  }
  // 4) constructor and deconstructor
  abstract class ORDERSCompanionAbs extends CompanionBase[ORDERSCompanionAbs] with ORDERSCompanion {
    override def toString = "ORDERS"
    def apply(p: Rep[ORDERSData]): Rep[ORDERS] =
      isoORDERS.to(p)
    def apply(o_orderkey: Rep[Int], o_custkey: Rep[Int], o_orderstatus: Rep[Char], o_totalprice: Rep[Double], o_orderdate: Rep[Int], o_orderpriority: Rep[String], o_clerk: Rep[String], o_shippriority: Rep[Int], o_comment: Rep[String]): Rep[ORDERS] =
      mkORDERS(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
    def unapply(p: Rep[ORDERS]) = unmkORDERS(p)
  }
  def ORDERS: Rep[ORDERSCompanionAbs]
  implicit def proxyORDERSCompanion(p: Rep[ORDERSCompanionAbs]): ORDERSCompanionAbs = {
    proxyOps[ORDERSCompanionAbs](p)
  }

  class ORDERSCompanionElem extends CompanionElem[ORDERSCompanionAbs] {
    lazy val tag = typeTag[ORDERSCompanionAbs]
    protected def getDefaultRep = ORDERS
  }
  implicit lazy val ORDERSCompanionElem: ORDERSCompanionElem = new ORDERSCompanionElem

  implicit def proxyORDERS(p: Rep[ORDERS]): ORDERS =
    proxyOps[ORDERS](p)

  implicit class ExtendedORDERS(p: Rep[ORDERS]) {
    def toData: Rep[ORDERSData] = isoORDERS.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoORDERS: Iso[ORDERSData, ORDERS] =
    new ORDERSIso

  // 6) smart constructor and deconstructor
  def mkORDERS(o_orderkey: Rep[Int], o_custkey: Rep[Int], o_orderstatus: Rep[Char], o_totalprice: Rep[Double], o_orderdate: Rep[Int], o_orderpriority: Rep[String], o_clerk: Rep[String], o_shippriority: Rep[Int], o_comment: Rep[String]): Rep[ORDERS]
  def unmkORDERS(p: Rep[ORDERS]): Option[(Rep[Int], Rep[Int], Rep[Char], Rep[Double], Rep[Int], Rep[String], Rep[String], Rep[Int], Rep[String])]

  //default wrapper implementation
  
  // elem for concrete class
  class CUSTOMERElem(iso: Iso[CUSTOMERData, CUSTOMER]) extends TableRecordElem[CUSTOMERData, CUSTOMER](iso)

  // state representation type
  type CUSTOMERData = (Int, (String, (String, (Int, (String, (Double, (String, String)))))))

  // 3) Iso for concrete class
  class CUSTOMERIso
    extends Iso[CUSTOMERData, CUSTOMER] {
    override def from(p: Rep[CUSTOMER]) =
      unmkCUSTOMER(p) match {
        case Some((c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)) => Pair(c_custkey, Pair(c_name, Pair(c_address, Pair(c_nationkey, Pair(c_phone, Pair(c_acctbal, Pair(c_mktsegment, c_comment)))))))
        case None => !!!
      }
    override def to(p: Rep[(Int, (String, (String, (Int, (String, (Double, (String, String)))))))]) = {
      val Pair(c_custkey, Pair(c_name, Pair(c_address, Pair(c_nationkey, Pair(c_phone, Pair(c_acctbal, Pair(c_mktsegment, c_comment))))))) = p
      CUSTOMER(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
    }
    lazy val tag = {
      weakTypeTag[CUSTOMER]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[CUSTOMER]](CUSTOMER(0, "", "", 0, "", 0.0, "", ""))
    lazy val eTo = new CUSTOMERElem(this)
  }
  // 4) constructor and deconstructor
  abstract class CUSTOMERCompanionAbs extends CompanionBase[CUSTOMERCompanionAbs] with CUSTOMERCompanion {
    override def toString = "CUSTOMER"
    def apply(p: Rep[CUSTOMERData]): Rep[CUSTOMER] =
      isoCUSTOMER.to(p)
    def apply(c_custkey: Rep[Int], c_name: Rep[String], c_address: Rep[String], c_nationkey: Rep[Int], c_phone: Rep[String], c_acctbal: Rep[Double], c_mktsegment: Rep[String], c_comment: Rep[String]): Rep[CUSTOMER] =
      mkCUSTOMER(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
    def unapply(p: Rep[CUSTOMER]) = unmkCUSTOMER(p)
  }
  def CUSTOMER: Rep[CUSTOMERCompanionAbs]
  implicit def proxyCUSTOMERCompanion(p: Rep[CUSTOMERCompanionAbs]): CUSTOMERCompanionAbs = {
    proxyOps[CUSTOMERCompanionAbs](p)
  }

  class CUSTOMERCompanionElem extends CompanionElem[CUSTOMERCompanionAbs] {
    lazy val tag = typeTag[CUSTOMERCompanionAbs]
    protected def getDefaultRep = CUSTOMER
  }
  implicit lazy val CUSTOMERCompanionElem: CUSTOMERCompanionElem = new CUSTOMERCompanionElem

  implicit def proxyCUSTOMER(p: Rep[CUSTOMER]): CUSTOMER =
    proxyOps[CUSTOMER](p)

  implicit class ExtendedCUSTOMER(p: Rep[CUSTOMER]) {
    def toData: Rep[CUSTOMERData] = isoCUSTOMER.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoCUSTOMER: Iso[CUSTOMERData, CUSTOMER] =
    new CUSTOMERIso

  // 6) smart constructor and deconstructor
  def mkCUSTOMER(c_custkey: Rep[Int], c_name: Rep[String], c_address: Rep[String], c_nationkey: Rep[Int], c_phone: Rep[String], c_acctbal: Rep[Double], c_mktsegment: Rep[String], c_comment: Rep[String]): Rep[CUSTOMER]
  def unmkCUSTOMER(p: Rep[CUSTOMER]): Option[(Rep[Int], Rep[String], Rep[String], Rep[Int], Rep[String], Rep[Double], Rep[String], Rep[String])]

  //default wrapper implementation
  
  // elem for concrete class
  class SUPPLIERElem(iso: Iso[SUPPLIERData, SUPPLIER]) extends TableRecordElem[SUPPLIERData, SUPPLIER](iso)

  // state representation type
  type SUPPLIERData = (Int, (String, (String, (Int, (String, (Double, String))))))

  // 3) Iso for concrete class
  class SUPPLIERIso
    extends Iso[SUPPLIERData, SUPPLIER] {
    override def from(p: Rep[SUPPLIER]) =
      unmkSUPPLIER(p) match {
        case Some((s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)) => Pair(s_suppkey, Pair(s_name, Pair(s_address, Pair(s_nationkey, Pair(s_phone, Pair(s_acctbal, s_comment))))))
        case None => !!!
      }
    override def to(p: Rep[(Int, (String, (String, (Int, (String, (Double, String))))))]) = {
      val Pair(s_suppkey, Pair(s_name, Pair(s_address, Pair(s_nationkey, Pair(s_phone, Pair(s_acctbal, s_comment)))))) = p
      SUPPLIER(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
    }
    lazy val tag = {
      weakTypeTag[SUPPLIER]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SUPPLIER]](SUPPLIER(0, "", "", 0, "", 0.0, ""))
    lazy val eTo = new SUPPLIERElem(this)
  }
  // 4) constructor and deconstructor
  abstract class SUPPLIERCompanionAbs extends CompanionBase[SUPPLIERCompanionAbs] with SUPPLIERCompanion {
    override def toString = "SUPPLIER"
    def apply(p: Rep[SUPPLIERData]): Rep[SUPPLIER] =
      isoSUPPLIER.to(p)
    def apply(s_suppkey: Rep[Int], s_name: Rep[String], s_address: Rep[String], s_nationkey: Rep[Int], s_phone: Rep[String], s_acctbal: Rep[Double], s_comment: Rep[String]): Rep[SUPPLIER] =
      mkSUPPLIER(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
    def unapply(p: Rep[SUPPLIER]) = unmkSUPPLIER(p)
  }
  def SUPPLIER: Rep[SUPPLIERCompanionAbs]
  implicit def proxySUPPLIERCompanion(p: Rep[SUPPLIERCompanionAbs]): SUPPLIERCompanionAbs = {
    proxyOps[SUPPLIERCompanionAbs](p)
  }

  class SUPPLIERCompanionElem extends CompanionElem[SUPPLIERCompanionAbs] {
    lazy val tag = typeTag[SUPPLIERCompanionAbs]
    protected def getDefaultRep = SUPPLIER
  }
  implicit lazy val SUPPLIERCompanionElem: SUPPLIERCompanionElem = new SUPPLIERCompanionElem

  implicit def proxySUPPLIER(p: Rep[SUPPLIER]): SUPPLIER =
    proxyOps[SUPPLIER](p)

  implicit class ExtendedSUPPLIER(p: Rep[SUPPLIER]) {
    def toData: Rep[SUPPLIERData] = isoSUPPLIER.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSUPPLIER: Iso[SUPPLIERData, SUPPLIER] =
    new SUPPLIERIso

  // 6) smart constructor and deconstructor
  def mkSUPPLIER(s_suppkey: Rep[Int], s_name: Rep[String], s_address: Rep[String], s_nationkey: Rep[Int], s_phone: Rep[String], s_acctbal: Rep[Double], s_comment: Rep[String]): Rep[SUPPLIER]
  def unmkSUPPLIER(p: Rep[SUPPLIER]): Option[(Rep[Int], Rep[String], Rep[String], Rep[Int], Rep[String], Rep[Double], Rep[String])]

  //default wrapper implementation
  
  // elem for concrete class
  class PARTSUPPElem(iso: Iso[PARTSUPPData, PARTSUPP]) extends TableRecordElem[PARTSUPPData, PARTSUPP](iso)

  // state representation type
  type PARTSUPPData = (Int, (Int, (Int, (Double, String))))

  // 3) Iso for concrete class
  class PARTSUPPIso
    extends Iso[PARTSUPPData, PARTSUPP] {
    override def from(p: Rep[PARTSUPP]) =
      unmkPARTSUPP(p) match {
        case Some((ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)) => Pair(ps_partkey, Pair(ps_suppkey, Pair(ps_availqty, Pair(ps_supplycost, ps_comment))))
        case None => !!!
      }
    override def to(p: Rep[(Int, (Int, (Int, (Double, String))))]) = {
      val Pair(ps_partkey, Pair(ps_suppkey, Pair(ps_availqty, Pair(ps_supplycost, ps_comment)))) = p
      PARTSUPP(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
    }
    lazy val tag = {
      weakTypeTag[PARTSUPP]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[PARTSUPP]](PARTSUPP(0, 0, 0, 0.0, ""))
    lazy val eTo = new PARTSUPPElem(this)
  }
  // 4) constructor and deconstructor
  abstract class PARTSUPPCompanionAbs extends CompanionBase[PARTSUPPCompanionAbs] with PARTSUPPCompanion {
    override def toString = "PARTSUPP"
    def apply(p: Rep[PARTSUPPData]): Rep[PARTSUPP] =
      isoPARTSUPP.to(p)
    def apply(ps_partkey: Rep[Int], ps_suppkey: Rep[Int], ps_availqty: Rep[Int], ps_supplycost: Rep[Double], ps_comment: Rep[String]): Rep[PARTSUPP] =
      mkPARTSUPP(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
    def unapply(p: Rep[PARTSUPP]) = unmkPARTSUPP(p)
  }
  def PARTSUPP: Rep[PARTSUPPCompanionAbs]
  implicit def proxyPARTSUPPCompanion(p: Rep[PARTSUPPCompanionAbs]): PARTSUPPCompanionAbs = {
    proxyOps[PARTSUPPCompanionAbs](p)
  }

  class PARTSUPPCompanionElem extends CompanionElem[PARTSUPPCompanionAbs] {
    lazy val tag = typeTag[PARTSUPPCompanionAbs]
    protected def getDefaultRep = PARTSUPP
  }
  implicit lazy val PARTSUPPCompanionElem: PARTSUPPCompanionElem = new PARTSUPPCompanionElem

  implicit def proxyPARTSUPP(p: Rep[PARTSUPP]): PARTSUPP =
    proxyOps[PARTSUPP](p)

  implicit class ExtendedPARTSUPP(p: Rep[PARTSUPP]) {
    def toData: Rep[PARTSUPPData] = isoPARTSUPP.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoPARTSUPP: Iso[PARTSUPPData, PARTSUPP] =
    new PARTSUPPIso

  // 6) smart constructor and deconstructor
  def mkPARTSUPP(ps_partkey: Rep[Int], ps_suppkey: Rep[Int], ps_availqty: Rep[Int], ps_supplycost: Rep[Double], ps_comment: Rep[String]): Rep[PARTSUPP]
  def unmkPARTSUPP(p: Rep[PARTSUPP]): Option[(Rep[Int], Rep[Int], Rep[Int], Rep[Double], Rep[String])]

  //default wrapper implementation
  
  // elem for concrete class
  class REGIONElem(iso: Iso[REGIONData, REGION]) extends TableRecordElem[REGIONData, REGION](iso)

  // state representation type
  type REGIONData = (Int, (String, String))

  // 3) Iso for concrete class
  class REGIONIso
    extends Iso[REGIONData, REGION] {
    override def from(p: Rep[REGION]) =
      unmkREGION(p) match {
        case Some((r_regionkey, r_name, r_comment)) => Pair(r_regionkey, Pair(r_name, r_comment))
        case None => !!!
      }
    override def to(p: Rep[(Int, (String, String))]) = {
      val Pair(r_regionkey, Pair(r_name, r_comment)) = p
      REGION(r_regionkey, r_name, r_comment)
    }
    lazy val tag = {
      weakTypeTag[REGION]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[REGION]](REGION(0, "", ""))
    lazy val eTo = new REGIONElem(this)
  }
  // 4) constructor and deconstructor
  abstract class REGIONCompanionAbs extends CompanionBase[REGIONCompanionAbs] with REGIONCompanion {
    override def toString = "REGION"
    def apply(p: Rep[REGIONData]): Rep[REGION] =
      isoREGION.to(p)
    def apply(r_regionkey: Rep[Int], r_name: Rep[String], r_comment: Rep[String]): Rep[REGION] =
      mkREGION(r_regionkey, r_name, r_comment)
    def unapply(p: Rep[REGION]) = unmkREGION(p)
  }
  def REGION: Rep[REGIONCompanionAbs]
  implicit def proxyREGIONCompanion(p: Rep[REGIONCompanionAbs]): REGIONCompanionAbs = {
    proxyOps[REGIONCompanionAbs](p)
  }

  class REGIONCompanionElem extends CompanionElem[REGIONCompanionAbs] {
    lazy val tag = typeTag[REGIONCompanionAbs]
    protected def getDefaultRep = REGION
  }
  implicit lazy val REGIONCompanionElem: REGIONCompanionElem = new REGIONCompanionElem

  implicit def proxyREGION(p: Rep[REGION]): REGION =
    proxyOps[REGION](p)

  implicit class ExtendedREGION(p: Rep[REGION]) {
    def toData: Rep[REGIONData] = isoREGION.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoREGION: Iso[REGIONData, REGION] =
    new REGIONIso

  // 6) smart constructor and deconstructor
  def mkREGION(r_regionkey: Rep[Int], r_name: Rep[String], r_comment: Rep[String]): Rep[REGION]
  def unmkREGION(p: Rep[REGION]): Option[(Rep[Int], Rep[String], Rep[String])]

  //default wrapper implementation
  
  // elem for concrete class
  class NATIONElem(iso: Iso[NATIONData, NATION]) extends TableRecordElem[NATIONData, NATION](iso)

  // state representation type
  type NATIONData = (Int, (String, (Int, String)))

  // 3) Iso for concrete class
  class NATIONIso
    extends Iso[NATIONData, NATION] {
    override def from(p: Rep[NATION]) =
      unmkNATION(p) match {
        case Some((n_nationkey, n_name, n_regionkey, n_comment)) => Pair(n_nationkey, Pair(n_name, Pair(n_regionkey, n_comment)))
        case None => !!!
      }
    override def to(p: Rep[(Int, (String, (Int, String)))]) = {
      val Pair(n_nationkey, Pair(n_name, Pair(n_regionkey, n_comment))) = p
      NATION(n_nationkey, n_name, n_regionkey, n_comment)
    }
    lazy val tag = {
      weakTypeTag[NATION]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[NATION]](NATION(0, "", 0, ""))
    lazy val eTo = new NATIONElem(this)
  }
  // 4) constructor and deconstructor
  abstract class NATIONCompanionAbs extends CompanionBase[NATIONCompanionAbs] with NATIONCompanion {
    override def toString = "NATION"
    def apply(p: Rep[NATIONData]): Rep[NATION] =
      isoNATION.to(p)
    def apply(n_nationkey: Rep[Int], n_name: Rep[String], n_regionkey: Rep[Int], n_comment: Rep[String]): Rep[NATION] =
      mkNATION(n_nationkey, n_name, n_regionkey, n_comment)
    def unapply(p: Rep[NATION]) = unmkNATION(p)
  }
  def NATION: Rep[NATIONCompanionAbs]
  implicit def proxyNATIONCompanion(p: Rep[NATIONCompanionAbs]): NATIONCompanionAbs = {
    proxyOps[NATIONCompanionAbs](p)
  }

  class NATIONCompanionElem extends CompanionElem[NATIONCompanionAbs] {
    lazy val tag = typeTag[NATIONCompanionAbs]
    protected def getDefaultRep = NATION
  }
  implicit lazy val NATIONCompanionElem: NATIONCompanionElem = new NATIONCompanionElem

  implicit def proxyNATION(p: Rep[NATION]): NATION =
    proxyOps[NATION](p)

  implicit class ExtendedNATION(p: Rep[NATION]) {
    def toData: Rep[NATIONData] = isoNATION.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoNATION: Iso[NATIONData, NATION] =
    new NATIONIso

  // 6) smart constructor and deconstructor
  def mkNATION(n_nationkey: Rep[Int], n_name: Rep[String], n_regionkey: Rep[Int], n_comment: Rep[String]): Rep[NATION]
  def unmkNATION(p: Rep[NATION]): Option[(Rep[Int], Rep[String], Rep[Int], Rep[String])]

  //default wrapper implementation
  
  // elem for concrete class
  class PARTElem(iso: Iso[PARTData, PART]) extends TableRecordElem[PARTData, PART](iso)

  // state representation type
  type PARTData = (Int, (String, (String, (String, (String, (Int, (String, (Double, String))))))))

  // 3) Iso for concrete class
  class PARTIso
    extends Iso[PARTData, PART] {
    override def from(p: Rep[PART]) =
      unmkPART(p) match {
        case Some((p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)) => Pair(p_partkey, Pair(p_name, Pair(p_mfgr, Pair(p_brand, Pair(p_type, Pair(p_size, Pair(p_container, Pair(p_retailprice, p_comment))))))))
        case None => !!!
      }
    override def to(p: Rep[(Int, (String, (String, (String, (String, (Int, (String, (Double, String))))))))]) = {
      val Pair(p_partkey, Pair(p_name, Pair(p_mfgr, Pair(p_brand, Pair(p_type, Pair(p_size, Pair(p_container, Pair(p_retailprice, p_comment)))))))) = p
      PART(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
    }
    lazy val tag = {
      weakTypeTag[PART]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[PART]](PART(0, "", "", "", "", 0, "", 0.0, ""))
    lazy val eTo = new PARTElem(this)
  }
  // 4) constructor and deconstructor
  abstract class PARTCompanionAbs extends CompanionBase[PARTCompanionAbs] with PARTCompanion {
    override def toString = "PART"
    def apply(p: Rep[PARTData]): Rep[PART] =
      isoPART.to(p)
    def apply(p_partkey: Rep[Int], p_name: Rep[String], p_mfgr: Rep[String], p_brand: Rep[String], p_type: Rep[String], p_size: Rep[Int], p_container: Rep[String], p_retailprice: Rep[Double], p_comment: Rep[String]): Rep[PART] =
      mkPART(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
    def unapply(p: Rep[PART]) = unmkPART(p)
  }
  def PART: Rep[PARTCompanionAbs]
  implicit def proxyPARTCompanion(p: Rep[PARTCompanionAbs]): PARTCompanionAbs = {
    proxyOps[PARTCompanionAbs](p)
  }

  class PARTCompanionElem extends CompanionElem[PARTCompanionAbs] {
    lazy val tag = typeTag[PARTCompanionAbs]
    protected def getDefaultRep = PART
  }
  implicit lazy val PARTCompanionElem: PARTCompanionElem = new PARTCompanionElem

  implicit def proxyPART(p: Rep[PART]): PART =
    proxyOps[PART](p)

  implicit class ExtendedPART(p: Rep[PART]) {
    def toData: Rep[PARTData] = isoPART.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoPART: Iso[PARTData, PART] =
    new PARTIso

  // 6) smart constructor and deconstructor
  def mkPART(p_partkey: Rep[Int], p_name: Rep[String], p_mfgr: Rep[String], p_brand: Rep[String], p_type: Rep[String], p_size: Rep[Int], p_container: Rep[String], p_retailprice: Rep[Double], p_comment: Rep[String]): Rep[PART]
  def unmkPART(p: Rep[PART]): Option[(Rep[Int], Rep[String], Rep[String], Rep[String], Rep[String], Rep[Int], Rep[String], Rep[Double], Rep[String])]
}

// Seq -----------------------------------
trait TablesSeq extends TablesAbs with TablesDsl with ScalanSeq
{ self: TablesDslSeq =>
  lazy val TableRecord: Rep[TableRecordCompanionAbs] = new TableRecordCompanionAbs with UserTypeSeq[TableRecordCompanionAbs, TableRecordCompanionAbs] {
    lazy val selfType = element[TableRecordCompanionAbs]
    
  }

  

  

  case class SeqDetail
      (override val id: Rep[Int], override val desc: Rep[String], override val weight: Rep[Double])
      
    extends Detail(id, desc, weight)
        with UserTypeSeq[TableRecord, Detail] {
    lazy val selfType = element[Detail].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val Detail = new DetailCompanionAbs with UserTypeSeq[DetailCompanionAbs, DetailCompanionAbs] {
    lazy val selfType = element[DetailCompanionAbs]
  }

  def mkDetail
      (id: Rep[Int], desc: Rep[String], weight: Rep[Double]) =
      new SeqDetail(id, desc, weight)
  def unmkDetail(p: Rep[Detail]) =
    Some((p.id, p.desc, p.weight))

  case class SeqSupplier
      (override val id: Rep[Int], override val company: Rep[String], override val address: Rep[String])
      
    extends Supplier(id, company, address)
        with UserTypeSeq[TableRecord, Supplier] {
    lazy val selfType = element[Supplier].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val Supplier = new SupplierCompanionAbs with UserTypeSeq[SupplierCompanionAbs, SupplierCompanionAbs] {
    lazy val selfType = element[SupplierCompanionAbs]
  }

  def mkSupplier
      (id: Rep[Int], company: Rep[String], address: Rep[String]) =
      new SeqSupplier(id, company, address)
  def unmkSupplier(p: Rep[Supplier]) =
    Some((p.id, p.company, p.address))

  case class SeqOrder
      (override val detail: Rep[Int], override val supplier: Rep[Int], override val amount: Rep[Int], override val price: Rep[Double], override val delivery: Rep[Int])
      
    extends Order(detail, supplier, amount, price, delivery)
        with UserTypeSeq[TableRecord, Order] {
    lazy val selfType = element[Order].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val Order = new OrderCompanionAbs with UserTypeSeq[OrderCompanionAbs, OrderCompanionAbs] {
    lazy val selfType = element[OrderCompanionAbs]
  }

  def mkOrder
      (detail: Rep[Int], supplier: Rep[Int], amount: Rep[Int], price: Rep[Double], delivery: Rep[Int]) =
      new SeqOrder(detail, supplier, amount, price, delivery)
  def unmkOrder(p: Rep[Order]) =
    Some((p.detail, p.supplier, p.amount, p.price, p.delivery))

  case class SeqLINEITEM
      (override val l_orderkey: Rep[Int], override val l_partkey: Rep[Int], override val l_suppkey: Rep[Int], override val l_linenumber: Rep[Int], override val l_quantity: Rep[Double], override val l_extendedprice: Rep[Double], override val l_discount: Rep[Double], override val l_tax: Rep[Double], override val l_returnflag: Rep[Char], override val l_linestatus: Rep[Char], override val l_shipdate: Rep[Int], override val l_commitdate: Rep[Int], override val l_receiptdate: Rep[Int], override val l_shipinstruct: Rep[String], override val l_shipmode: Rep[String], override val l_comment: Rep[String])
      
    extends LINEITEM(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
        with UserTypeSeq[TableRecord, LINEITEM] {
    lazy val selfType = element[LINEITEM].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val LINEITEM = new LINEITEMCompanionAbs with UserTypeSeq[LINEITEMCompanionAbs, LINEITEMCompanionAbs] {
    lazy val selfType = element[LINEITEMCompanionAbs]
  }

  def mkLINEITEM
      (l_orderkey: Rep[Int], l_partkey: Rep[Int], l_suppkey: Rep[Int], l_linenumber: Rep[Int], l_quantity: Rep[Double], l_extendedprice: Rep[Double], l_discount: Rep[Double], l_tax: Rep[Double], l_returnflag: Rep[Char], l_linestatus: Rep[Char], l_shipdate: Rep[Int], l_commitdate: Rep[Int], l_receiptdate: Rep[Int], l_shipinstruct: Rep[String], l_shipmode: Rep[String], l_comment: Rep[String]) =
      new SeqLINEITEM(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
  def unmkLINEITEM(p: Rep[LINEITEM]) =
    Some((p.l_orderkey, p.l_partkey, p.l_suppkey, p.l_linenumber, p.l_quantity, p.l_extendedprice, p.l_discount, p.l_tax, p.l_returnflag, p.l_linestatus, p.l_shipdate, p.l_commitdate, p.l_receiptdate, p.l_shipinstruct, p.l_shipmode, p.l_comment))

  case class SeqORDERS
      (override val o_orderkey: Rep[Int], override val o_custkey: Rep[Int], override val o_orderstatus: Rep[Char], override val o_totalprice: Rep[Double], override val o_orderdate: Rep[Int], override val o_orderpriority: Rep[String], override val o_clerk: Rep[String], override val o_shippriority: Rep[Int], override val o_comment: Rep[String])
      
    extends ORDERS(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
        with UserTypeSeq[TableRecord, ORDERS] {
    lazy val selfType = element[ORDERS].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val ORDERS = new ORDERSCompanionAbs with UserTypeSeq[ORDERSCompanionAbs, ORDERSCompanionAbs] {
    lazy val selfType = element[ORDERSCompanionAbs]
  }

  def mkORDERS
      (o_orderkey: Rep[Int], o_custkey: Rep[Int], o_orderstatus: Rep[Char], o_totalprice: Rep[Double], o_orderdate: Rep[Int], o_orderpriority: Rep[String], o_clerk: Rep[String], o_shippriority: Rep[Int], o_comment: Rep[String]) =
      new SeqORDERS(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
  def unmkORDERS(p: Rep[ORDERS]) =
    Some((p.o_orderkey, p.o_custkey, p.o_orderstatus, p.o_totalprice, p.o_orderdate, p.o_orderpriority, p.o_clerk, p.o_shippriority, p.o_comment))

  case class SeqCUSTOMER
      (override val c_custkey: Rep[Int], override val c_name: Rep[String], override val c_address: Rep[String], override val c_nationkey: Rep[Int], override val c_phone: Rep[String], override val c_acctbal: Rep[Double], override val c_mktsegment: Rep[String], override val c_comment: Rep[String])
      
    extends CUSTOMER(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
        with UserTypeSeq[TableRecord, CUSTOMER] {
    lazy val selfType = element[CUSTOMER].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val CUSTOMER = new CUSTOMERCompanionAbs with UserTypeSeq[CUSTOMERCompanionAbs, CUSTOMERCompanionAbs] {
    lazy val selfType = element[CUSTOMERCompanionAbs]
  }

  def mkCUSTOMER
      (c_custkey: Rep[Int], c_name: Rep[String], c_address: Rep[String], c_nationkey: Rep[Int], c_phone: Rep[String], c_acctbal: Rep[Double], c_mktsegment: Rep[String], c_comment: Rep[String]) =
      new SeqCUSTOMER(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
  def unmkCUSTOMER(p: Rep[CUSTOMER]) =
    Some((p.c_custkey, p.c_name, p.c_address, p.c_nationkey, p.c_phone, p.c_acctbal, p.c_mktsegment, p.c_comment))

  case class SeqSUPPLIER
      (override val s_suppkey: Rep[Int], override val s_name: Rep[String], override val s_address: Rep[String], override val s_nationkey: Rep[Int], override val s_phone: Rep[String], override val s_acctbal: Rep[Double], override val s_comment: Rep[String])
      
    extends SUPPLIER(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
        with UserTypeSeq[TableRecord, SUPPLIER] {
    lazy val selfType = element[SUPPLIER].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val SUPPLIER = new SUPPLIERCompanionAbs with UserTypeSeq[SUPPLIERCompanionAbs, SUPPLIERCompanionAbs] {
    lazy val selfType = element[SUPPLIERCompanionAbs]
  }

  def mkSUPPLIER
      (s_suppkey: Rep[Int], s_name: Rep[String], s_address: Rep[String], s_nationkey: Rep[Int], s_phone: Rep[String], s_acctbal: Rep[Double], s_comment: Rep[String]) =
      new SeqSUPPLIER(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
  def unmkSUPPLIER(p: Rep[SUPPLIER]) =
    Some((p.s_suppkey, p.s_name, p.s_address, p.s_nationkey, p.s_phone, p.s_acctbal, p.s_comment))

  case class SeqPARTSUPP
      (override val ps_partkey: Rep[Int], override val ps_suppkey: Rep[Int], override val ps_availqty: Rep[Int], override val ps_supplycost: Rep[Double], override val ps_comment: Rep[String])
      
    extends PARTSUPP(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
        with UserTypeSeq[TableRecord, PARTSUPP] {
    lazy val selfType = element[PARTSUPP].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val PARTSUPP = new PARTSUPPCompanionAbs with UserTypeSeq[PARTSUPPCompanionAbs, PARTSUPPCompanionAbs] {
    lazy val selfType = element[PARTSUPPCompanionAbs]
  }

  def mkPARTSUPP
      (ps_partkey: Rep[Int], ps_suppkey: Rep[Int], ps_availqty: Rep[Int], ps_supplycost: Rep[Double], ps_comment: Rep[String]) =
      new SeqPARTSUPP(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
  def unmkPARTSUPP(p: Rep[PARTSUPP]) =
    Some((p.ps_partkey, p.ps_suppkey, p.ps_availqty, p.ps_supplycost, p.ps_comment))

  case class SeqREGION
      (override val r_regionkey: Rep[Int], override val r_name: Rep[String], override val r_comment: Rep[String])
      
    extends REGION(r_regionkey, r_name, r_comment)
        with UserTypeSeq[TableRecord, REGION] {
    lazy val selfType = element[REGION].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val REGION = new REGIONCompanionAbs with UserTypeSeq[REGIONCompanionAbs, REGIONCompanionAbs] {
    lazy val selfType = element[REGIONCompanionAbs]
  }

  def mkREGION
      (r_regionkey: Rep[Int], r_name: Rep[String], r_comment: Rep[String]) =
      new SeqREGION(r_regionkey, r_name, r_comment)
  def unmkREGION(p: Rep[REGION]) =
    Some((p.r_regionkey, p.r_name, p.r_comment))

  case class SeqNATION
      (override val n_nationkey: Rep[Int], override val n_name: Rep[String], override val n_regionkey: Rep[Int], override val n_comment: Rep[String])
      
    extends NATION(n_nationkey, n_name, n_regionkey, n_comment)
        with UserTypeSeq[TableRecord, NATION] {
    lazy val selfType = element[NATION].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val NATION = new NATIONCompanionAbs with UserTypeSeq[NATIONCompanionAbs, NATIONCompanionAbs] {
    lazy val selfType = element[NATIONCompanionAbs]
  }

  def mkNATION
      (n_nationkey: Rep[Int], n_name: Rep[String], n_regionkey: Rep[Int], n_comment: Rep[String]) =
      new SeqNATION(n_nationkey, n_name, n_regionkey, n_comment)
  def unmkNATION(p: Rep[NATION]) =
    Some((p.n_nationkey, p.n_name, p.n_regionkey, p.n_comment))

  case class SeqPART
      (override val p_partkey: Rep[Int], override val p_name: Rep[String], override val p_mfgr: Rep[String], override val p_brand: Rep[String], override val p_type: Rep[String], override val p_size: Rep[Int], override val p_container: Rep[String], override val p_retailprice: Rep[Double], override val p_comment: Rep[String])
      
    extends PART(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
        with UserTypeSeq[TableRecord, PART] {
    lazy val selfType = element[PART].asInstanceOf[Elem[TableRecord]]
    
  }
  lazy val PART = new PARTCompanionAbs with UserTypeSeq[PARTCompanionAbs, PARTCompanionAbs] {
    lazy val selfType = element[PARTCompanionAbs]
  }

  def mkPART
      (p_partkey: Rep[Int], p_name: Rep[String], p_mfgr: Rep[String], p_brand: Rep[String], p_type: Rep[String], p_size: Rep[Int], p_container: Rep[String], p_retailprice: Rep[Double], p_comment: Rep[String]) =
      new SeqPART(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
  def unmkPART(p: Rep[PART]) =
    Some((p.p_partkey, p.p_name, p.p_mfgr, p.p_brand, p.p_type, p.p_size, p.p_container, p.p_retailprice, p.p_comment))
}

// Exp -----------------------------------
trait TablesExp extends TablesAbs with TablesDsl with ScalanExp
{ self: TablesDslExp =>
  lazy val TableRecord: Rep[TableRecordCompanionAbs] = new TableRecordCompanionAbs with UserTypeDef[TableRecordCompanionAbs, TableRecordCompanionAbs] {
    lazy val selfType = element[TableRecordCompanionAbs]
    override def mirror(t: Transformer) = this
  }



  case class ExpDetail
      (override val id: Rep[Int], override val desc: Rep[String], override val weight: Rep[Double])
      
    extends Detail(id, desc, weight) with UserTypeDef[TableRecord, Detail] {
    lazy val selfType = element[Detail].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpDetail(t(id), t(desc), t(weight))
  }

  lazy val Detail: Rep[DetailCompanionAbs] = new DetailCompanionAbs with UserTypeDef[DetailCompanionAbs, DetailCompanionAbs] {
    lazy val selfType = element[DetailCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object DetailMethods {

  }

  object DetailCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[DetailCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[(Rep[Int], Rep[String], Rep[Double])] = d match {
        case MethodCall(receiver, method, Seq(id, desc, weight, _*)) if receiver.elem.isInstanceOf[DetailCompanionElem] && method.getName == "create" =>
          Some((id, desc, weight)).asInstanceOf[Option[(Rep[Int], Rep[String], Rep[Double])]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Int], Rep[String], Rep[Double])] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkDetail
    (id: Rep[Int], desc: Rep[String], weight: Rep[Double]) =
    new ExpDetail(id, desc, weight)
  def unmkDetail(p: Rep[Detail]) =
    Some((p.id, p.desc, p.weight))

  case class ExpSupplier
      (override val id: Rep[Int], override val company: Rep[String], override val address: Rep[String])
      
    extends Supplier(id, company, address) with UserTypeDef[TableRecord, Supplier] {
    lazy val selfType = element[Supplier].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpSupplier(t(id), t(company), t(address))
  }

  lazy val Supplier: Rep[SupplierCompanionAbs] = new SupplierCompanionAbs with UserTypeDef[SupplierCompanionAbs, SupplierCompanionAbs] {
    lazy val selfType = element[SupplierCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SupplierMethods {

  }

  object SupplierCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[SupplierCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[(Rep[Int], Rep[String], Rep[String])] = d match {
        case MethodCall(receiver, method, Seq(id, company, address, _*)) if receiver.elem.isInstanceOf[SupplierCompanionElem] && method.getName == "create" =>
          Some((id, company, address)).asInstanceOf[Option[(Rep[Int], Rep[String], Rep[String])]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Int], Rep[String], Rep[String])] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkSupplier
    (id: Rep[Int], company: Rep[String], address: Rep[String]) =
    new ExpSupplier(id, company, address)
  def unmkSupplier(p: Rep[Supplier]) =
    Some((p.id, p.company, p.address))

  case class ExpOrder
      (override val detail: Rep[Int], override val supplier: Rep[Int], override val amount: Rep[Int], override val price: Rep[Double], override val delivery: Rep[Int])
      
    extends Order(detail, supplier, amount, price, delivery) with UserTypeDef[TableRecord, Order] {
    lazy val selfType = element[Order].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpOrder(t(detail), t(supplier), t(amount), t(price), t(delivery))
  }

  lazy val Order: Rep[OrderCompanionAbs] = new OrderCompanionAbs with UserTypeDef[OrderCompanionAbs, OrderCompanionAbs] {
    lazy val selfType = element[OrderCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object OrderMethods {

  }

  object OrderCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[OrderCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[(Rep[Int], Rep[Int], Rep[Int], Rep[Double], Rep[Int])] = d match {
        case MethodCall(receiver, method, Seq(detail, supplier, amount, price, delivery, _*)) if receiver.elem.isInstanceOf[OrderCompanionElem] && method.getName == "create" =>
          Some((detail, supplier, amount, price, delivery)).asInstanceOf[Option[(Rep[Int], Rep[Int], Rep[Int], Rep[Double], Rep[Int])]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Int], Rep[Int], Rep[Int], Rep[Double], Rep[Int])] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkOrder
    (detail: Rep[Int], supplier: Rep[Int], amount: Rep[Int], price: Rep[Double], delivery: Rep[Int]) =
    new ExpOrder(detail, supplier, amount, price, delivery)
  def unmkOrder(p: Rep[Order]) =
    Some((p.detail, p.supplier, p.amount, p.price, p.delivery))

  case class ExpLINEITEM
      (override val l_orderkey: Rep[Int], override val l_partkey: Rep[Int], override val l_suppkey: Rep[Int], override val l_linenumber: Rep[Int], override val l_quantity: Rep[Double], override val l_extendedprice: Rep[Double], override val l_discount: Rep[Double], override val l_tax: Rep[Double], override val l_returnflag: Rep[Char], override val l_linestatus: Rep[Char], override val l_shipdate: Rep[Int], override val l_commitdate: Rep[Int], override val l_receiptdate: Rep[Int], override val l_shipinstruct: Rep[String], override val l_shipmode: Rep[String], override val l_comment: Rep[String])
      
    extends LINEITEM(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) with UserTypeDef[TableRecord, LINEITEM] {
    lazy val selfType = element[LINEITEM].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpLINEITEM(t(l_orderkey), t(l_partkey), t(l_suppkey), t(l_linenumber), t(l_quantity), t(l_extendedprice), t(l_discount), t(l_tax), t(l_returnflag), t(l_linestatus), t(l_shipdate), t(l_commitdate), t(l_receiptdate), t(l_shipinstruct), t(l_shipmode), t(l_comment))
  }

  lazy val LINEITEM: Rep[LINEITEMCompanionAbs] = new LINEITEMCompanionAbs with UserTypeDef[LINEITEMCompanionAbs, LINEITEMCompanionAbs] {
    lazy val selfType = element[LINEITEMCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object LINEITEMMethods {

  }

  object LINEITEMCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[LINEITEMCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[Arr[String]] = d match {
        case MethodCall(receiver, method, Seq(in, _*)) if receiver.elem.isInstanceOf[LINEITEMCompanionElem] && method.getName == "create" =>
          Some(in).asInstanceOf[Option[Arr[String]]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Arr[String]] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkLINEITEM
    (l_orderkey: Rep[Int], l_partkey: Rep[Int], l_suppkey: Rep[Int], l_linenumber: Rep[Int], l_quantity: Rep[Double], l_extendedprice: Rep[Double], l_discount: Rep[Double], l_tax: Rep[Double], l_returnflag: Rep[Char], l_linestatus: Rep[Char], l_shipdate: Rep[Int], l_commitdate: Rep[Int], l_receiptdate: Rep[Int], l_shipinstruct: Rep[String], l_shipmode: Rep[String], l_comment: Rep[String]) =
    new ExpLINEITEM(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
  def unmkLINEITEM(p: Rep[LINEITEM]) =
    Some((p.l_orderkey, p.l_partkey, p.l_suppkey, p.l_linenumber, p.l_quantity, p.l_extendedprice, p.l_discount, p.l_tax, p.l_returnflag, p.l_linestatus, p.l_shipdate, p.l_commitdate, p.l_receiptdate, p.l_shipinstruct, p.l_shipmode, p.l_comment))

  case class ExpORDERS
      (override val o_orderkey: Rep[Int], override val o_custkey: Rep[Int], override val o_orderstatus: Rep[Char], override val o_totalprice: Rep[Double], override val o_orderdate: Rep[Int], override val o_orderpriority: Rep[String], override val o_clerk: Rep[String], override val o_shippriority: Rep[Int], override val o_comment: Rep[String])
      
    extends ORDERS(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) with UserTypeDef[TableRecord, ORDERS] {
    lazy val selfType = element[ORDERS].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpORDERS(t(o_orderkey), t(o_custkey), t(o_orderstatus), t(o_totalprice), t(o_orderdate), t(o_orderpriority), t(o_clerk), t(o_shippriority), t(o_comment))
  }

  lazy val ORDERS: Rep[ORDERSCompanionAbs] = new ORDERSCompanionAbs with UserTypeDef[ORDERSCompanionAbs, ORDERSCompanionAbs] {
    lazy val selfType = element[ORDERSCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object ORDERSMethods {

  }

  object ORDERSCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[ORDERSCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkORDERS
    (o_orderkey: Rep[Int], o_custkey: Rep[Int], o_orderstatus: Rep[Char], o_totalprice: Rep[Double], o_orderdate: Rep[Int], o_orderpriority: Rep[String], o_clerk: Rep[String], o_shippriority: Rep[Int], o_comment: Rep[String]) =
    new ExpORDERS(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
  def unmkORDERS(p: Rep[ORDERS]) =
    Some((p.o_orderkey, p.o_custkey, p.o_orderstatus, p.o_totalprice, p.o_orderdate, p.o_orderpriority, p.o_clerk, p.o_shippriority, p.o_comment))

  case class ExpCUSTOMER
      (override val c_custkey: Rep[Int], override val c_name: Rep[String], override val c_address: Rep[String], override val c_nationkey: Rep[Int], override val c_phone: Rep[String], override val c_acctbal: Rep[Double], override val c_mktsegment: Rep[String], override val c_comment: Rep[String])
      
    extends CUSTOMER(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) with UserTypeDef[TableRecord, CUSTOMER] {
    lazy val selfType = element[CUSTOMER].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpCUSTOMER(t(c_custkey), t(c_name), t(c_address), t(c_nationkey), t(c_phone), t(c_acctbal), t(c_mktsegment), t(c_comment))
  }

  lazy val CUSTOMER: Rep[CUSTOMERCompanionAbs] = new CUSTOMERCompanionAbs with UserTypeDef[CUSTOMERCompanionAbs, CUSTOMERCompanionAbs] {
    lazy val selfType = element[CUSTOMERCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object CUSTOMERMethods {

  }

  object CUSTOMERCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[CUSTOMERCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkCUSTOMER
    (c_custkey: Rep[Int], c_name: Rep[String], c_address: Rep[String], c_nationkey: Rep[Int], c_phone: Rep[String], c_acctbal: Rep[Double], c_mktsegment: Rep[String], c_comment: Rep[String]) =
    new ExpCUSTOMER(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
  def unmkCUSTOMER(p: Rep[CUSTOMER]) =
    Some((p.c_custkey, p.c_name, p.c_address, p.c_nationkey, p.c_phone, p.c_acctbal, p.c_mktsegment, p.c_comment))

  case class ExpSUPPLIER
      (override val s_suppkey: Rep[Int], override val s_name: Rep[String], override val s_address: Rep[String], override val s_nationkey: Rep[Int], override val s_phone: Rep[String], override val s_acctbal: Rep[Double], override val s_comment: Rep[String])
      
    extends SUPPLIER(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) with UserTypeDef[TableRecord, SUPPLIER] {
    lazy val selfType = element[SUPPLIER].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpSUPPLIER(t(s_suppkey), t(s_name), t(s_address), t(s_nationkey), t(s_phone), t(s_acctbal), t(s_comment))
  }

  lazy val SUPPLIER: Rep[SUPPLIERCompanionAbs] = new SUPPLIERCompanionAbs with UserTypeDef[SUPPLIERCompanionAbs, SUPPLIERCompanionAbs] {
    lazy val selfType = element[SUPPLIERCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SUPPLIERMethods {

  }

  object SUPPLIERCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[SUPPLIERCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkSUPPLIER
    (s_suppkey: Rep[Int], s_name: Rep[String], s_address: Rep[String], s_nationkey: Rep[Int], s_phone: Rep[String], s_acctbal: Rep[Double], s_comment: Rep[String]) =
    new ExpSUPPLIER(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
  def unmkSUPPLIER(p: Rep[SUPPLIER]) =
    Some((p.s_suppkey, p.s_name, p.s_address, p.s_nationkey, p.s_phone, p.s_acctbal, p.s_comment))

  case class ExpPARTSUPP
      (override val ps_partkey: Rep[Int], override val ps_suppkey: Rep[Int], override val ps_availqty: Rep[Int], override val ps_supplycost: Rep[Double], override val ps_comment: Rep[String])
      
    extends PARTSUPP(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) with UserTypeDef[TableRecord, PARTSUPP] {
    lazy val selfType = element[PARTSUPP].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpPARTSUPP(t(ps_partkey), t(ps_suppkey), t(ps_availqty), t(ps_supplycost), t(ps_comment))
  }

  lazy val PARTSUPP: Rep[PARTSUPPCompanionAbs] = new PARTSUPPCompanionAbs with UserTypeDef[PARTSUPPCompanionAbs, PARTSUPPCompanionAbs] {
    lazy val selfType = element[PARTSUPPCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object PARTSUPPMethods {

  }

  object PARTSUPPCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[PARTSUPPCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkPARTSUPP
    (ps_partkey: Rep[Int], ps_suppkey: Rep[Int], ps_availqty: Rep[Int], ps_supplycost: Rep[Double], ps_comment: Rep[String]) =
    new ExpPARTSUPP(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
  def unmkPARTSUPP(p: Rep[PARTSUPP]) =
    Some((p.ps_partkey, p.ps_suppkey, p.ps_availqty, p.ps_supplycost, p.ps_comment))

  case class ExpREGION
      (override val r_regionkey: Rep[Int], override val r_name: Rep[String], override val r_comment: Rep[String])
      
    extends REGION(r_regionkey, r_name, r_comment) with UserTypeDef[TableRecord, REGION] {
    lazy val selfType = element[REGION].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpREGION(t(r_regionkey), t(r_name), t(r_comment))
  }

  lazy val REGION: Rep[REGIONCompanionAbs] = new REGIONCompanionAbs with UserTypeDef[REGIONCompanionAbs, REGIONCompanionAbs] {
    lazy val selfType = element[REGIONCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object REGIONMethods {

  }

  object REGIONCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[REGIONCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkREGION
    (r_regionkey: Rep[Int], r_name: Rep[String], r_comment: Rep[String]) =
    new ExpREGION(r_regionkey, r_name, r_comment)
  def unmkREGION(p: Rep[REGION]) =
    Some((p.r_regionkey, p.r_name, p.r_comment))

  case class ExpNATION
      (override val n_nationkey: Rep[Int], override val n_name: Rep[String], override val n_regionkey: Rep[Int], override val n_comment: Rep[String])
      
    extends NATION(n_nationkey, n_name, n_regionkey, n_comment) with UserTypeDef[TableRecord, NATION] {
    lazy val selfType = element[NATION].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpNATION(t(n_nationkey), t(n_name), t(n_regionkey), t(n_comment))
  }

  lazy val NATION: Rep[NATIONCompanionAbs] = new NATIONCompanionAbs with UserTypeDef[NATIONCompanionAbs, NATIONCompanionAbs] {
    lazy val selfType = element[NATIONCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object NATIONMethods {

  }

  object NATIONCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[NATIONCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkNATION
    (n_nationkey: Rep[Int], n_name: Rep[String], n_regionkey: Rep[Int], n_comment: Rep[String]) =
    new ExpNATION(n_nationkey, n_name, n_regionkey, n_comment)
  def unmkNATION(p: Rep[NATION]) =
    Some((p.n_nationkey, p.n_name, p.n_regionkey, p.n_comment))

  case class ExpPART
      (override val p_partkey: Rep[Int], override val p_name: Rep[String], override val p_mfgr: Rep[String], override val p_brand: Rep[String], override val p_type: Rep[String], override val p_size: Rep[Int], override val p_container: Rep[String], override val p_retailprice: Rep[Double], override val p_comment: Rep[String])
      
    extends PART(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) with UserTypeDef[TableRecord, PART] {
    lazy val selfType = element[PART].asInstanceOf[Elem[TableRecord]]
    override def mirror(t: Transformer) = ExpPART(t(p_partkey), t(p_name), t(p_mfgr), t(p_brand), t(p_type), t(p_size), t(p_container), t(p_retailprice), t(p_comment))
  }

  lazy val PART: Rep[PARTCompanionAbs] = new PARTCompanionAbs with UserTypeDef[PARTCompanionAbs, PARTCompanionAbs] {
    lazy val selfType = element[PARTCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object PARTMethods {

  }

  object PARTCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[PARTCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkPART
    (p_partkey: Rep[Int], p_name: Rep[String], p_mfgr: Rep[String], p_brand: Rep[String], p_type: Rep[String], p_size: Rep[Int], p_container: Rep[String], p_retailprice: Rep[Double], p_comment: Rep[String]) =
    new ExpPART(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
  def unmkPART(p: Rep[PART]) =
    Some((p.p_partkey, p.p_name, p.p_mfgr, p.p_brand, p.p_type, p.p_size, p.p_container, p.p_retailprice, p.p_comment))

  object TableRecordMethods {

  }

  object TableRecordCompanionMethods {

  }
}
