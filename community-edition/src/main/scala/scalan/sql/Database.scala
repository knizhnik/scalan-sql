package scalan.sql

import scalan._
import scalan.common.Default

/**
 * Created by knizhnik on 2/26/15.
 */
trait Database extends Base {
  self: DatabaseDsl =>

  trait Session extends Reifiable[Session] {}
  trait SessionCompanion {}

  abstract class DatabaseSession(val login: Rep[String]) extends Session

  trait DatabaseSessionCompanion extends ConcreteClass0[DatabaseSession] with SessionCompanion {
    def defaultOf = Default.defaultVal(DatabaseSession(""))
  }

  def schema = sql(s"""
  create table lineitem(
    l_orderkey integer,
    l_partkey integer,
    l_suppkey integer,
    l_linenumber integer,
    l_quantity real,
    l_extendedprice real,
    l_discount real,
    l_tax real,
    l_returnflag char,
    l_linestatus char,
    l_shipdate date,
    l_commitdate date,
    l_receiptdate date,
    l_shipinstruct varchar,
    l_shipmode varchar,
    l_comment varchar);
    """)
  
  type Lineitem
  type Q1_Result

  def Q1(items: Table[Lineitem]): Q1_Result = sql("""
  select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
  from
    lineitem
  where
    l_shipdate <= 19981201
  group by
    l_returnflag,
    l_linestatus
  order by
    l_returnflag,
    l_linestatus""")
}

trait DatabaseDsl extends ScalanDsl with SqlDsl with impl.DatabaseAbs with Database

trait DatabaseDslSeq extends DatabaseDsl with SqlDslSeq with impl.DatabaseSeq with ScalanSeq

trait DatabaseDslExp extends DatabaseDsl with SqlDslExp with impl.DatabaseExp with ScalanExp
