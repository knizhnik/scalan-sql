package main.scala.scalan.sql.parser

/**
 * Created by knizhnik on 1/13/15.
 */
trait SqlAST {

  abstract sealed class Statement

  case class SqlException(msg: String) extends Exception(msg)

  type Script = Array[Statement]
  type Schema = Array[Column]
  type ColumnList = Array[ColumnRef]
  type ExprList = Array[ColumnExpr]

  case class Table(name: String, schema: Schema)

  case class Column(name: String, ctype: ColumnType)

  case class ColumnType(sqlName: String, scalaName: String)

  val IntType = ColumnType("integer", "Int")
  val DoubleType = ColumnType("real", "Double")
  val LongType = ColumnType("bigint", "Long")
  val StringType = ColumnType("varchar", "String")
  val CharType = ColumnType("char", "Char")
  val BoolType = ColumnType("bit", "Boolean")
  val DateType = ColumnType("date", "Int")

  abstract sealed class Operator

  case class Scan(table: Table) extends Operator

  case class TableAlias(table: Operator, alias: String) extends Operator

  case class Project(parent: Operator, columns: ExprList) extends Operator

  case class Filter(parent: Operator, predicate: ColumnExpr) extends Operator

  case class GroupBy(parent: Operator, columns: ColumnList) extends Operator

  case class OrderBy(parent: Operator, columns: ColumnList) extends Operator

  case class SubSelect(parent: Operator) extends Operator

  case class Join(outer: Operator, inner: Operator, on: ColumnExpr) extends Operator

  case class SelectStmt(operator: Operator) extends Statement


  case class CreateTableStmt(table: Table) extends Statement

  case class CreateIndexStmt(name: String, table: Table, key: ColumnList) extends Statement

  sealed abstract class ColumnExpr {
    var alias = ""
  }

  case class AddExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class SubExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class MulExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class DivExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class EqExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class NeExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class GtExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class GeExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class LtExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class LeExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class AndExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class OrExpr(left: ColumnExpr, right: ColumnExpr) extends ColumnExpr

  case class NegExpr(opd: ColumnExpr) extends ColumnExpr

  case class NotExpr(opd: ColumnExpr) extends ColumnExpr

  case class AvgExpr(opd: ColumnExpr) extends ColumnExpr

  case class SumExpr(opd: ColumnExpr) extends ColumnExpr

  case class MaxExpr(opd: ColumnExpr) extends ColumnExpr

  case class MinExpr(opd: ColumnExpr) extends ColumnExpr

  case class CountExpr() extends ColumnExpr

  case class CastExpr(expr: ColumnExpr, to: ColumnType) extends ColumnExpr

  case class StrLiteral(value: String) extends ColumnExpr

  case class IntLiteral(value: Int) extends ColumnExpr

  case class DoubleLiteral(value: Double) extends ColumnExpr

  case class ColumnRef(table: String, name: String) extends ColumnExpr

  def ColumnList(list: ColumnRef*): ColumnList = list.toArray

  def Schema(list: Column*): Schema = list.toArray

  def Script(stmts: Statement*): Script = stmts.toArray

  def ExprList(exprs: ColumnExpr*): ExprList = exprs.toArray
}
