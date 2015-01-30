package main.scala.scalan.sql.parser


/**
 * Created by knizhnik on 1/14/15.
 */
trait SqlCompiler extends SqlAST with SqlParser {
  def generate(sql: String): String = {
    val statements = parseSql(sql)
    var nQueries = 0

    statements.map(stmt => {
      stmt match {
        case s: SelectStmt => {
          nQueries += 1
          generateQuery(nQueries, s.operator)
        }
        case s: CreateIndexStmt => generateIndex(s)
        case s: CreateTableStmt => generateTable(s.table)
      }
    }).mkString("\n\n")
  }

  def tables(op:Operator, alias: String = ""):String = {
    op match {
      case Join(outer, inner, on) => tables(outer) + ", " + tables(inner)
      case Scan(t) => (if (alias.isEmpty) t.name.toLowerCase else alias) + ": Rep[Table[" + t.name.capitalize + "]]"
      case OrderBy(p, by) => tables(p)
      case GroupBy(p, by) => tables(p)
      case Filter(p, predicate) => tables(p)
      case Project(p, columns) => tables(p)
      case TableAlias(t, a) => tables(t, a)
      case SubSelect(p) => tables(p, alias)
    }
  }

  def indexToPath(i:Int, n:Int) = {
    val path = ".tail" * i
    if (i == n-1) path else path + ".head"
  }

  case class Binding(table:String, path:String, column:Column)

  abstract class Context {
    def resolve(scope:String, name:String): Option[Binding]
  }

  case class TableContext(table:Table) extends Context {
    def resolve(scope:String, name:String) = {
      if (scope.isEmpty || scope == table.name) {
        val i = table.schema.indexWhere(c => c.name == name)
        if (i >= 0) Some(Binding(table.name, indexToPath(i, table.schema.length), table.schema(i))) else None
      } else None
    }
  }

  case class JoinContext(outer:Context, inner:Context) extends Context {
    def resolve(scope: String, name: String) = {
      (outer.resolve(scope, name), inner.resolve(scope, name)) match {
        case (Some(b), None) => Some(Binding(b.table, ".head" + b.path, b.column))
        case (None, Some(b)) => Some(Binding(b.table, ".tail" + b.path, b.column))
        case (Some(_), Some(_)) => throw SqlException( s"""Ambiguous reference to $scope.$name""")
        case _ => None

      }
    }
  }

  case class AliasContext(parent:Context, alias:String) extends Context {
    def resolve(scope: String, name: String) = {
      if (scope == alias) {
        parent.resolve("", name) match {
          case Some(b) => Some(Binding(alias, b.path, b.column))
          case None => None
        }
      } else parent.resolve(scope, name)
    }
  }

  case class ProjectContext(parent:Context, columns:ExprList) extends Context {
    def resolve(scope: String, name: String) = {
      val i = columns.indexWhere(c => (scope.isEmpty && c.alias == name) || c == ColumnRef(scope, name))
      if (i >= 0) Some(Binding("r", indexToPath(i, columns.length), Column(name, getExprType(parent, columns(i))))) else None
    }
  }

  def buildContext(op:Operator):Context = {
    op match {
      case Join(outer, inner, on) => JoinContext(buildContext(outer), buildContext(inner))
      case Scan(t) => TableContext(t)
      case OrderBy(p, by) => buildContext(p)
      case GroupBy(p, by) => buildContext(p)
      case Filter(p, predicate) => buildContext(p)
      case Project(p, columns) => ProjectContext(buildContext(p), columns)
      case TableAlias(p, a) => AliasContext(buildContext(p), a)
      case SubSelect(p) => buildContext(p)
    }

  }

  def divOp(ctx:Context, expr:ColumnExpr) = if (getExprType(ctx, expr) == IntType) "/!" else "/"

  def lookup(ctx:Context, col: ColumnRef): Binding = {
    ctx.resolve(col.table, col.name) match {
      case Some(b) => b
      case None => throw SqlException(s"""Failed to lookup column ${col.table}.${col.name}""")
    }
  }

  def castTo(ctx: Context, from:ColumnExpr, to:ColumnExpr): ColumnExpr = {
    val fromType = getExprType(ctx, from)
    val toType = getExprType(ctx, to)
    if (fromType != toType && (toType == StringType || toType == DoubleType)) CastExpr(from, toType) else from
  }
  
  def generateExpr(ctx:Context, expr:ColumnExpr): String = {
    expr match {
      case AndExpr(l, r) => generateExpr(ctx, l) + " && " +  generateExpr(ctx, r)
      case OrExpr(l, r) => "(" + generateExpr(ctx, l) + " || " +  generateExpr(ctx, r) + ")"
      case AddExpr(l, r) => "(" + generateExpr(ctx, castTo(ctx, l, r)) + " + " +  generateExpr(ctx, castTo(ctx, r, l)) + ")"
      case SubExpr(l, r) => "(" + generateExpr(ctx, castTo(ctx, l, r)) + " - " +  generateExpr(ctx, castTo(ctx, r, l)) + ")"
      case MulExpr(l, r) => "(" +generateExpr(ctx, castTo(ctx, l, r)) + " * " +  generateExpr(ctx, castTo(ctx, r, l)) + ")"
      case DivExpr(l, r) => "(" +generateExpr(ctx, castTo(ctx, l, r)) + divOp(ctx, expr) +  generateExpr(ctx, castTo(ctx, r, l)) + ")"
      case EqExpr(l, r) => generateExpr(ctx, castTo(ctx, l, r)) + " === " +  generateExpr(ctx, castTo(ctx, r, l))
      case NeExpr(l, r) => generateExpr(ctx, castTo(ctx, l, r)) + " !=== " +  generateExpr(ctx, castTo(ctx, r, l))
      case LeExpr(l, r) => generateExpr(ctx, castTo(ctx, l, r)) + " <= " +  generateExpr(ctx, castTo(ctx, r, l))
      case LtExpr(l, r) => generateExpr(ctx, castTo(ctx, l, r)) + " < " +  generateExpr(ctx, castTo(ctx, r, l))
      case GtExpr(l, r) => generateExpr(ctx, castTo(ctx, l, r)) + " > " +  generateExpr(ctx, castTo(ctx, r, l))
      case GeExpr(l, r) => generateExpr(ctx, castTo(ctx, l, r)) + " >= " +  generateExpr(ctx, castTo(ctx, r, l))
      case NegExpr(opd) => "-" + generateExpr(ctx, opd)
      case NotExpr(opd) => "!(" + generateExpr(ctx, opd) + ")"
      case StrLiteral(v) => "toRep(" + v.toString + ")"
      case IntLiteral(v) => "toRep(" + v.toString + ")"
      case DoubleLiteral(v) => "toRep(" + v.toString + ")"
      case CastExpr(exp, typ) => generateExpr(ctx, exp) + (if (typ == StringType) ".toStr" else ".to" + typ.scalaName)
      case c:ColumnRef => "r" + lookup(ctx, c).path
    }
  }

  implicit class TypeImplicitCasts(left:ColumnType) {
    def |(right: ColumnType): ColumnType = {
      if (left == right) left
      else if (left == StringType || right == StringType) StringType
      else if (left == DoubleType || right == DoubleType) DoubleType
      else if (left == IntType || right == IntType) IntType
      else throw SqlException("Incompatible types " + left.sqlName + " and " + right.sqlName)
    }
  }

  def getExprType(ctx:Context, expr:ColumnExpr): ColumnType = {
    expr match {
      case AndExpr(l, r) => BoolType
      case OrExpr(l, r) => BoolType
      case AddExpr(l, r) => getExprType(ctx, l) | getExprType(ctx, r)
      case SubExpr(l, r) => getExprType(ctx, l) | getExprType(ctx, r)
      case MulExpr(l, r) => getExprType(ctx, l) | getExprType(ctx, r)
      case DivExpr(l, r) => getExprType(ctx, l) | getExprType(ctx, r)
      case NeExpr(l, r) => BoolType
      case EqExpr(l, r) => BoolType
      case LeExpr(l, r) => BoolType
      case LtExpr(l, r) => BoolType
      case GtExpr(l, r) => BoolType
      case GeExpr(l, r) => BoolType
      case NegExpr(opd) => getExprType(ctx, opd)
      case NotExpr(_) => BoolType
      case CountExpr() => IntType
      case AvgExpr(_) => DoubleType
      case SumExpr(agg) => getExprType(ctx, agg)
      case MaxExpr(agg) => getExprType(ctx, agg)
      case MinExpr(agg) => getExprType(ctx, agg)
      case StrLiteral(_) => StringType
      case IntLiteral(_) => IntType
      case DoubleLiteral(_) => DoubleType
      case CastExpr(e,t) => t
      case c:ColumnRef => lookup(ctx, c).column.ctype
    }
  }

  def buildTree(elems:Array[String], lpar:String = "(", rpar:String = ")", i:Int = 0): String = {
    val n = elems.length
    if (i < n - 2) lpar + elems(i) + ", " + buildTree(elems, lpar, rpar, i + 1)
    else if (n >= 2) lpar + elems(i) + ", " + elems(i + 1) + (rpar * (n - 1))
    else if (n != 0) elems(i)
    else "()"
  }

  def generateExprList(ctx:Context, list:ExprList): String = buildTree(list.map(expr => generateExpr(ctx, expr)), "Pair(")

  def generateColumnList(ctx:Context, list:ColumnList): String = buildTree(list.map(col => "r" + lookup(ctx, col).path), "Pair(")

  def resolveKey(ctx:Context, key:ColumnExpr): Option[Binding] = {
    key match {
      case ColumnRef(table, name) => ctx.resolve(table, name)
      case _ => throw SqlException("Unsupported join condition")
    }
  }

  def extractKey(ctx:Context, on:ColumnExpr):String = {
    on match {
      case AndExpr(l, r) => "Pair(" + extractKey(ctx, l)  + ", " + extractKey(ctx, r) + ")"
      case EqExpr(l, r) => (resolveKey(ctx, l), resolveKey(ctx, r)) match {
        case (Some(_),Some(_)) => throw SqlException("Ambiguous reference to column")
        case (Some(b),None) => "r" + b.path
        case (None,Some(b)) => "r" + b.path
        case (None,None) => throw SqlException("Failed to locate column in join condition")
      }
    }
  }

  def isAggregate(agg: ColumnExpr): Boolean = {
    agg match {
      case CountExpr() => true
      case AvgExpr(_) => true
      case SumExpr(_) => true
      case MaxExpr(_) => true
      case MinExpr(_) => true
      case _ => false
    }
  }


  def generateAggOperand(ctx:Context, agg: ColumnExpr): String = {
    agg match {
      case CountExpr() => "1"
      case AvgExpr(opd) => generateExpr(ctx, opd)
      case SumExpr(opd) => generateExpr(ctx, opd)
      case MaxExpr(opd) => generateExpr(ctx, opd)
      case MinExpr(opd) => generateExpr(ctx, opd)
    }
  }

  def aggCombine(agg: ColumnExpr, s1:String, s2:String): String = {
    agg match {
      case CountExpr() => s"""$s1 + $s2"""
      case AvgExpr(opd) => s"""$s1 + $s2"""
      case SumExpr(opd) => s"""$s1 + $s2"""
      case MaxExpr(opd) => s"""if ($s1 > $s2) $s1 else $s2"""
      case MinExpr(opd) => s"""if ($s1 < $s2) $s1 else $s2"""
    }
  }

  def getAggPath(columns: ExprList, aggregates: ExprList, n: Int): String = {
    var aggIndex = 0
    for (i <- 0 until n) {
      if (isAggregate(columns(i))) aggIndex += 1
    }
    "r.tail" + indexToPath(aggIndex, aggregates.length)
  }


  def generateAggResult(columns: ExprList, aggregates: ExprList, gby: ColumnList, i: Int, count: Int): String = {
    columns(i) match {
      case CountExpr() => getAggPath(columns, aggregates, i)
      case AvgExpr(opd) => s"""(${getAggPath(columns, aggregates, i)}.toDouble / r.tail${indexToPath(count, aggregates.length)}.toDouble)"""
      case SumExpr(opd) => getAggPath(columns, aggregates, i)
      case MaxExpr(opd) => getAggPath(columns, aggregates, i)
      case MinExpr(opd) => getAggPath(columns, aggregates, i)
      case c: ColumnRef => {
        val keyIndex = gby.indexWhere(k => k.name == c.alias || (k == c))
        if (keyIndex < 0) throw SqlException("Unsupported group-by clause")
        "r.head" + indexToPath(keyIndex, gby.length)
      }
    }
  }


  def groupBy(agg: Operator, gby: ColumnList): String = {
    agg match {
      case Project(p, columns)  => {
        var aggregates = columns.filter(e => isAggregate(e))
        var countIndex = aggregates.indexWhere(e => e.isInstanceOf[CountExpr])
        if (countIndex < 0 && aggregates.exists(e => e.isInstanceOf[AvgExpr])) {
          countIndex = aggregates.length
          aggregates = aggregates :+ CountExpr()
        }
        val ctx = buildContext(p)
        val aggTypes = buildTree(aggregates.map(agg => getExprType(ctx, agg).scalaName))
        val groupBy = buildTree(gby.map(col => "r" + lookup(ctx, col).path), "Pair(")
        val map = buildTree(aggregates.map(agg => generateAggOperand(ctx, agg)), "Pair(")
        val reduce = Array.tabulate(aggregates.length)(i => aggCombine(aggregates(i), "s1._" + (i+1), "s2._" + (i+1))).mkString(",")
        val aggResult = buildTree(Array.tabulate(columns.length)(i => generateAggResult(columns, aggregates, gby, i, countIndex)), "Pair(")
        s"""ReadOnlyTable(${generateOperator(p)}.mapReduce(r => Pair(${groupBy}, ${map}),
           | (s1: Rep[${aggTypes}], s2: Rep[${aggTypes}]) => (${reduce})).toArray.map(r => ${aggResult}))""".stripMargin
      }
      case _ => throw SqlException("Unsupported group-by clause")
    }
  }

  def generateOperator(op:Operator):String = {
    op match {
      case Join(outer, inner, on) => generateOperator(outer) + ".join(" + generateOperator(inner) + ")(r => " + extractKey(buildContext(outer), on) + ", r => "  + extractKey(buildContext(inner), on) + ")"
      case Scan(t) => t.name.toLowerCase
      case OrderBy(p, by) => generateOperator(p) + ".orderBy(r => " + generateColumnList(buildContext(p), by) + ")"
      case GroupBy(p, by) => groupBy(p, by)
      case Filter(p, predicate) => generateOperator(p) + ".where(r => " + generateExpr(buildContext(p), predicate) + ")"
      case Project(p, columns) => generateOperator(p) + ".select(r => " + generateExprList(buildContext(p), columns) + ")"
      case TableAlias(t, a) => a
      case SubSelect(p) => generateOperator(p)
    }
  }

  def tableToArray(op:Operator):String = {
    op match {
      case OrderBy(p, by) => ""
      case _ => ".toArray"
    }
  }

  def generateQuery(q:Int, op:Operator): String = {
     s"""def Q$q(${tables(op)}) = ${generateOperator(op)}${tableToArray(op)}"""
  }

  def parseType(t: ColumnType): String = {
    t match {
      case StringType => ""
      case DateType => ".toDate"
      case _ => ".to" + t.scalaName
    }
  }

  def generateTable(table:Table): String = {
    val columns = table.schema
    val typeName = table.name.capitalize
    val typeDef = buildTree(columns.map(c => c.ctype.scalaName))
    val parse = buildTree(Array.tabulate(columns.length)(i => "c(" + i + ")" + parseType(columns(i).ctype)), "Pair(")
    val pairTableDef = buildTree(columns.map(c => "Table.create[" + c.ctype.scalaName + "](tableName + \"." + c.name + "\")"), "PairTable.create(")
    s"""type $typeName = $typeDef
       |
       |def create$typeName(tableName: Rep[String]) = $pairTableDef
       |
       |def parse$typeName(c: Arr[String]): Rep[$typeName] = $parse
       |""".stripMargin
  }

  def generateIndex(index:CreateIndexStmt): String = {
    val ctx = TableContext(index.table)
    s"""def ${index.name}(r: Rep[${index.table.name.capitalize}]) = ${buildTree(index.key.map(part => "r" + lookup(ctx, part).path), "Pair(")}"""
  }
}
