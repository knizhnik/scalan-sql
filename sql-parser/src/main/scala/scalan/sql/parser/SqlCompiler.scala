package main.scala.scalan.sql.parser


/**
 * Created by knizhnik on 1/14/15.
 */
trait SqlCompiler extends SqlAST with SqlParser {
  case class Scope(ctx: Context, outer: Option[Scope], nesting: Int, self: String) {
    def lookup(col: ColumnRef): Binding = {
      ctx.resolve(col.table, col.name) match {
        case Some(b) => b
        case None => {
          outer match {
            case Some(s: Scope) => s.lookup(col)
            case _ => throw SqlException( s"""Failed to lookup column ${col.table}.${col.name}""")
          }
        }
      }
    }
  }

  var currScope: Scope = Scope(new GlobalContext, None, 0, "scalan")

  def pushContext(opd: Operator) = {
    currScope = Scope(buildContext(opd), Some(currScope), currScope.nesting + 1, if (currScope.nesting == 0) "r" else "r" + currScope.nesting.toString)
  }

  def popContext() = {
    currScope = currScope.outer.get
  }

  def lookup(col: ColumnRef): Binding = currScope.lookup(col)
  
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

  def tables(op: Operator, alias: String = ""): String = {
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

  def indexToPath(i: Int, n: Int) = {
    val path = ".tail" * i
    if (i == n - 1) path else path + ".head"
  }

  case class Binding(table: String, path: String, column: Column)

  abstract class Context {
    def resolve(scope: String, name: String): Option[Binding]
  }

  class GlobalContext() extends Context {
    def resolve(scope: String, name: String): Option[Binding] = None
  }
  
  case class TableContext(table: Table) extends Context {
    def resolve(scope: String, name: String): Option[Binding] = {
      if (scope.isEmpty || scope == table.name) {
        val i = table.schema.indexWhere(c => c.name == name)
        if (i >= 0) Some(Binding(table.name, indexToPath(i, table.schema.length), table.schema(i))) else None
      } else None
    }
  }

  case class JoinContext(outer: Context, inner: Context) extends Context {
    def resolve(scope: String, name: String): Option[Binding] = {
      (outer.resolve(scope, name), inner.resolve(scope, name)) match {
        case (Some(b), None) => Some(Binding(b.table, ".head" + b.path, b.column))
        case (None, Some(b)) => Some(Binding(b.table, ".tail" + b.path, b.column))
        case (Some(_), Some(_)) => throw SqlException( s"""Ambiguous reference to $scope.$name""")
        case _ => None

      }
    }
  }

  case class AliasContext(parent: Context, alias: String) extends Context {
    def resolve(scope: String, name: String): Option[Binding] = {
      if (scope == alias) {
        parent.resolve("", name) match {
          case Some(b) => Some(Binding(alias, b.path, b.column))
          case None => None
        }
      } else parent.resolve(scope, name)
    }
  }

  case class ProjectContext(parent: Context, columns: ExprList) extends Context {
    def resolve(scope: String, name: String): Option[Binding] = {
      val i = columns.indexWhere(c => (scope.isEmpty && c.alias == name) || c == ColumnRef(scope, name))
      if (i >= 0) {
        val saveScope = currScope
        currScope = Scope(parent, None, 0, "r")
        val cType =  getExprType(columns(i))
        currScope = saveScope
        Some(Binding(currScope.self, indexToPath(i, columns.length), Column(name, cType)))
      }
      else None
    }
  }

  def buildContext(op: Operator): Context = {
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

  def divOp(expr: Expression) = if (getExprType(expr) == IntType) "/!" else "/"

 
  def castTo(from: Expression, to: Expression): Expression = {
    val fromType = getExprType(from)
    val toType = getExprType(to)
    if (fromType != toType && (toType == StringType || toType == DoubleType)) CastExpr(from, toType) else from
  }

  def patternMatch(text: Expression, pattern: Expression): String = {
    val left = generateExpr(text)
    pattern match {
      case Literal(v, t) if (t == StringType) =>
        val p = v.toString
        if (p.indexOf('%') < 0 && p.indexOf('_') < 0) "(" + left + " == \"" + p + "\")"
        else if (p.lastIndexOf('%') == 0 && p.indexOf('_') < 0) left + ".startsWith(\"" + p.substring(1) + "\")"
        else if (p.indexOf('%') == p.length - 1 && p.indexOf('_') < 0) left + ".endsWith(\"" + p.substring(0, p.length - 1) + "\")"
        else if (p.lastIndexOf('%', p.length - 2) == 0 && p.indexOf('%', 1) == p.length - 1 && p.indexOf('_') < 0) left + ".contains(\"" + p.substring(1, p.length - 1) + "\")"
        else left + ".matches(\"" + p.replace("%", ".*").replace('_', '.') + "\")"
    }
  }

  def generateCaseWhen(list: ExprList, i: Int): String = {
    if (i == list.length) ""
    else if (i == list.length - 1) " else " + generateExpr(list(i))
    else (if (i == 0) "if (" else " else if (") + generateExpr(list(i)) + ") " + generateExpr(list(i + 1)) + generateCaseWhen(list, i + 2)
  }

  def generateIn(sel: Expression, lst: ExprList): String = {
    if (lst.length == 1) generateExpr(lst(0)) + ".toArray.contains(" + generateExpr(sel) + ")"
    else "(" + lst.map(alt => (generateExpr(sel) + " = " + generateExpr(alt))).mkString(" or ") + ")"
  }

  def printValue(value: Any, tp: ColumnType): String = {
    if (tp == StringType) "\"" + value.toString + "\""
    else value.toString
  }

  def generateExpr(expr: Expression): String = {
    expr match {
      case AndExpr(l, r) => generateExpr(l) + " && " + generateExpr(r)
      case OrExpr(l, r) => "(" + generateExpr(l) + " || " + generateExpr(r) + ")"
      case AddExpr(l, r) => "(" + generateExpr(castTo(l, r)) + " + " + generateExpr(castTo(r, l)) + ")"
      case SubExpr(l, r) => "(" + generateExpr(castTo(l, r)) + " - " + generateExpr(castTo(r, l)) + ")"
      case MulExpr(l, r) => "(" + generateExpr(castTo(l, r)) + " * " + generateExpr(castTo(r, l)) + ")"
      case DivExpr(l, r) => "(" + generateExpr(castTo(l, r)) + divOp(expr) + generateExpr(castTo(r, l)) + ")"
      case EqExpr(l, r) => generateExpr(castTo(l, r)) + " === " + generateExpr(castTo(r, l))
      case NeExpr(l, r) => generateExpr(castTo(l, r)) + " !=== " + generateExpr(castTo(r, l))
      case LeExpr(l, r) => generateExpr(castTo(l, r)) + " <= " + generateExpr(castTo(r, l))
      case LtExpr(l, r) => generateExpr(castTo(l, r)) + " < " + generateExpr(castTo(r, l))
      case GtExpr(l, r) => generateExpr(castTo(l, r)) + " > " + generateExpr(castTo(r, l))
      case GeExpr(l, r) => generateExpr(castTo(l, r)) + " >= " + generateExpr(castTo(r, l))
      case ExistsExpr(q) => generateExpr(q) + ".count <> 0"
      case LikeExpr(l, r) => patternMatch(l, r)
      case NegExpr(opd) => "-" + generateExpr(opd)
      case NotExpr(opd) => "!(" + generateExpr(opd) + ")"
      case Literal(v, t) => "toRep(" + printValue(v, t) + ")"
      case CastExpr(exp, typ) => generateExpr(exp) + (if (typ == StringType) ".toStr" else ".to" + typ.scalaName)
      case c: ColumnRef => currScope.self + lookup(c).path
      case SelectExpr(s) => "(" + generateOperator(s.operator) + ")"
      case CountExpr() => "count"
      case CountDistinctExpr(_) => "count" // TODO: exclude duplicates
      case CountNotNullExpr(_) => "count"  // TODO: NULLs are not supported now
      case AvgExpr(opd) => "avg(" + generateExpr(opd) + ")"
      case SumExpr(opd) => "sum(" + generateExpr(opd) + ")"
      case MaxExpr(opd) => "max(" + generateExpr(opd) + ")"
      case MinExpr(opd) => "min(" + generateExpr(opd) + ")"
      case SubstrExpr(str, from, len) => generateExpr(str) + ".substring(" + generateExpr(from) + ", " + generateExpr(from) + " + " + generateExpr(len) + ")"
      case CaseWhenExpr(list) => generateCaseWhen(list, 0)
      case InListExpr(sel, lst) => "(" + lst.map(alt => (generateExpr(sel) + " = " + generateExpr(alt))).mkString(" or ") + ")"
      case InExpr(sel, query) => generateOperator(query.operator) + ".where(e => e == " + generateExpr(sel) + ").count <> 0"
    }
  }

  implicit class TypeImplicitCasts(left: ColumnType) {
    def |(right: ColumnType): ColumnType = {
      if (left == right) left
      else if (left == StringType || right == StringType) StringType
      else if (left == DoubleType || right == DoubleType) DoubleType
      else if (left == IntType || right == IntType) IntType
      else throw SqlException("Incompatible types " + left.sqlName + " and " + right.sqlName)
    }
  }

  def getExprType(expr: Expression): ColumnType = {
    expr match {
      case AndExpr(l, r) => BoolType
      case OrExpr(l, r) => BoolType
      case AddExpr(l, r) => getExprType(l) | getExprType(r)
      case SubExpr(l, r) => getExprType(l) | getExprType(r)
      case MulExpr(l, r) => getExprType(l) | getExprType(r)
      case DivExpr(l, r) => getExprType(l) | getExprType(r)
      case NeExpr(l, r) => BoolType
      case EqExpr(l, r) => BoolType
      case LeExpr(l, r) => BoolType
      case LtExpr(l, r) => BoolType
      case GtExpr(l, r) => BoolType
      case GeExpr(l, r) => BoolType
      case LikeExpr(l, r) => BoolType
      case InExpr(l, r) => BoolType
      case ExistsExpr(_) => BoolType
      case NegExpr(opd) => getExprType(opd)
      case NotExpr(_) => BoolType
      case CountExpr() => IntType
      case CountNotNullExpr(_) => IntType
      case CountDistinctExpr(_) => IntType
      case AvgExpr(_) => DoubleType
      case SubstrExpr(str,from,len) => StringType
      case SumExpr(agg) => getExprType(agg)
      case MaxExpr(agg) => getExprType(agg)
      case MinExpr(agg) => getExprType(agg)
      case CaseWhenExpr(list) => getExprType(list(1))
      case Literal(v, t) => t
      case CastExpr(e, t) => t
      case c: ColumnRef => lookup(c).column.ctype
      case SelectExpr(s) => DoubleType
    }
  }

  def buildTree(elems: Seq[String], lpar: String = "(", rpar: String = ")", i: Int = 0): String = {
    val n = elems.length
    if (i < n - 2) lpar + elems(i) + ", " + buildTree(elems, lpar, rpar, i + 1)
    else if (n >= 2) lpar + elems(i) + ", " + elems(i + 1) + (rpar * (n - 1))
    else if (n != 0) elems(i)
    else "()"
  }

  def generateExprList(list: ExprList): String = buildTree(list.map(expr => generateExpr(expr)), "Pair(")

  def resolveKey(key: Expression): Option[Binding] = {
    key match {
      case ColumnRef(table, name) => currScope.ctx.resolve(table, name)
      case _ => throw SqlException("Unsupported join condition")
    }
  }

  def extractKey(on: Expression): String = {
    on match {
      case AndExpr(l, r) => "Pair(" + extractKey(l) + ", " + extractKey(r) + ")"
      case EqExpr(l, r) => (resolveKey(l), resolveKey(r)) match {
        case (Some(_), Some(_)) => throw SqlException("Ambiguous reference to column")
        case (Some(b), None) => currScope.self + b.path
        case (None, Some(b)) => currScope.self + b.path
        case (None, None) => throw SqlException("Failed to locate column in join condition")
      }
    }
  }

  def generateJoinKey(table: Operator, on: Expression): String = {
    pushContext(table)
    val result = currScope.self + " => " + extractKey(on)
    popContext()
    result
  }

  def generateLambdaExpr(table: Operator, exp: Expression): String = {
    pushContext(table)
    val result = currScope.self + " => " + generateExpr(exp)
    popContext()
    result
  }

  def generateLambdaExprList(table: Operator, exps: ExprList): String = {
    pushContext(table)
    val result = currScope.self + " => " + generateExprList(exps)
    popContext()
    result
  }


  def isAggregate(agg: Expression): Boolean = {
    agg match {
      case CountExpr() => true
      case CountNotNullExpr(_) => true
      case CountDistinctExpr(_) => true
      case AvgExpr(_) => true
      case SumExpr(_) => true
      case MaxExpr(_) => true
      case MinExpr(_) => true
      case _ => false
    }
  }


  def generateAggOperand(agg: Expression): String = {
    agg match {
      case CountExpr() => "1"
      case CountNotNullExpr(_) => "1"
      case CountDistinctExpr(_) => "1"
      case AvgExpr(opd) => generateExpr(opd)
      case SumExpr(opd) => generateExpr(opd)
      case MaxExpr(opd) => generateExpr(opd)
      case MinExpr(opd) => generateExpr(opd)
    }
  }

  def aggCombine(agg: Expression, s1: String, s2: String): String = {
    agg match {
      case CountExpr() => s"""$s1 + $s2"""
      case CountNotNullExpr(_) => s"""$s1 + $s2"""
      case CountDistinctExpr(_) => s"""$s1 + $s2"""
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
    currScope.self + ".tail" + indexToPath(aggIndex, aggregates.length)
  }

  def matchExpr(col: Expression, exp: Expression): Boolean = {
    col == exp || (exp match {
      case ColumnRef(table, name) if table.isEmpty => name == col.alias
      case _ => false
    })
  }

  def generateAggResult(columns: ExprList, aggregates: ExprList, gby: ExprList, i: Int, count: Int): String = {
    columns(i) match {
      case CountExpr() => getAggPath(columns, aggregates, i)
      case CountNotNullExpr(_) => getAggPath(columns, aggregates, i)
      case CountDistinctExpr(_) => getAggPath(columns, aggregates, i)
      case AvgExpr(opd) => s"""(${getAggPath(columns, aggregates, i)}.toDouble / ${currScope.self}.tail${indexToPath(count, aggregates.length)}.toDouble)"""
      case SumExpr(opd) => getAggPath(columns, aggregates, i)
      case MaxExpr(opd) => getAggPath(columns, aggregates, i)
      case MinExpr(opd) => getAggPath(columns, aggregates, i)
      case c: ColumnRef => {
        val keyIndex = gby.indexWhere(k => matchExpr(c, k))
        if (keyIndex < 0) throw SqlException("Unsupported group-by clause")
        currScope.self + ".head" + indexToPath(keyIndex, gby.length)
      }
    }
  }

  def ref(e: Expression): ColumnRef = {
    e match {
      case c: ColumnRef => c
      case _ => throw SqlException("Column reference expected")
    }
  }

  def groupBy(agg: Operator, gby: ExprList): String = {
    agg match {
      case Project(p, columns)  => {
        var aggregates = columns.filter(e => isAggregate(e))
        var countIndex = aggregates.indexWhere(e => e.isInstanceOf[CountExpr])
        if (countIndex < 0 && aggregates.exists(e => e.isInstanceOf[AvgExpr])) {
          countIndex = aggregates.length
          aggregates = aggregates :+ CountExpr()
        }
        pushContext(p)
        val aggTypes = buildTree(aggregates.map(agg => getExprType(agg).scalaName))
        val groupBy = buildTree(gby.map(col => currScope.self + lookup(ref(col)).path), "Pair(")
        val map = buildTree(aggregates.map(agg => generateAggOperand(agg)), "Pair(")
        val reduce = Array.tabulate(aggregates.length)(i => aggCombine(aggregates(i), "s1._" + (i+1), "s2._" + (i+1))).mkString(",")
        val aggResult = buildTree(Array.tabulate(columns.length)(i => generateAggResult(columns, aggregates, gby, i, countIndex)), "Pair(")
        val result = s"""ReadOnlyTable(${generateOperator(p)}.mapReduce(${currScope.self} => Pair(${groupBy}, ${map}),
           | (s1: Rep[${aggTypes}], s2: Rep[${aggTypes}]) => (${reduce})).toArray.map(${currScope.self} => ${aggResult}))""".stripMargin
        popContext()
        result
      }
      case _ => throw SqlException("Unsupported group-by clause")
    }
  }

  def isGrandAggregate(columns: ExprList): Boolean = {
    columns.length == 1 && isAggregate(columns(0))
  }

  def generateOperator(op:Operator): String = {
    op match {
      case Join(outer, inner, on) => generateOperator(outer) + ".join(" + generateOperator(inner) + ")(" + generateJoinKey(outer, on) + ", " + generateJoinKey(inner, on) + ")"
      case Scan(t) => t.name.toLowerCase
      case OrderBy(p, by) => generateOperator(p) + ".orderBy(" + generateLambdaExprList(p, by) + ")"
      case GroupBy(p, by) => groupBy(p, by)
      case Filter(p, predicate) => generateOperator(p) + ".where(" + generateLambdaExpr(p, predicate) + ")"
      case Project(p, columns) =>
        if (isGrandAggregate(columns)) {
          generateOperator(p) + "." + generateLambdaExprList(p, columns)
        } else {
          generateOperator(p) + ".select(" + generateLambdaExprList(p, columns) + ")"
        }
      case TableAlias(t, a) => a
      case SubSelect(p) => generateOperator(p)
    }
  }

  def tableToArray(op:Operator):String = {
    op match {
      case OrderBy(p, by) => ""
      case Project(p, c) if (isGrandAggregate(c)) => ""
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
    currScope = Scope(TableContext(index.table), Some(currScope), 0, "r")
    val result = s"""def ${index.name}(${currScope.self}: Rep[${index.table.name.capitalize}]) = ${buildTree(index.key.map(part => currScope.self + lookup(ColumnRef("", part)).path), "Pair(")}"""
    popContext()
    result
  }
}
