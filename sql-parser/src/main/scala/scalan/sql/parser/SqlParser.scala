package main.scala.scalan.sql.parser
import scala.collection.mutable.Map

/**
 * Created by knizhnik on 1/13/15.
 */
trait SqlParser extends SqlAST {
  import scala.util.parsing.combinator._

  def parseSql(input: String) = Grammar.parseAll(input)

  object Grammar extends JavaTokenParsers with PackratParsers {
    val builtinTypes = Array(IntType, DoubleType, LongType, StringType, CharType, BoolType, DateType)
    val types = builtinTypes.map(t => (t.sqlName, t)).toMap
    val tables = Map.empty[String, Table]

    lazy val script: Parser[Script] =
      rep(statement <~ ";") ^^ (list => Script(list: _*))

    lazy val statement: Parser[Statement] = createTableStmt | createIndexStmt | selectStmt

    lazy val createTableStmt: Parser[Statement] =
      "create" ~> "table" ~> ident ~ ("(" ~> columns <~ ")") ^^ {
        case name ~ schema => {
          val table = Table(name, schema)
          tables.update(name, table)
          CreateTableStmt(table)
        }
      }

    lazy val createIndexStmt: Parser[Statement] =
      "create" ~> "index" ~> ident ~ ("on" ~> table) ~ ("(" ~> fieldList <~ ")") ^^ {
        case name ~ table ~ key => CreateIndexStmt(name, table, key)
      }

    lazy val table: Parser[Table] =
      ident ^^ { name => if (tables.contains(name)) tables(name) else throw SqlException("Unknown table " + name) }

    lazy val columns: Parser[Schema] =
      repsep(column, ",") ^^ (list => Schema(list: _*))

    lazy val column: Parser[Column] =
      ident ~ fieldType ^^ { case i ~ t => Column(i, t)}

    lazy val selectStmt: Parser[SelectStmt] =
      selectClause ~ fromClause ~ whereClause ~ groupClause ~ orderClause ^^ {
        case p ~ s ~ f ~ g ~ o => SelectStmt(o(g(p(f(s)))))
      }

    lazy val selectClause: Parser[Operator => Operator] =
      "select" ~> ("*" ^^ { _ => (op: Operator) => op} | exprList ^^ { list => (op: Operator) => Project(op, list)})

    lazy val exprList: Parser[ExprList] =
      repsep(aliasExpr, ",") ^^ (list => ExprList(list: _*))

    lazy val fromClause: Parser[Operator] =
      "from" ~> joinClause

    lazy val whereClause: Parser[Operator => Operator] =
      opt("where" ~> expr ^^ { p => Filter(_: Operator, p)}) ^^ {
        _.getOrElse(op => op)
      }

    lazy val groupClause: Parser[Operator => Operator] =
      opt("group" ~> "by" ~> fieldList ^^ {
        p => GroupBy(_: Operator, p)
      }) ^^ {
        _.getOrElse(op => op)
      }

    lazy val orderClause: Parser[Operator => Operator] =
      opt("order" ~> "by" ~> fieldList ^^ {
        p => OrderBy(_: Operator, p)
      }) ^^ {
        _.getOrElse(op => op)
      }

    lazy val joinClause: Parser[Operator] =
      tableClause |
      joinClause ~ (opt("left") ~> opt("inner") ~> "join" ~> tableClause) ~ ("on" ~> expr) ^^ { case l ~ r ~ on => Join(l, r, on) }

    lazy val tableClause: Parser[Operator] =
      ident ^^ { name => if (!tables.contains(name)) throw SqlException("Table " + name + " not found") else Scan(tables(name)) } |
      tableClause ~ ("as" ~> ident) ^^ { case t ~ alias => TableAlias(t, alias) } |
      "(" ~> selectStmt <~ ")" ^^ { s => SubSelect(s.operator) }

    lazy val fieldType: Parser[ColumnType] = ident ^^ {t => if (!types.contains(t)) throw SqlException("Not supported type " + t) else types(t)}

    //lazy val ident: Parser[String] = """[\w]+""".r

    lazy val fieldList:  Parser[ColumnList] =
      repsep(columnRef,",") ^^ { fs => ColumnList(fs:_*) }

    lazy val columnRef: Parser[ColumnRef] =
      ident ^^ { id => ColumnRef("",id) } |
      ident ~ ("." <~ ident) ^^ { case table ~ field => ColumnRef(table, field)}

    lazy val aliasExpr: Parser[ColumnExpr] =
      expr ~ ("as" ~> ident) ^^ {
        case exp ~ alias => {
          exp.alias = alias
          exp
        }
      } |
      expr

    lazy val expr: Parser[ColumnExpr] = orExpr

    lazy val orExpr: Parser[ColumnExpr] =
      andExpr * ("or" ^^^ { (l:ColumnExpr, r:ColumnExpr) => OrExpr(l,r) } )

    lazy val andExpr: Parser[ColumnExpr] =
      cmpExpr * ("and" ^^^ { (l:ColumnExpr, r:ColumnExpr) => AndExpr(l,r) } )

    lazy val cmpExpr: Parser[ColumnExpr] =
      addExpr ~ ("=" ~> cmpExpr) ^^  { case l ~ r => EqExpr(l,r) } |
      addExpr ~ ("<>" ~> cmpExpr) ^^ { case l ~ r => NeExpr(l,r) } |
      addExpr ~ (">=" ~> cmpExpr) ^^ { case l ~ r => GeExpr(l,r) } |
      addExpr ~ (">" ~> cmpExpr) ^^  { case l ~ r => GtExpr(l,r) } |
      addExpr ~ ("<=" ~> cmpExpr) ^^ { case l ~ r => LeExpr(l,r) } |
      addExpr ~ ("<" ~> cmpExpr) ^^  { case l ~ r => LtExpr(l,r) } |
      addExpr

    lazy val addExpr: Parser[ColumnExpr] =
      mulExpr ~ ("+" ~> addExpr) ^^  { case l ~ r => AddExpr(l,r) } |
      mulExpr ~ ("-" ~> addExpr) ^^  { case l ~ r => SubExpr(l,r) } |
      mulExpr

    lazy val mulExpr: Parser[ColumnExpr] =
      unaryExpr ~ ("*" ~> mulExpr) ^^  { case l ~ r => MulExpr(l,r) } |
      unaryExpr ~ ("/" ~> mulExpr) ^^  { case l ~ r => DivExpr(l,r) } |
      unaryExpr

    lazy val unaryExpr: Parser[ColumnExpr] =
       "-" ~> unaryExpr ^^ { opd => NegExpr(opd) } |
       "not" ~> unaryExpr ^^ { opd => NotExpr(opd) } |
       "max" ~> "(" ~> expr <~ ")" ^^ { opd => MaxExpr(opd) } |
       "min" ~> "(" ~> expr <~ ")" ^^ { opd => MinExpr(opd) } |
       "sum" ~> "(" ~> expr <~ ")" ^^ { opd => SumExpr(opd) } |
       "avg" ~> "(" ~> expr <~ ")" ^^ { opd => AvgExpr(opd) } |
       "count" ~> "(" ~> "*" <~ ")" ^^ { any => CountExpr() } |
       "cast" ~> "(" ~> expr ~ ("as" ~> fieldType) <~ ")" ^^ { case exp ~ as => CastExpr(exp, as) } |
       term

    lazy val term: Parser[ColumnExpr] =
      "(" ~> expr <~ ")" |
      columnRef |
      """'[^']*'""".r ^^ { s => StrLiteral(s.drop(1).dropRight(1)) } |
      """-?\d+""".r ^^ { s => IntLiteral(s.toInt) } |
      """-?\d*\.\d+""".r ^^ { s => DoubleLiteral(s.toDouble) }

    def parseAll(input: String): Script = parseAll(script, input) match {
      case Success(res,_)  => res
      case res => throw SqlException(res.toString)
    }

  }
}
