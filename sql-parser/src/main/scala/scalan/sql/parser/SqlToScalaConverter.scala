package main.scala.scalan.sql.parser

import scalan.util.FileUtil

/**
 * Created by knizhnik on 1/17/15.
 */
object SqlToScalaConverter extends  SqlCompiler {
  val defaultTargets = Array("community-edition/src/main/scala/scalan/sql/Schema.sql")
  val packageName = "scalan.sql"

  def main(args: Array[String]): Unit = {
    val targets = if (args.isEmpty) defaultTargets else args
    for (filePath <- targets) {
      val input = FileUtil.read(filePath)
      val output =
        s"""package $packageName
           |import scalan._
           |
           |trait Queries extends ScalanDsl with SqlDsl {
           |
           |implicit class StringFormatter(str: Rep[String]) {
           |  def toDate: Rep[Int] = (str.substring(0, 4) + str.substring(5, 7) + str.substring(8, 10)).toInt
           |  def toChar: Rep[Char] = str(0)
           |}
           |
           |${generate(input)}
           |
           |}""".stripMargin;

      FileUtil.write(filePath.replace(".sql", ".scala"), output)
    }
  }
}
