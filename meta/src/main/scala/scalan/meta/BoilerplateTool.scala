package scalan.meta

object StarterBoilerplateTool extends BoilerplateTool {
  val starterTypeSynonims = Map(
    "RThrow" -> "Throwable",
    "Arr" -> "Array"
  )
  lazy val starterConfig = CodegenConfig(
    name = "SqlMeta",
    srcPath = "community-edition/src/main/scala",
    entityFiles = List(
      "scalan/sql/Database.scala",
      "scalan/sql/Sql.scala",
      "scalan/sql/Tables.scala"),
    baseContextTrait = "Scalan",
    seqContextTrait = "ScalanSeq",
    stagedContextTrait = "ScalanExp",
    extraImports = List(
      "scala.reflect.runtime.universe._",
      "scalan.common.Default"),
    starterTypeSynonims
  )

  override def getConfigs(args: Array[String]) = Seq(starterConfig)

  override def main(args: Array[String]) = super.main(args)
}
