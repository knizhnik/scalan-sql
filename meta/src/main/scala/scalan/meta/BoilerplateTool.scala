package scalan.meta

object StarterBoilerplateTool extends BoilerplateTool {
  val starterTypeSynonims = Set(
    "My"
    // declare your type synonims for User Defined types here (see type PA[A] = Rep[PArray[A]])
  )
  lazy val starterConfig = CodegenConfig(
    srcPath = "src/main/scala",
    entityFiles = List(
      "scalan/examples/MyArrays.scala"
    ),
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
