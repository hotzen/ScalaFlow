import sbt._
import de.element34.sbteclipsify._

class ScalaFlowProject(info: ProjectInfo) extends DefaultProject(info) with AutoCompilerPlugins with Eclipsify
{
  val cps = compilerPlugin("org.scala-lang.plugins" % "continuations" % "2.8.0")
  override def compileOptions = super.compileOptions ++ Seq(Deprecation, Unchecked /*, Verbose, ExplainTypes*/) ++ compileOptions("-P:continuations:enable")
}