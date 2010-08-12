import sbt._
class Plugins(info: ProjectInfo) extends PluginDefinition(info)
{
  val eclipse = "de.element34" % "sbt-eclipsify" % "0.5.4"
}