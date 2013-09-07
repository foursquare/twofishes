import sbt._

object Plugins extends Build {
  lazy val root = Project("root", file(".")) dependsOn(
    uri("http://github.com/foursquare/spindle.git"),
    uri("http://github.com/sbt/sbt-assembly.git#0.9.2")
  )
}

