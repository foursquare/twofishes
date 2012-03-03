import sbt._
import Keys._

object GeocoderBuild extends Build {
  override lazy val projects = Seq(all, indexer, server)

  lazy val all: Project = Project("all", file(".")) aggregate(indexer)

  lazy val indexer = Project("indexer", file("indexer/"))

  lazy val server = Project("server", file("server/"))
}
