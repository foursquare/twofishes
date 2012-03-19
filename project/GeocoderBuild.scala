import sbt._
import Keys._

object GeocoderBuild extends Build {
  lazy val buildSettings = Seq(
    organization := "com.foursquare.geocoder",
    version      := "0.1",
    scalaVersion := "2.9.1"
  )

  lazy val defaultSettings = super.settings ++ buildSettings ++ Defaults.defaultSettings ++ Seq(
    resolvers += "geomajas" at "http://maven.geomajas.org",
    resolvers += "twitter" at "http://maven.twttr.com",
    resolvers += "repo.novus rels" at "http://repo.novus.com/releases/",
    resolvers += "repo.novus snaps" at "http://repo.novus.com/snapshots/",
    resolvers += "Java.net Maven 2 Repo" at "http://download.java.net/maven/2",
    resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                      "releases"  at "http://oss.sonatype.org/content/repositories/releases")
  )

  lazy val core = Project(id = "core",
      base = file("core"),
      settings = defaultSettings ++ Seq(
        libraryDependencies ++= Seq(
          "com.twitter" % "util-core_2.9.1" % "2.0.0",
          "com.twitter" % "util-logging_2.9.1" % "2.0.0",
          "org.slf4j" % "slf4j-api" % "1.6.1",
          "com.novus" % "salat-core_2.9.1" % "0.0.8-SNAPSHOT"
        )
      )
    ) dependsOn(interface)

  lazy val interface = Project(id = "interface",
      settings = defaultSettings ++ Seq(
        libraryDependencies ++= Seq(
          "com.twitter" % "finagle-thrift_2.9.1" % "2.0.1",
          "org.slf4j" % "slf4j-api" % "1.6.1"
        )
      ),
      base = file("interface"))

  lazy val server = Project(id = "server",
      settings = defaultSettings ++ Seq(
        libraryDependencies ++= Seq(
          "com.twitter" % "finagle-http_2.9.1" % "2.0.1",
          "org.specs2" %% "specs2" % "1.8.2" % "test",
          "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
        )
      ),
      base = file("server")) dependsOn(core, interface)

  lazy val indexer = Project(id = "indexer",
      base = file("indexer"),
      settings = defaultSettings ++ Seq(
        libraryDependencies ++= Seq(
          "com.twitter" % "util-core_2.9.1" % "2.0.0",
          "com.twitter" % "util-logging_2.9.1" % "2.0.0",
          "com.novus" % "salat-core_2.9.1" % "0.0.8-SNAPSHOT"
        )
      )
  ) dependsOn(core)
}
