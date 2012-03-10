name := "Geocoder"

version := "0.1"

scalaVersion := "2.9.1"

scalacOptions += "-deprecation"

resolvers += "geomajas" at "http://maven.geomajas.org"

resolvers += "twitter" at "http://maven.twttr.com"

resolvers += "repo.novus rels" at "http://repo.novus.com/releases/"

resolvers += "repo.novus snaps" at "http://repo.novus.com/snapshots/"

resolvers += "Java.net Maven 2 Repo" at "http://download.java.net/maven/2"


resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "releases"  at "http://oss.sonatype.org/content/repositories/releases")

libraryDependencies ++= Seq(
  "com.codahale" % "jerkson_2.9.1" % "0.5.0",
  "thrift" % "libthrift" % "0.5.0"  from  "http://maven.twttr.com/thrift/libthrift/0.5.0/libthrift-0.5.0.jar", 
  "com.twitter" % "util-core_2.9.1" % "2.0.0",
  "com.twitter" % "finagle-thrift_2.9.1" % "2.0.1",
  "com.twitter" % "finagle-http_2.9.1" % "2.0.1",
  "org.slf4j" % "slf4j-api" % "1.6.1",
  "com.novus" % "salat-core_2.9.1" % "0.0.8-SNAPSHOT",
  "org.specs2" %% "specs2" % "1.8.2" % "test",
  "org.scala-tools.testing" %% "specs" % "1.6.9" % "test"
)