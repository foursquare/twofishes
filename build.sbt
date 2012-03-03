name := "Geocoder"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "geomajas" at "http://maven.geomajas.org"

resolvers += "twitter" at "http://maven.twttr.com"

resolvers += "repo.novus rels" at "http://repo.novus.com/releases/"

resolvers += "repo.novus snaps" at "http://repo.novus.com/snapshots/"

libraryDependencies ++= Seq(
  "org.geotools" % "gt-geojson" % "2.7.4",
  "thrift" % "libthrift" % "0.5.0"  from  "http://maven.twttr.com/thrift/libthrift/0.5.0/libthrift-0.5.0.jar", 
  "com.twitter" % "util-core_2.9.1" % "2.0.0",
  "com.twitter" % "finagle-thrift_2.9.1" % "2.0.1",
  "org.slf4j" % "slf4j-api" % "1.6.1",
  "com.novus" % "salat-core_2.9.1" % "0.0.8-SNAPSHOT" 
)
