name := "Geocoder"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "geomajas" at "http://maven.geomajas.org"

libraryDependencies ++= Seq(
  "org.geotools" % "gt-geojson" % "2.7.4",
  "org.apache.thrift"    % "libthrift"          % "0.8.0"
)
