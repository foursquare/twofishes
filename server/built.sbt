resolvers += "geomajas" at "http://maven.geomajas.org"

resolvers += "twitter" at "http://maven.twttr.com"

libraryDependencies ++= Seq(
  "org.geotools" % "gt-geojson" % "2.7.4",
  "thrift" % "libthrift" % "0.5.0"  from  "http://maven.twttr.com/thrift/libthrift/0.5.0/libthrift-0.5.0.jar", 
  "com.twitter" % "util-core_2.9.1" % "2.0.0",
  "com.twitter" % "finagle-thrift_2.9.1" % "2.0.1",
  "org.slf4j" % "slf4j-api" % "1.6.1"
)