import net.virtualvoid.sbt.graph.Plugin.graphSettings
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._

object GeocoderBuild extends Build {
  lazy val buildSettings = Seq(
    organization := "com.foursquare.twofishes",
    name := "twofishes",
    version      := "0.90.0",
    scalaVersion := "2.11.4",
    crossScalaVersions := Seq("2.10.2", "2.11.4"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    javacOptions in doc := Seq("-source", "1.6")
  )

  lazy val scoptSettings = Seq(
    libraryDependencies += "com.github.scopt" %% "scopt" % "3.1.0"
  )

  lazy val specsSettings = Seq(
      libraryDependencies <<= (scalaVersion, libraryDependencies) {(version, dependencies) =>
        val specs2 =
          if (version.startsWith("2.10"))
            "org.specs2" %% "specs2" % "1.14" % "test"
          else if (version == "2.9.3")
            "org.specs2" %% "specs2" % "1.12.4.1" % "test"
          else if (version.startsWith("2.11"))
            "org.specs2" %% "specs2-core" % "2.4.17" % "test"
          else
            "org.specs2" %% "specs2" % "1.12.3" % "test"
        dependencies :+ specs2
      }
  )

  lazy val defaultSettings = super.settings ++ buildSettings ++ Defaults.defaultSettings ++ Seq(
    resolvers += "geomajas" at "http://maven.geomajas.org",
    resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools/",
    resolvers += "twitter" at "http://maven.twttr.com",
    resolvers += "repo.novus rels" at "http://repo.novus.com/releases/",
    resolvers += "repo.novus snaps" at "http://repo.novus.com/snapshots/",
    resolvers += "Java.net Maven 2 Repo" at "http://download.java.net/maven/2",
    resolvers += "apache" at "http://repo2.maven.org/maven2/org/apache/hbase/hbase/",
    resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    resolvers += "codahale" at "http://repo.codahale.com",
    resolvers += "springsource" at "http://repo.springsource.org/libs-release-remote",
    resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo",
    resolvers += "sonatype maven repo" at "http://oss.sonatype.org/content/repositories/releases/",

    fork in run := true,
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishArtifact := false,
    pomIncludeRepository := { _ => false },
    publishTo <<= (version) { v =>
      val publishTo = System.getenv("publishTo")
      if (publishTo != null && publishTo.contains("sona")) {
       val nexus = "https://oss.sonatype.org/"
        if (v.endsWith("-SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
      } else {
        Some("foursquare Nexus" at "http://nexus.prod.foursquare.com/nexus/content/repositories/releases/")
      }
    },

    pomExtra := (
      <url>http://github.com/foursquare/twofishes</url>
      <licenses>
        <license>
          <name>Apache</name>
          <url>http://www.opensource.org/licenses/Apache-2.0</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:foursquare/twofishes.git</url>
        <connection>scm:git:git@github.com:foursquare/twofishes.git</connection>
      </scm>
      <developers>
        <developer>
          <id>blackmad</id>
          <name>David Blackman</name>
          <url>http://github.com/blackmad</url>
        </developer>
      </developers>),

    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
  )

  lazy val all = Project(id = "all",
    settings = defaultSettings ++ assemblySettings ++ Seq(
      publishArtifact := true
    ),
    base = file(".")) aggregate(countryinfo, util, core, interface, server, indexer)

  lazy val core = Project(id = "core",
      base = file("core"),
      settings = defaultSettings ++ specsSettings ++ Seq(
        publishArtifact := true,
        libraryDependencies ++= Seq(
          "com.twitter" %% "util-core" % "6.3.0",
          "com.twitter" %% "util-logging" % "6.3.0",
          "org.slf4j" % "slf4j-api" % "1.6.1",
          "org.apache.avro" % "avro" % "1.7.1.cloudera.2",
          //"org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.4.0" intransitive(),
          "org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.4.0",
          "org.apache.hbase" % "hbase" % "0.94.6-cdh4.4.0" intransitive(),
          "com.google.guava" % "guava" % "r09",
          "commons-cli" % "commons-cli" % "1.2",
          "commons-logging" % "commons-logging" % "1.1.1",
          "commons-daemon" % "commons-daemon" % "1.0.9",
          "commons-configuration" % "commons-configuration" % "1.6"
        ),
        ivyXML := (
          <dependencies>
            <exclude org="thrift"/>
            <exclude org="com.twitter" module="finagle-core"/>
	          <exclude org="org.scalaj" module="scalaj-collection_2.9.1"/>
            <exclude org="org.apache.thrift" module="thrift"/>
            <exclude org="commons-beanutils" module="commons-beanutils"/>
            <exclude org="commons-beanutils" module="commons-beanutils-core"/>
            <exclude org="org.mockito" module="mockito-all"/>
            <exclude org="tomcat" module="jasper-runtime"/>
          </dependencies>
        )
      )
    ) dependsOn(interface, util, countryinfo)

  lazy val interface = Project(id = "interface",
      settings = defaultSettings ++ Seq(
        publishArtifact := true,
        libraryDependencies ++= Seq(
          "org.slf4j" % "slf4j-api" % "1.6.1"
        )
      ),
      base = file("interface"))

  lazy val server = Project(id = "server",
      settings = graphSettings ++ scoptSettings ++ defaultSettings ++ assemblySettings ++ specsSettings ++ Seq(
        mainClass in assembly := Some("com.foursquare.twofishes.GeocodeFinagleServer"),
        initialCommands := """
        import com.foursquare.twofishes._
        import com.foursquare.twofishes.util._

        val config: GeocodeServerConfig = GeocodeServerConfigSingleton.init(args)
        val store = ServerStore.getStore(config)
        val server = new GeocodeServerImpl(store, false)
        """,
        baseDirectory in run := file("."),
        publishArtifact := true,
        libraryDependencies ++= Seq(
          "com.foursquare" %% "country_revgeo" % "0.1a",
          "com.twitter" %% "ostrich" % "9.1.0",
          "com.twitter" %% "finagle-http" % "6.3.0",
          "com.vividsolutions" % "jts" % "1.13"
        ),
        ivyXML := (
          <dependencies>
            <exclude org="com.twitter" module="finagle-core"/>
            <exclude org="org.scalaj" module="scalaj-collection_2.9.1"/>
            <exclude org="org.mongodb" module="bson"/>
            <exclude org="org.slf4j" module="slf4j-log4j12"/>
          </dependencies>
        ),
        mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
          case entry => {
            val strategy = mergeStrategy(entry)
            if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
            else strategy
          }
        }}
      ),
      base = file("server")) dependsOn(core, interface, util, countryinfo)

  lazy val indexer = Project(id = "indexer",
      base = file("indexer"),
      settings = defaultSettings ++ assemblySettings ++ scoptSettings ++ specsSettings ++
        net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq(
        baseDirectory in run := file("."),
        initialCommands := """
        import com.foursquare.twofishes._
        import com.foursquare.twofishes.importers.geonames._
        import com.foursquare.twofishes.mongo._
        import com.foursquare.twofishes.output._
        import com.foursquare.twofishes.util.Helpers._
        import com.foursquare.twofishes.util._
        import com.mongodb.casbah.Imports._
        import com.novus.salat._
        import com.novus.salat.annotations._
        import com.novus.salat.dao._
        import com.novus.salat.global._
        import com.vividsolutions.jts.io._

        val store = new MongoGeocodeStorageService()
        val slugIndexer = new SlugIndexer()
        GeonamesParser.config = GeonamesImporterConfigParser.parse(
          Array("--hfile_basepath", ".", "--output_revgeo_index", "true")
        )
        val parser = new GeonamesParser(store, slugIndexer)

        GeonamesParser.parseAdminInfoFile("data/downloaded/adminCodes.txt")
        val polygonLoader = new PolygonLoader(parser, store, GeonamesParser.config)
        """,

        publishArtifact := false,
        libraryDependencies ++= Seq(
          "com.foursquare" %% "country_revgeo" % "0.1a",
          "com.novus" %% "salat" % "1.9.6",
          "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.3",
          "org.json4s" %% "json4s-native" % "3.2.8",
          "org.json4s" %% "json4s-jackson" % "3.2.8",
          "com.typesafe.akka" %% "akka-actor" % "2.3.1",
          // "backtype" % "cascading-thrift" % "0.3.0-SNAPSHOT",
          "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.4.0",
          "com.twitter" %% "scalding-core" % "0.12.0"
        ),
        mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
          case entry => {
            val strategy = mergeStrategy(entry)
            if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
            else strategy
          }
        }}
      )
  ) dependsOn(core, util, countryinfo)

   lazy val countryinfo = Project(id = "countryinfo",
      base = file("countryinfo"),
      settings = defaultSettings ++ assemblySettings ++ specsSettings ++ Seq(
        organization := "com.foursquare",
        publishArtifact := true
      )
    )

  val geoToolsVersion = "9.2"

  lazy val util = Project(id = "util",
      base = file("util"),
      settings = defaultSettings ++ assemblySettings ++ specsSettings ++ Seq(
        publishArtifact := true,
        libraryDependencies ++= Seq(
          "com.twitter" %% "ostrich" % "9.1.3",
          "com.google.caliper" % "caliper" % "0.5-rc1",
          "org.geotools" % "gt-shapefile" % geoToolsVersion,
          "org.geotools" % "gt-geojson" % geoToolsVersion,
          "org.geotools" % "gt-epsg-hsql" % geoToolsVersion,
          "org.geotools" % "gt-epsg-extension" % geoToolsVersion,
          "org.geotools" % "gt-referencing" % geoToolsVersion,
          "org.scalaj" %% "scalaj-collection" % "1.5",
          "org.mongodb" % "mongo-java-driver" % "2.9.3",
          "com.weiglewilczek.slf4s" % "slf4s_2.9.1" % "1.0.7",
          "ch.qos.logback" % "logback-classic" % "1.0.9",
          "com.ibm.icu" % "icu4j" % "49.1"
        ),
        ivyXML := (
          <dependencies>
            <exclude org="org.slf4j" module="slf4j-log4j12"/>
          </dependencies>
        )
      )
    ) dependsOn(interface)

  lazy val replayer = Project(id = "replayer",
      settings = scoptSettings ++ defaultSettings ++ assemblySettings ++ specsSettings ++ Seq(
        mainClass in assembly := Some("com.foursquare.twofishes.replayer.GeocoderReplayerClient"),
        libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.2"
      ),
      base = file("replayer")) dependsOn(core, interface, util)
}
