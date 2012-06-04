import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object GeocoderBuild extends Build {
  lazy val buildSettings = Seq(
    organization := "com.foursquare.twofishes",
    name := "twofishes",
    version      := "0.25",
    scalaVersion := "2.9.1"
  )

  lazy val defaultSettings = super.settings ++ buildSettings ++ Defaults.defaultSettings ++ Seq(
    resolvers += "geomajas" at "http://maven.geomajas.org",
    resolvers += "twitter" at "http://maven.twttr.com",
    resolvers += "repo.novus rels" at "http://repo.novus.com/releases/",
    resolvers += "repo.novus snaps" at "http://repo.novus.com/snapshots/",
    resolvers += "Java.net Maven 2 Repo" at "http://download.java.net/maven/2",
    resolvers += "apache" at "http://repo2.maven.org/maven2/org/apache/hbase/hbase/",
    resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                      "releases"  at "http://oss.sonatype.org/content/repositories/releases"),

    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    publishTo := Some("foursquare Nexus" at "http://nexus.prod.foursquare.com/nexus/content/repositories/releases/"),
    Keys.publishArtifact in (Compile, Keys.packageDoc) := false,

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

    credentials ++= {
      val sonatype = ("Sonatype Nexus Repository Manager", "repo.foursquare.com")
      def loadMavenCredentials(file: java.io.File) : Seq[Credentials] = {
        xml.XML.loadFile(file) \ "servers" \ "server" map (s => {
          val host = (s \ "id").text
          val realm = if (host == sonatype._2) sonatype._1 else "Unknown"
          Credentials(realm, host, (s \ "username").text, (s \ "password").text)
        })  
      }   
      val ivyCredentials   = Path.userHome / ".ivy2" / ".credentials"
      val mavenCredentials = Path.userHome / ".m2"   / "settings.xml"
      (ivyCredentials.asFile, mavenCredentials.asFile) match {
        case (ivy, _) if ivy.canRead => Credentials(ivy) :: Nil
        case (_, mvn) if mvn.canRead => loadMavenCredentials(mvn)
        case _ => Nil
      }
    }
  )

  lazy val all = Project(id = "all",
    settings = defaultSettings ++ assemblySettings ++ Seq(
      publishArtifact := true
    ),
    base = file(".")) aggregate(core, interface, server, indexer)

  lazy val core = Project(id = "core",
      base = file("core"),
      settings = defaultSettings ++ Seq(
        publishArtifact := true,
        libraryDependencies ++= Seq(
          "com.twitter" % "util-core_2.9.1" % "1.12.8",
          "com.twitter" % "util-logging_2.9.1" % "1.12.8",
          "org.slf4j" % "slf4j-api" % "1.6.1",
          "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u3" intransitive(),
          "org.apache.hbase" % "hbase" % "0.92.1" intransitive(),
          "com.novus" % "salat-core_2.9.1" % "0.0.8-SNAPSHOT",
          "org.apache.thrift" % "libthrift" % "0.8.0"
          // "thrift" % "libthrift" % "0.5.0" from "http://maven.twttr.com/org/apache/thrift/libthrift/0.5.0/libthrift-0.5.0.jar"
        ),
        ivyXML := (
          <dependencies>
            <exclude org="thrift"/>
          </dependencies>
        )
      )
    ) dependsOn(interface)

  lazy val interface = Project(id = "interface",
      settings = defaultSettings ++ Seq(
        publishArtifact := true,
        libraryDependencies ++= Seq(
          "thrift" % "libthrift" % "0.5.0" from "http://maven.twttr.com/org/apache/thrift/libthrift/0.5.0/libthrift-0.5.0.jar",
          "com.twitter" % "finagle-thrift_2.9.1" % "1.9.12",
          "org.slf4j" % "slf4j-api" % "1.6.1"
        )
      ),
      base = file("interface"))

  lazy val server = Project(id = "server",
      settings = defaultSettings ++ assemblySettings ++ Seq(
        mainClass in assembly := Some("com.foursquare.twofishes.GeocodeFinagleServer"),
        publishArtifact := true,
        libraryDependencies ++= Seq(
          "com.twitter" % "finagle-http_2.9.1" % "1.9.12",
          "org.specs2" %% "specs2" % "1.8.2" % "test",
          "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
          "com.github.scopt" %% "scopt" % "2.0.0"        )
      ),
      base = file("server")) dependsOn(core, interface)

  lazy val indexer = Project(id = "indexer",
      base = file("indexer"),
      settings = defaultSettings ++ assemblySettings ++ Seq(
        mainClass in assembly := Some("com.foursquare.twofishes.importers.geonames.GeonamesParser"),
        initialCommands := """
        import com.foursquare.twofishes.importers.geonames._
        import com.foursquare.twofishes._
        import com.foursquare.twofishes.Helpers._
        import com.foursquare.twofishes.Implicits._
        import java.io.File

        val store = new MongoGeocodeStorageService()
        val parser = new GeonamesParser(store)
        """,

        publishArtifact := false,
        libraryDependencies ++= Seq(
          "org.specs2" %% "specs2" % "1.8.2" % "test",
          "com.twitter" % "util-core_2.9.1" % "1.12.8",
          "com.twitter" % "util-logging_2.9.1" % "1.12.8",
          "com.novus" % "salat-core_2.9.1" % "0.0.8-SNAPSHOT",
          "com.github.scopt" %% "scopt" % "2.0.0"
        )
      )
  ) dependsOn(core)
}
