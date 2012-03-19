publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

publishTo <<= "foursquare Nexus" at "http://nexus.prod.foursquare.com/nexus/content/repositories/releases/"

pomExtra := (
  <url>http://github.com/foursquare/twofish</url>
  <licenses>
    <license>
      <name>Apache</name>
      <url>http://www.opensource.org/licenses/Apache-2.0</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:foursquare/twofish.git</url>
    <connection>scm:git:git@github.com:foursquare/twofish.git</connection>
  </scm>
  <developers>
    <developer>
      <id>blackmad</id>
      <name>David Blackman</name>
      <url>http://github.com/blackmad</url>
    </developer>
  </developers>)

scalacOptions ++= Seq("-deprecation", "-unchecked")

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
