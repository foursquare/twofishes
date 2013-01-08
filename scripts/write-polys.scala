import com.vividsolutions.jts.io.{WKBWriter, WKTReader}  
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.global._
import com.novus.salat.annotations._
import com.novus.salat.dao._

  val polygonDirs = List(
    new File("data/computed/polygons"),
    new File("data/private/polygons")
  )
  val polygonFiles = polygonDir.toList.flatMap(_.listFiles.toList.sorted)
  val polygonTable: Map[String, String] = polygonFiles.flatMap(f => {
    scala.io.Source.fromFile(f).getLines.filterNot(_.startsWith("#")).toList.map(l => {
      val parts = l.split("\t")
      (parts(0) -> parts(1))
    })  
  }).toMap

      val wktReader = new WKTReader()
      val wkbWriter = new WKBWriter()


  def addPolygonToRecord(id: String, wkt: String) {
      println(wkt)
      val geom = wktReader.read(wkt)
      val wkb = wkbWriter.write(geom)

    MongoGeocodeDAO.update(MongoDBObject("ids" -> id),
      MongoDBObject("$set" -> MongoDBObject("polygon" -> wkb)),
      false, false)
    MongoGeocodeDAO.update(MongoDBObject("ids" -> id),
      MongoDBObject("$set" -> MongoDBObject("hasPoly" -> true)),
      false, false)
  }

  for { (id, poly) <- polygonTable} { addPolygonToRecord(id, poly) }

// vim: set ts=4 sw=4 et:
