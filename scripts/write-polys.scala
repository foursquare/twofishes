import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}

    val polygonTable: Map[String, Geometry] = PolygonLoader.load() 

      val wktReader = new WKTReader()
      val wkbWriter = new WKBWriter()


  def addPolygonToRecord(id: String, geom: Geometry) {
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
