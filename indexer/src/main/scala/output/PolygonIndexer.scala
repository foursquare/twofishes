
package com.foursquare.twofishes.output

import com.foursquare.twofishes.Indexes
import com.foursquare.twofishes.mongo.{MongoGeocodeDAO, PolygonIndex, PolygonIndexDAO}
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import com.vividsolutions.jts.io.WKBReader
import java.io._
import org.apache.hadoop.hbase.util.Bytes._
import scalaj.collection.Implicits._

class PolygonIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  val index = Indexes.GeometryIndex
  override val outputs = Seq(index)

  def writeIndexImpl() {
    val polygonSize = PolygonIndexDAO.collection.count()
    val usedPolygonSize = MongoGeocodeDAO.count(MongoDBObject("hasPoly" -> true))

    val hasPolyCursor =
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
        .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc
    hasPolyCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    val writer = buildMapFileWriter(index)

    val wkbReader = new WKBReader()

    var numUsedPolygon = 0
    val groupSize = 1000
    // would be great to unify this with featuresIndex
    for {
      (g, groupIndex) <- hasPolyCursor.grouped(groupSize).zipWithIndex
      group = g.toList
      toFindPolys: Map[Long, ObjectId] = group.filter(f => f.hasPoly).map(r => (r._id, r.polyId)).toMap
      polyMap: Map[ObjectId, PolygonIndex] = PolygonIndexDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> toFindPolys.values.toList)))
        .toList
        .groupBy(_._id).map({case (k, v) => (k, v(0))})
      (f, polygonIndex) <- group.zipWithIndex
      poly <- polyMap.get(f.polyId)
    } {
      if (polygonIndex == 0) {
        logger.info("PolygonIndexer: outputted %d of %d used polys, %d of %d total polys seen".format(
          numUsedPolygon, usedPolygonSize, polygonSize, groupIndex*groupSize))
      }
      numUsedPolygon += 1
      writer.append(f.featureId, wkbReader.read(poly.polygon))
    }
    writer.close()

    logger.info("done")
  }
}
