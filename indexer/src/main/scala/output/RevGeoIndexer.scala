package com.foursquare.twofishes.output

import com.foursquare.twofishes.{CellGeometries, CellGeometry, Indexes, YahooWoeType}
import com.foursquare.twofishes.mongo.{MongoGeocodeDAO, RevGeoIndexDAO}
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.io._
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.util.Bytes._
import scala.collection.mutable.ListBuffer
import scalaj.collection.Implicits._


class RevGeoIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  val minS2Level = 8
  val maxS2Level = 12
  val maxCells = 10000
  val levelMod = 2

  val index = Indexes.S2Index
  override val outputs = Seq(index)

  lazy val writer = buildMapFileWriter(
    index,
    Map(
      "minS2Level" -> minS2Level.toString,
      "maxS2Level" -> maxS2Level.toString,
      "levelMod" -> levelMod.toString
    )
  )

  def writeRevGeoIndex(
    idMap: Map[ObjectId, (Long, YahooWoeType)],
    restrict: MongoDBObject
  ) = {
    val revGeoCursor = RevGeoIndexDAO.find(restrict)
      .sort(orderBy = MongoDBObject("cellid" -> 1))
    revGeoCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    var currentKey = 0L
    var currentCells = new ListBuffer[CellGeometry]
    for {
      revgeoIndexRecord <- revGeoCursor
      (geoid, woeType) <- idMap.get(revgeoIndexRecord.polyId)
    } {
      if (currentKey != revgeoIndexRecord.cellid) {
        if (currentKey != 0L) {
          writer.append(currentKey, CellGeometries(currentCells))
        }
        currentKey = revgeoIndexRecord.cellid
        currentCells.clear
      }
      val builder = CellGeometry.newBuilder
        .woeType(woeType)
        .longId(geoid)

      if (revgeoIndexRecord.full) {
        builder.full(true)
      } else {
        builder.wkbGeometry(revgeoIndexRecord.geom.map(ByteBuffer.wrap))
      }
      currentCells.append(builder.result)
    }

    writer.append(currentKey, CellGeometries(currentCells))

  }

  def writeIndexImpl() {
    val hasPolyCursor =
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
    hasPolyCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
    val idMap: Map[ObjectId, (Long, YahooWoeType)] = hasPolyCursor.map(r => {
      (r.polyId, (r._id, r.woeType))
    }).toMap

    // in byte order, positives come before negative
    writeRevGeoIndex(idMap, MongoDBObject("cellid" -> MongoDBObject("$gte" -> 0)))
    writeRevGeoIndex(idMap, MongoDBObject("cellid" -> MongoDBObject("$lt" -> 0)))
    //

    writer.close()
  }
}
