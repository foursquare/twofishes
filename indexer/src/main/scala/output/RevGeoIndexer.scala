package com.foursquare.twofishes.output

import com.foursquare.twofishes.{CellGeometries, CellGeometry, Indexes, MongoGeocodeDAO, RevGeoIndexDAO, YahooWoeType}
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

  def writeIndexImpl() {
    val writer = buildMapFileWriter(
      Indexes.S2Index,
      Map(
        "minS2Level" -> minS2Level.toString,
        "maxS2Level" -> maxS2Level.toString,
        "levelMod" -> levelMod.toString
      )
    )

    val hasPolyCursor =
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
    hasPolyCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
    val idMap: Map[ObjectId, (Long, YahooWoeType)] = hasPolyCursor.map(r => {
      (r.polyId, (r._id, r.woeType))
    }).toMap

    println("did all the s2 indexing")

    val revGeoCursor = RevGeoIndexDAO.find(MongoDBObject())
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

    writer.close()
  }
}
