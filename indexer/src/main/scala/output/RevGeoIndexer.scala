package com.foursquare.twofishes.output

import com.foursquare.twofishes.{CellGeometries, CellGeometry, Indexes, YahooWoeType}
import com.foursquare.twofishes.mongo.RevGeoIndexDAO
import com.foursquare.twofishes.util.RevGeoConstants
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

class RevGeoIndexer(
  override val basepath: String,
  override val fidMap: FidMap,
  polygonMap: Map[ObjectId, List[(Long, YahooWoeType)]]
) extends Indexer with RevGeoConstants{
  val index = Indexes.S2Index
  override val outputs = Seq(index)

  lazy val writer = buildMapFileWriter(
    index,
    Map(
      "minS2Level" -> minS2LevelForRevGeo.toString,
      "maxS2Level" -> maxS2LevelForRevGeo.toString,
      "levelMod" -> defaultLevelModForRevGeo.toString
    )
  )

  def writeRevGeoIndex(
    restrict: MongoDBObject
  ) = {
    val total = RevGeoIndexDAO.count(restrict)

    val revGeoCursor = RevGeoIndexDAO.find(restrict)
      .sort(orderBy = MongoDBObject("cellid" -> 1))
    revGeoCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    var currentKey = 0L
    var currentCells = new ListBuffer[CellGeometry]
    for {
      (revgeoIndexRecord, index) <- revGeoCursor.zipWithIndex
      (geoid, woeType) <- polygonMap.getOrElse(revgeoIndexRecord.polyId, Nil)
    } {
      if (index % 10000 == 0) {
        logger.info("processed %d of %d revgeo entries for %s".format(index, total, restrict))
      }
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
    // in byte order, positives come before negative
    writeRevGeoIndex(MongoDBObject("cellid" -> MongoDBObject("$gte" -> 0)))
    writeRevGeoIndex(MongoDBObject("cellid" -> MongoDBObject("$lt" -> 0)))
    //

    writer.close()
  }
}
