package com.foursquare.twofishes.output

import com.foursquare.twofishes.{GeocodeRecord, GeocodeServingFeature, Indexes, MongoGeocodeDAO, PolygonIndex,
    PolygonIndexDAO}
import com.foursquare.twofishes.util.StoredFeatureId
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.io._
import org.apache.hadoop.hbase.util.Bytes._
import scalaj.collection.Implicits._

class FeatureIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  def canonicalizeParentId(fid: StoredFeatureId) = fidMap.get(fid)

  def makeGeocodeRecordWithoutGeometry(g: GeocodeRecord, poly: Option[PolygonIndex]): GeocodeServingFeature = {
    val fullFeature = poly.map(p => g.copy(polygon = Some(p.polygon)))
      .getOrElse(g).toGeocodeServingFeature()

    val partialFeature = fullFeature.copy(
      feature = fullFeature.feature.copy(
        geometry = fullFeature.feature.geometry.copy(wkbGeometry = null)
      )
    )

    makeGeocodeServingFeature(partialFeature)
  }

  def makeGeocodeRecord(g: GeocodeRecord) = {
    makeGeocodeServingFeature(g.toGeocodeServingFeature())
  }

  def makeGeocodeServingFeature(f: GeocodeServingFeature) = {
    val parents = for {
      parentLongId <- f.scoringFeatures.parentIds
      parentFid <- StoredFeatureId.fromLong(parentLongId)
      parentId <- canonicalizeParentId(parentFid)
    } yield {
      parentFid
    }

    f.copy(
      scoringFeatures = f.scoringFeatures.copy(parentIds = parents.map(_.longId))
    )
  }

  def writeFeatures() {
    val writer = buildMapFileWriter(Indexes.FeatureIndex, indexInterval = Some(2))
    var fidCount = 0
    val fidSize = MongoGeocodeDAO.collection.count()
    val fidCursor = MongoGeocodeDAO.find(MongoDBObject())
      .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc
    fidCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    for {
      gCursor <- fidCursor.grouped(1000)
      group = gCursor.toList
      toFindPolys: Map[Long, ObjectId] = group.filter(f => f.hasPoly).map(r => (r._id, r.polyId)).toMap
      polyMap: Map[ObjectId, PolygonIndex] = PolygonIndexDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> toFindPolys.values)))
        .toList
        .groupBy(_._id).map({case (k, v) => (k, v(0))})
      f <- group
      polyOpt = polyMap.get(f.polyId)
    } {
      writer.append(
        f.featureId, makeGeocodeRecordWithoutGeometry(f, polyOpt))
      fidCount += 1
      if (fidCount % 100000 == 0) {
        logger.info("processed %d of %d features".format(fidCount, fidSize))
      }
    }
    writer.close()
  }
}