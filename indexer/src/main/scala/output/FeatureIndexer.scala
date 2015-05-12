package com.foursquare.twofishes.output

import com.foursquare.twofishes.{GeocodeRecord, GeocodeServingFeature, Indexes, YahooWoeType}
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.mongo.{MongoGeocodeDAO, PolygonIndex, PolygonIndexDAO, RevGeoIndexDAO}
import com.foursquare.twofishes.util.{GeoTools, GeometryUtils, StoredFeatureId}
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

class FeatureIndexer(
  override val basepath: String, 
  override val fidMap: FidMap,
  polygonMap: Map[ObjectId, List[(Long, YahooWoeType)]]
) extends Indexer {
  def canonicalizeParentId(fid: StoredFeatureId) = fidMap.get(fid)

  val index = Indexes.FeatureIndex
  override val outputs = Seq(index)

  def makeGeocodeRecordWithoutGeometry(g: GeocodeRecord, poly: Option[PolygonIndex]): GeocodeServingFeature = {
    val fullFeature = poly.map(p => 
        g.copy(
          polygon = Some(p.polygon), 
          polygonSource = Some(p.source))
        ).getOrElse(g).toGeocodeServingFeature()

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

  val wkbReader = new WKBReader()
  def makeGeocodeServingFeature(f: GeocodeServingFeature) = {
    var parents = (for {
      parentLongId <- f.scoringFeatures.parentIds
      parentFid <- StoredFeatureId.fromLong(parentLongId)
      parentId <- canonicalizeParentId(parentFid)
    } yield {
      parentFid
    }).map(_.longId)

    if (f.scoringFeatures.parentIds.isEmpty &&
        f.feature.woeType !=? YahooWoeType.COUNTRY) {
      // take the center and reverse geocode it against the revgeo index!
      val geom = GeoTools.pointToGeometry(f.feature.geometryOrNull.center)
      val cells: Seq[Long] = GeometryUtils.s2PolygonCovering(geom).map(_.id)

      // now for each cell, find the matches in our index
      val candidates = RevGeoIndexDAO.find(MongoDBObject("cellid" -> MongoDBObject("$in" -> cells)))

      // for each candidate, check if it's full or we're in it
      val matches = (for {
        revGeoCell <- candidates
        fidLong <- polygonMap.getOrElse(revGeoCell.polyId, Nil)
        if (revGeoCell.full || revGeoCell.geom.exists(geomBytes =>
          wkbReader.read(geomBytes).contains(geom)))
      } yield { fidLong }).toList

      parents = matches.map(_._1)
    }

    f.copy(
      scoringFeatures = f.scoringFeatures.copy(parentIds = parents)
    )
  }

  def writeIndexImpl() {
    val writer = buildMapFileWriter(index, indexInterval = Some(2))
    var fidCount = 0
    val fidSize = MongoGeocodeDAO.collection.count()
    val fidCursor = MongoGeocodeDAO.find(MongoDBObject())
      .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc
    fidCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    for {
      gCursor <- fidCursor.grouped(1000)
      group = gCursor.toList
      toFindPolys: Map[Long, ObjectId] = group.filter(f => f.hasPoly).map(r => (r._id, r.polyId)).toMap
      polyMap: Map[ObjectId, PolygonIndex] =
        PolygonIndexDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> toFindPolys.values.toList)))
          .toList
          .groupBy(_._id).map({case (k, v) => (k, v(0))})
      f <- group
    } {
      val polyOpt = polyMap.get(f.polyId)
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
