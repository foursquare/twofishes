package com.foursquare.twofishes.output

import com.foursquare.twofishes.mongo.MongoGeocodeDAO
import com.foursquare.twofishes.util.{DurationUtils, StoredFeatureId}
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.io._
import org.apache.hadoop.hbase.util.Bytes._
import scala.collection.mutable.HashMap
import scalaj.collection.Implicits._

class FidMap(preload: Boolean) extends DurationUtils {
  val fidMap = new HashMap[StoredFeatureId, Option[StoredFeatureId]]

  if (preload) {
    logPhase("preloading fids") {
      var i = 0
      val total = MongoGeocodeDAO.collection.count()
      val geocodeCursor = MongoGeocodeDAO.find(MongoDBObject())
      geocodeCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
      geocodeCursor.foreach(geocodeRecord => {
        geocodeRecord.featureIds.foreach(id => {
          fidMap(id) = Some(geocodeRecord.featureId)
        })
        i += 1
        if (i % (100*1000) == 0) {
          logger.info("preloaded %d/%d fids".format(i, total))
        }
      })
    }
  }

  def get(fid: StoredFeatureId): Option[StoredFeatureId] = {
    if (preload) {
      fidMap.getOrElse(fid, None)
    } else {
      if (!fidMap.contains(fid)) {
        val longidOpt = MongoGeocodeDAO.primitiveProjection[Long](
          MongoDBObject("_id" -> fid.longId), "_id")
        fidMap(fid) = longidOpt.flatMap(StoredFeatureId.fromLong _)
        if (longidOpt.isEmpty) {
          //println("missing fid: %s".format(fid))
        }
      }

      fidMap.getOrElseUpdate(fid, None)
    }
  }
}
