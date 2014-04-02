package com.foursquare.twofishes.output

import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util.{GeometryUtils, StoredFeatureId}
import com.google.common.geometry.S2CellId
import com.mongodb.casbah.Imports._
import com.mongodb.Bytes
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.util.concurrent.CountDownLatch
import com.twitter.util.Duration
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import java.io._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.{TwofishesFoursquareCacheConfig, Compression, HFile}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.hadoop.io.{BytesWritable, MapFile}
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TCompactProtocol
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scalaj.collection.Implicits._
import com.weiglewilczek.slf4s.Logging
import akka.actor.ActorSystem
import akka.actor.Props
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

class FidMap(preload: Boolean) extends DurationUtils {
  val fidMap = new HashMap[StoredFeatureId, Option[StoredFeatureId]]

  if (preload) {
    logDuration("preloading fids") {
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
          MongoDBObject("ids" -> fid.longId), "_id")
        fidMap(fid) = longidOpt.flatMap(StoredFeatureId.fromLong _)
        if (longidOpt.isEmpty) {
          //println("missing fid: %s".format(fid))
        }
      }

      fidMap.getOrElseUpdate(fid, None)
    }
  }
}