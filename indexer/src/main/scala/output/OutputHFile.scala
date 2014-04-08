package com.foursquare.twofishes.output

import com.foursquare.twofishes.{SlugEntry, SlugEntryMap}
import com.foursquare.twofishes.mongo.MongoGeocodeDAO
import com.foursquare.twofishes.util.DurationUtils
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.io._
import java.util.concurrent.CountDownLatch
import org.apache.hadoop.hbase.util.Bytes._
import scala.collection.mutable.HashMap
import scalaj.collection.Implicits._
import com.twitter.util.{Future, FuturePool}
import java.util.concurrent.Executors

class OutputIndexes(
  basepath: String,
  outputPrefixIndex: Boolean = true,
  slugEntryMap: SlugEntryMap.SlugEntryMap = HashMap.empty,
  outputRevgeo: Boolean = true
) extends DurationUtils {
  def buildIndexes(revgeoLatch: Option[CountDownLatch]) {
    val fidMap = logPhase("preload fid map") { new FidMap(preload = true) }

    // This one wastes a lot of ram, so do it on it's own
    (new NameIndexer(basepath, fidMap, outputPrefixIndex)).writeIndex()

    // this should really really be done by now
    revgeoLatch.foreach(_.await())

    val hasPolyCursor =
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
    hasPolyCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
    val polygonMap = logPhase("preloading polygon map") {
      hasPolyCursor.map(r => (r.polyId, (r._id, r.woeType))).toList.toMap
    }

    val parallelizedIndexers = List(
      new IdIndexer(basepath, fidMap, slugEntryMap),
      new FeatureIndexer(basepath, fidMap, polygonMap),
      new PolygonIndexer(basepath, fidMap)
    ) ++ (if (outputRevgeo) {
      List(new RevGeoIndexer(basepath, fidMap, polygonMap))
    } else { Nil })

    val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(4))
    val indexFutures = parallelizedIndexers.map(indexer =>
      diskIoFuturePool(indexer.writeIndex())
    )
    // wait forever to finish
    Future.collect(indexFutures).apply()
  }
}
