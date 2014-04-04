package com.foursquare.twofishes.output

import com.foursquare.twofishes.SlugEntryMap
import com.foursquare.twofishes.util.DurationUtils
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.io._
import java.util.concurrent.CountDownLatch
import org.apache.hadoop.hbase.util.Bytes._
import scalaj.collection.Implicits._
import com.twitter.util.{Future, FuturePool}
import java.util.concurrent.Executors

class OutputIndexes(
  basepath: String,
  outputPrefixIndex: Boolean,
  slugEntryMap: SlugEntryMap,
  outputRevgeo: Boolean
) extends DurationUtils {
  def buildIndexes(revgeoLatch: CountDownLatch) {
    val fidMap = new FidMap(preload = false)

    // This one wastes a lot of ram, so do it on it's own
    (new NameIndexer(basepath, fidMap, outputPrefixIndex)).writeIndex()

    // this should really really be done by now
    revgeoLatch.await()

    val parallelizedIndexers = List(
      new IdIndexer(basepath, fidMap, slugEntryMap),
      new FeatureIndexer(basepath, fidMap),
      new PolygonIndexer(basepath, fidMap)
    ) ++ (if (outputRevgeo) {
      List(new RevGeoIndexer(basepath, fidMap))
    } else { Nil })

    val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(4))
    val indexFutures = parallelizedIndexers.map(indexer =>
      diskIoFuturePool(indexer.writeIndex())
    )
    // wait forever to finish
    Future.collect(indexFutures).apply()
  }
}
