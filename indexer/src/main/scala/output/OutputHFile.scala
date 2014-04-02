package com.foursquare.twofishes.output

import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.io._
import java.util.concurrent.CountDownLatch
import org.apache.hadoop.hbase.util.Bytes._
import scalaj.collection.Implicits._

class OutputIndexes(
  basepath: String, 
  outputPrefixIndex: Boolean, 
  slugEntryMap: SlugEntryMap, 
  outputRevgeo: Boolean
) with DurationUtils {
  def buildIndexes(revgeoLatch: CountDownLatch) {
    val fidMap = new FidMap(preload = false)

    (new NameIndexer(basepath, fidMap, outputPrefixIndex)).writeNames()
    (new IdIndexer(basepath, fidMap, slugEntryMap)).writeSlugsAndIds()
    (new FeatureIndexer(basepath, fidMap)).writeFeatures()
    (new PolygonIndexer(basepath, fidMap)).buildPolygonIndex()
    if (outputRevgeo) {
      revgeoLatch.await()
      (new RevGeoIndexer(basepath, fidMap)).buildRevGeoIndex()
    }
  }
}
