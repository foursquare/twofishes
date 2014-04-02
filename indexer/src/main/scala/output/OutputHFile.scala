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


class OutputIndexes(
  basepath: String, 
  outputPrefixIndex: Boolean, 
  slugEntryMap: SlugEntryMap, 
  outputRevgeo: Boolean
) extends DurationUtils {
  def buildIndexes(revgeoLatch: CountDownLatch) {
    val fidMap = new FidMap(preload = false)

    (new NameIndexer(basepath, fidMap, outputPrefixIndex)).writeIndex()
    (new IdIndexer(basepath, fidMap, slugEntryMap)).writeIndex()
    (new FeatureIndexer(basepath, fidMap)).writeIndex()
    (new PolygonIndexer(basepath, fidMap)).writeIndex()
    if (outputRevgeo) {
      revgeoLatch.await()
      (new RevGeoIndexer(basepath, fidMap)).writeIndex()
    }
  }
}
