// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.twitter.scalding._
// import backtype.hadoop.ThriftSerialization
import org.apache.hadoop.io.{BytesWritable, LongWritable, Writable}
import com.twitter.scalding.typed.{Grouped, TypedSink}
import com.twitter.scalding.filecache.{DistributedCacheFile}
import com.foursquare.twofishes.{GeocodeRecord, GeocodeServingFeature}
import com.foursquare.common.thrift.ThriftConverter
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.util.{GeonamesNamespace, StoredFeatureId}
import com.foursquare.twofishes.{GeocodeBoundingBox, GeocodePoint}

class ScaldingParser(args : Args) extends Job(args) {
  val featureJob: Grouped[Long, GeocodeServingFeature] =
    new FeaturesParser(args).doPhase().group
  val boundingBoxJob: Grouped[Long, GeocodeBoundingBox] =
    new BoundingBoxParser(args).doPhase().group

  val withBoundingBoxes =
    featureJob.join(boundingBoxJob).map({case (k: Long, (f: GeocodeServingFeature, bb: GeocodeBoundingBox)) => {
      (k -> f.copy(feature = f.feature.copy(geometry = f.feature.geometry.copy(bounds = bb))))
    }})

  withBoundingBoxes
    .map({case (k, v) => (new LongWritable(k), v)})
    .write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](args("output"))))
}

class BoundingBoxParser(args: Args) extends Job(args) {
  def doPhase() = {
    val lines: TypedPipe[String] =
      TypedPipe.from(TextLine(args("boundingbox_input")))

    lines.filterNot(_.startsWith("#")).flatMap(line => {
      val parts = line.split("[\t ]")
      // 0: geonameid
      // 1->5:       // west, south, east, north
      if (parts.size != 5) {
        // logger.error("wrong # of parts: %d vs %d in %s".format(parts.size, 5, line))
        None
      } else {
        try {
          val id = parts(0)
          val w = parts(1).toDouble
          val s = parts(2).toDouble
          val e = parts(3).toDouble
          val n = parts(4).toDouble
          StoredFeatureId.fromHumanReadableString(id, Some(GeonamesNamespace)) match {
            case Some(fid) => {
              Some((
                fid.longId,
                GeocodeBoundingBox(GeocodePoint(n, e), GeocodePoint(s, w))
              ))
            }
            case None => {
              // logger.error("%s: couldn't parse into StoredFeatureId".format(line))
              None
            }
          }
        } catch {
          case e: Throwable =>
            // logger.error("%s: %s".format(line, e))
            None
        }
      }
    })
  }
}


class FeaturesParser(args: Args) extends Job(args) {
  def doPhase(): TypedPipe[(Long, GeocodeServingFeature)] = {
    val lines: TypedPipe[String] =
      TypedPipe.from(TextLine(args("feature_input")))

    (for {
      line <- lines
      feature <- GeonamesFeature.parseFromAdminLine(0, line)
    } yield {
      var lat = feature.latitude
      var lng = feature.longitude

      val geocodeRecord = GeocodeRecord(
        feature.featureId.longId,
        names = Nil,
        cc = feature.countryCode,
        _woeType = feature.featureClass.woeType.getValue,
        lat = feature.latitude,
        lng = feature.longitude,
        parents = Nil,
        population = feature.population,
        displayNames = Nil,
        boost = None,
        boundingbox = None,
        displayBounds = None
      )
      val servingFeature = geocodeRecord.toGeocodeServingFeature()
      (servingFeature.longId -> servingFeature)
    })
  }
}
