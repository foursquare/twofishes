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

class ScaldingParser(args : Args) extends Job(args) {
  // override def ioSerializations =
  //   super.ioSerializations ++ List(classOf[backtype.hadoop.ThriftSerialization])

  val lines: TypedPipe[String] =
    TypedPipe.from(TextLine(args("input")))

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
    (new LongWritable(servingFeature.longId) -> servingFeature)
  }).write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](args("output"))))
}
