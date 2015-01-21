// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._
import com.foursquare.twofishes.util.GeometryUtils
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.{BytesWritable, LongWritable}

class BaseFeatureCenterS2CellIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources)

  features.map({case (id: LongWritable, f: GeocodeServingFeature) => {
    val woeType = f.feature.woeType
    val s2Level = PolygonMatchingHelper.getS2LevelForWoeType(woeType)
    val center = f.feature.geometry.center
    val centerS2CellId = GeometryUtils.getS2CellIdForLevel(center.lat, center.lng, s2Level).id
    val matchingValue = PolygonMatchingValue.newBuilder
      .featureId(id.get)
      .names(f.feature.names)
      .result
    val matchingKey = PolygonMatchingKey(centerS2CellId, woeType)
    (PolygonMatchingHelper.getKeyAsBytesWritable(matchingKey) -> matchingValue)
  }}).group
    .toList
    .mapValues({matchingValues: List[PolygonMatchingValue] => {
      PolygonMatchingValues(matchingValues)
    }})
    .write(TypedSink[(BytesWritable, PolygonMatchingValues)](SpindleSequenceFileSource[BytesWritable, PolygonMatchingValues](outputPath)))
}