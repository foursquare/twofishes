// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._
import com.foursquare.twofishes.util.GeometryUtils
import com.foursquare.twofishes.util.Lists.Implicits._
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.LongWritable

class BaseFeatureCenterS2CellIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources)

  features.flatMap({case (id: LongWritable, f: GeocodeServingFeature) => {
    // we do not allow polygon-name matching to override embedded shapes on features
    // those can only be overridden by explicit id-matched polygons
    // prevent matching by not emitting s2cellId here
    if (f.scoringFeatures.hasPolyOption.has(true)) {
      None
    } else {
      val woeType = f.feature.woeType
      val s2Level = PolygonMatchingHelper.getS2LevelForWoeType(woeType)
      val center = f.feature.geometry.center
      val centerS2CellId = GeometryUtils.getS2CellIdForLevel(center.lat, center.lng, s2Level).id
      val matchingValue = PolygonMatchingValue.newBuilder
        .featureId(id.get)
        .names(f.feature.names)
        .result
      val matchingKey = PolygonMatchingKey(centerS2CellId, woeType)
      Some(new PolygonMatchingKeyWritable(matchingKey) -> matchingValue)
    }
  }}).group
    .toList
    .mapValues({matchingValues: List[PolygonMatchingValue] => {
      PolygonMatchingValues(matchingValues)
    }})
    .write(TypedSink[(PolygonMatchingKeyWritable, PolygonMatchingValues)](SpindleSequenceFileSource[PolygonMatchingKeyWritable, PolygonMatchingValues](outputPath)))
}
