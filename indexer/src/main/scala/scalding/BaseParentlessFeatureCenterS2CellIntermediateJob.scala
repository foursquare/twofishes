// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{RevGeoConstants, GeometryUtils}
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.{Text, LongWritable}

class BaseParentlessFeatureCenterS2CellIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources)

  val s2Levels = scala.collection.immutable.Range.inclusive(
    RevGeoConstants.minS2LevelForRevGeo,
    RevGeoConstants.maxS2LevelForRevGeo,
    RevGeoConstants.defaultLevelModForRevGeo)

  (for {
    (featureId, servingFeature) <- features
    if servingFeature.scoringFeatures.parentIds.isEmpty
    center = servingFeature.feature.geometry.center
    woeType = servingFeature.feature.woeTypeOrDefault
    s2Level <- s2Levels
    cellId = GeometryUtils.getS2CellIdForLevel(center.lat, center.lng, s2Level).id
  } yield {
    // HACK: cellIds seem to hash terribly and all go to the same reducer so use Text for now
    (new Text(cellId.toString) -> ParentMatchingValue(featureId.get, center, woeType))
  }).group
    .toList
    .map({case (idText: Text, matchingValues: List[ParentMatchingValue]) => {
      (new LongWritable(idText.toString.toLong) -> ParentMatchingValues(matchingValues))
    }})
    .write(TypedSink[(LongWritable, ParentMatchingValues)](SpindleSequenceFileSource[LongWritable, ParentMatchingValues](outputPath)))
}
