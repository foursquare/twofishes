// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.LongWritable

class BaseMatchedPolygonsGeometryJoinIntermediateJob(
  name: String,
  prematchedPolygonSources: Seq[String],
  matchedPolygonSources: Seq[String],
  geometrySources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val prematched = getJobOutputsAsTypedPipe[LongWritable, PolygonMatchingValue](prematchedPolygonSources).group
  val matched = getJobOutputsAsTypedPipe[LongWritable, PolygonMatchingValue](matchedPolygonSources).group
  val geometries = getJobOutputsAsTypedPipe[LongWritable, PolygonMatchingValue](geometrySources).group

  prematched.outerJoin(matched)
    .flatMap({case (k: LongWritable, (pOpt: Option[PolygonMatchingValue], mOpt: Option[PolygonMatchingValue])) => {
      (pOpt, mOpt) match {
        case (Some(p), None) => Some(k -> p)
        case (None, Some(m)) => Some(k -> m)
        case (Some(p), Some(m)) => Some(k -> List(p, m).maxBy(_.polygonIdOption.getOrElse(0L)))
        case _ => None
      }}
    })
    // flip polygonId and featureId for geometry join
    .map({case (featureId: LongWritable, matchingValue: PolygonMatchingValue) => {
      val flippedValue = matchingValue.toBuilder
        .featureId(featureId.get)
        .result
      (new LongWritable(matchingValue.polygonIdOption.getOrElse(0L)) -> flippedValue)
    }})
    .group
    .toList
    // join with geometries and populate geometry and source and flip back featureId and polygonId
    .join(geometries)
    .flatMap({case (k: LongWritable, (matchingValues: List[PolygonMatchingValue], geometry: PolygonMatchingValue)) => {
      matchingValues.map(matchingValue => {
        val finalValue = matchingValue.toBuilder
          .wkbGeometryBase64String(geometry.wkbGeometryBase64StringOption)
          .source(geometry.sourceOption)
          .result
        (new LongWritable(finalValue.featureIdOption.getOrElse(0L)) -> finalValue)
      })
    }})
    .group
    .head
    .write(TypedSink[(LongWritable, PolygonMatchingValue)](SpindleSequenceFileSource[LongWritable, PolygonMatchingValue](outputPath)))
}
