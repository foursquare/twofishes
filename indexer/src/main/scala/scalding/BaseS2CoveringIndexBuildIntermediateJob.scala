// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{S2CoveringConstants, GeometryUtils}
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.vividsolutions.jts.io.WKBReader
import org.apache.hadoop.io.LongWritable

class BaseS2CoveringIndexBuildIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources).group

  (for {
    (featureId, servingFeature) <- features
    if servingFeature.feature.geometry.wkbGeometryIsSet
    geometry = new WKBReader().read(servingFeature.feature.geometry.wkbGeometryByteArray)
    cellIds = GeometryUtils.s2PolygonCovering(
      geometry,
      S2CoveringConstants.minS2LevelForS2Covering,
      S2CoveringConstants.maxS2LevelForS2Covering,
      levelMod = Some(S2CoveringConstants.defaultLevelModForS2Covering),
      maxCellsHintWhichMightBeIgnored = Some(S2CoveringConstants.defaultMaxCellsHintForS2Covering)
    ).map(_.id)
  } yield {
    (featureId -> IntermediateDataContainer.newBuilder.longList(cellIds).result)
  }).group
    .withReducers(1)
    .head
    .write(TypedSink[(LongWritable, IntermediateDataContainer)](SpindleSequenceFileSource[LongWritable, IntermediateDataContainer](outputPath)))
}
