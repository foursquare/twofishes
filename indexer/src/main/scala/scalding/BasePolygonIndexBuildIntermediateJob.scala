// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import java.nio.ByteBuffer

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.LongWritable

class BasePolygonIndexBuildIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources).group

  (for {
    (featureId, servingFeature) <- features
    if servingFeature.feature.geometry.wkbGeometryIsSet
    wkbGeometryByteBuffer = ByteBuffer.wrap(servingFeature.feature.geometry.wkbGeometryByteArray)
  } yield {
    (featureId -> IntermediateDataContainer.newBuilder.bytes(wkbGeometryByteBuffer).result)
  }).group
    .withReducers(1)
    .head
    .write(TypedSink[(LongWritable, IntermediateDataContainer)](SpindleSequenceFileSource[LongWritable, IntermediateDataContainer](outputPath)))
}
