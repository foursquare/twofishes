// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.StoredFeatureId
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.{LongWritable, Text}

class BaseFeatureIndexBuildIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources).group

  features.map({case (featureId: LongWritable, servingFeature: GeocodeServingFeature) => {
    // remove geometry and full list of slugs (single slug on feature remains)
    val partialFeature = servingFeature.copy(
      slugs = null,
      feature = servingFeature.feature.copy(
        geometry = servingFeature.feature.geometry.copy(wkbGeometry = null)))

    (featureId -> partialFeature)
  }}).group
    .withReducers(1)
    .head
    .write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](outputPath)))
}
