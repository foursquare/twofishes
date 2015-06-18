// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import org.apache.hadoop.io.LongWritable

class BaseFeatureUnionIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources)
    .group
    .head
    .write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](outputPath)))
}
