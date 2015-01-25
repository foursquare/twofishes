// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.LongWritable

class BasePrematchedPolygonsImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  (for {
    line <- lines
    if !line.startsWith("#")
    parts = line.split("\t")
    if parts.size == 6
    polygonId <- Helpers.TryO({ parts(0).toLong }).toList
    featureIdsString <- parts(2).split(",")
    featureId <- Helpers.TryO({ featureIdsString.toLong }).toList
  } yield {
    val matchingValue = PolygonMatchingValue.newBuilder
      .polygonId(polygonId)
      .featureId(featureId)
      .result
    (new LongWritable(featureId) -> matchingValue)
  }).group
    .maxBy(_.polygonIdOption.getOrElse(0L))
    .write(TypedSink[(LongWritable, PolygonMatchingValue)](SpindleSequenceFileSource[LongWritable, PolygonMatchingValue](outputPath)))
}
