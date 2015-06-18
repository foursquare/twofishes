// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.LongWritable

class BasePolygonGeometriesImporterJob(
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
    source = parts(1)
    geometryBase64String = parts(5)
  } yield {
    val matchingValue = PolygonMatchingValue.newBuilder
      .wkbGeometryBase64String(geometryBase64String)
      .source(source)
      .result
    (new LongWritable(polygonId) -> matchingValue)
  }).group
    .head
    .write(TypedSink[(LongWritable, PolygonMatchingValue)](SpindleSequenceFileSource[LongWritable, PolygonMatchingValue](outputPath)))
}
