// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.LongWritable

class BaseMatchedPolygonsImporterJob(
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
    featureIdsString <- parts(2).split(",")
    featureId <- Helpers.TryO({ featureIdsString.toLong }).toList
    // skip names (3) and woeTypes (4)
    geometryBase64String = parts(5)
  } yield {
    val matchingValue = PolygonMatchingValue.newBuilder
      .polygonId(polygonId)
      .featureId(featureId)
      .wkbGeometryBase64String(geometryBase64String)
      .source(source)
      .result
    (new LongWritable(featureId) -> matchingValue)
  }).group
    .toList
    .mapValues({matchingValues: List[PolygonMatchingValue] => {
      PolygonMatchingValues(matchingValues)
    }})
    .write(TypedSink[(LongWritable, PolygonMatchingValues)](SpindleSequenceFileSource[LongWritable, PolygonMatchingValues](outputPath)))
}
