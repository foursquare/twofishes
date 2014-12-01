// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._

class BaseFeatureEditsImporterJob(
  name: String,
  lineProcessor: (String) => Option[(LongWritable, GeocodeServingFeatureEdit)],
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  lines.filterNot(_.startsWith("#")).flatMap(line => lineProcessor(line))
    .group
    .toList
    .mapValues({edits: List[GeocodeServingFeatureEdit] => GeocodeServingFeatureEdits(edits)})
    .write(TypedSink[(LongWritable, GeocodeServingFeatureEdits)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeatureEdits](outputPath)))
}