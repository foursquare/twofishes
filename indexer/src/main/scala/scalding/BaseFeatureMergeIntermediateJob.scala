// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._
import com.twitter.scalding._
import com.twitter.scalding.typed.{MultiJoin, TypedSink}
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import org.apache.hadoop.io.LongWritable

class BaseFeatureMergeIntermediateJob(
  name: String,
  sources: Seq[String],
  merger: (Seq[GeocodeServingFeature]) => GeocodeServingFeature,
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val coGroupables = sources.map(source => getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](Seq(source)).group)

  val joined = coGroupables.size match {
    case 2 => MultiJoin(coGroupables(0), coGroupables(1))
    case 3 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2))
    case 4 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3))
    case 5 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4))
    case 6 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5))
    case 7 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6))
    case 8 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7))
    case 9 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8))
    case 10 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9))
    case 11 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10))
    case 12 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11))
    case 13 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12))
    case 14 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13))
    case 15 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13), coGroupables(14))
    case 16 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13), coGroupables(14), coGroupables(15))
    case 17 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13), coGroupables(14), coGroupables(15), coGroupables(16))
    case 18 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13), coGroupables(14), coGroupables(15), coGroupables(16), coGroupables(17))
    case 19 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13), coGroupables(14), coGroupables(15), coGroupables(16), coGroupables(17), coGroupables(18))
    case 20 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13), coGroupables(14), coGroupables(15), coGroupables(16), coGroupables(17), coGroupables(18), coGroupables(19))
    case 21 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13), coGroupables(14), coGroupables(15), coGroupables(16), coGroupables(17), coGroupables(18), coGroupables(19), coGroupables(20))
    case 22 => MultiJoin(coGroupables(0), coGroupables(1), coGroupables(2), coGroupables(3), coGroupables(4), coGroupables(5), coGroupables(6), coGroupables(7), coGroupables(8), coGroupables(9), coGroupables(10), coGroupables(11), coGroupables(12), coGroupables(13), coGroupables(14), coGroupables(15), coGroupables(16), coGroupables(17), coGroupables(18), coGroupables(19), coGroupables(20), coGroupables(21))
    case other: Int => throw new IllegalArgumentException("Cannot multi-join output from %d jobs (between 2 and 22 supported).".format(other))
  }

  val merged = joined.map({case (k: LongWritable, t: Product) => {
    val features = t.productIterator.toSeq.collect({case f: GeocodeServingFeature => f})
    (k -> merger(features))
  }})

  merged.write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](outputPath)))
}
