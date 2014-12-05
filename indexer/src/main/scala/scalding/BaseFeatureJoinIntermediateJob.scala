// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.Writable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.common.thrift.ThriftConverter

class BaseFeatureJoinIntermediateJob[
  K <: Writable with Comparable[K]: Manifest,
  L <: ThriftConverter.TType: Manifest: TupleConverter,
  R <: ThriftConverter.TType: Manifest: TupleConverter,
  O <: ThriftConverter.TType: Manifest: TupleConverter
](
  name: String,
  leftSources: Seq[String],
  rightSources: Seq[String],
  joiner: (Grouped[K, L], Grouped[K, R]) => TypedPipe[(K, O)],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val left = getJobOutputsAsTypedPipe[K, L](leftSources).group
  val right = getJobOutputsAsTypedPipe[K, R](rightSources).group
  
  val joined = joiner(left, right)

  joined.write(TypedSink[(K, O)](SpindleSequenceFileSource[K, O](outputPath)))
}
