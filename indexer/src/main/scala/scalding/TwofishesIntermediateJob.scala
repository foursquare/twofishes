// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.Writable
import com.foursquare.common.thrift.ThriftConverter
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource

class TwofishesIntermediateJob(
  name: String,
  args: Args
) extends TwofishesJob(name, args) {

  def getJobOutputsAsTypedPipe[K <: Writable with Comparable[K]: Manifest, T <: ThriftConverter.TType: Manifest: TupleConverter](names: Seq[String]): TypedPipe[(K, T)] = {
    var pipe: TypedPipe[(K, T)] = TypedPipe.empty
    names.foreach(name => {
      val path = concatenatePaths(outputBaseDir, name)
      pipe = pipe ++ TypedPipe.from(SpindleSequenceFileSource[K, T](path))
    })
    pipe
  }
}
