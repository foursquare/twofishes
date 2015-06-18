// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.IntermediateDataContainer
import com.foursquare.twofishes.util._

class BaseRelationsImporterJob(
  name: String,
  fromColumnIndex: Int,
  toColumnIndex: Int,
  lineAcceptor: (Array[String]) => Boolean,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  lines.filterNot(_.startsWith("#")).flatMap(line => {
    val parts = line.split("[\t|]")
    val fromOpt = StoredFeatureId.fromHumanReadableString(parts(fromColumnIndex), Some(GeonamesNamespace))
    val toOpt = StoredFeatureId.fromHumanReadableString(parts(toColumnIndex), Some(GeonamesNamespace))

    if (lineAcceptor(parts)) {
      (fromOpt, toOpt) match {
        case (Some(fromId), Some(toId)) => {
          Some(new LongWritable(fromId.longId), toId.longId)
        }
        case _ => {
          // logger.error("%s: couldn't parse StoredFeatureId pair".format(line))
          None
        }
      }
    } else {
      None
    }
  }).group
    .toList
    .mapValues({ids: List[Long] => IntermediateDataContainer.newBuilder.longList(ids).result})
    .write(TypedSink[(LongWritable, IntermediateDataContainer)](SpindleSequenceFileSource[LongWritable, IntermediateDataContainer](outputPath)))
}
