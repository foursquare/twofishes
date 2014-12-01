// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util._

class BaseBoostsImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  lines.filterNot(_.startsWith("#")).flatMap(line => {
    val parts = line.split("\\|")
    if (parts.size == 2) {
      val fidOpt = StoredFeatureId.fromHumanReadableString(parts(0), Some(GeonamesNamespace))
      val boostOpt = Helpers.TryO { parts(1).toInt }

      (fidOpt, boostOpt) match {
        case (Some(fid), Some(boost)) => {
          Some(new LongWritable(fid.longId), boost)
        }
        case _ => {
          // logger.error("%s: couldn't parse StoredFeatureId-boost pair".format(line))
          None
        }
      }
    } else {
      None
    }
  }).group
    .sum
    .mapValues({boost: Int => IntermediateDataContainer.newBuilder.intValue(boost).result})
    .write(TypedSink[(LongWritable, IntermediateDataContainer)](SpindleSequenceFileSource[LongWritable, IntermediateDataContainer](outputPath)))
}
