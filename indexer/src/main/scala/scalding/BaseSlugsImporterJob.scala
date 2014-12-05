// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util._

class BaseSlugsImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  lines.filterNot(_.startsWith("#")).flatMap(line => {
    val parts = line.split("\t")
    if (parts.size == 4) {
      val slug = parts(0)
      val fidOpt = StoredFeatureId.fromHumanReadableString(parts(1), Some(GeonamesNamespace))
      val scoreOpt = Helpers.TryO { parts(2).toInt }
      val deprecatedOpt = Helpers.TryO { parts(3).toBoolean }

      (fidOpt, scoreOpt, deprecatedOpt) match {
        case (Some(fid), Some(score), Some(deprecated)) => {
          Some(new LongWritable(fid.longId), (slug, score, deprecated))
        }
        case _ => {
          // logger.error("%s: couldn't parse slug entry".format(line))
          None
        }
      }
    } else {
      None
    }
  }).group
    .toList
    .mapValues({slugs: List[(String, Int, Boolean)] => {
      val sortedSlugs = slugs.sortBy({case (slug: String, score: Int, deprecated: Boolean) => (deprecated, score * -1)})
      IntermediateDataContainer.newBuilder
        .stringList(sortedSlugs.map(_._1))
        .intList(sortedSlugs.map(_._2))
        .boolList(sortedSlugs.map(_._3))
        .result
    }})
    .write(TypedSink[(LongWritable, IntermediateDataContainer)](SpindleSequenceFileSource[LongWritable, IntermediateDataContainer](outputPath)))
}
