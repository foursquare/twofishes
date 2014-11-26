// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.IntermediateDataContainer
import com.foursquare.twofishes.util.{GeonamesNamespace, StoredFeatureId}

class BaseHierarchyImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  lines.flatMap(line => {
    val parts = line.split("\\t")
    val parentIdOpt = StoredFeatureId.fromHumanReadableString(parts(0), Some(GeonamesNamespace))
    val childIdOpt = StoredFeatureId.fromHumanReadableString(parts(1), Some(GeonamesNamespace))
    val hierarchyType = parts.lift(2).getOrElse("")

    if (hierarchyType == "ADM") {
      (parentIdOpt, childIdOpt) match {
        case (Some(parentId), Some(childId)) => {
          Some(new LongWritable(childId.longId), parentId.longId)
        }
        case _ => {
          // logger.error("%s: couldn't parse into StoredFeatureId pair".format(line))
          None
        }
      }
    } else {
      None
    }
  }).group
    .toList
    .mapValues({parents: List[Long] => IntermediateDataContainer.newBuilder.longList(parents).result})
    .write(TypedSink[(LongWritable, IntermediateDataContainer)](SpindleSequenceFileSource[LongWritable, IntermediateDataContainer](outputPath)))
}
