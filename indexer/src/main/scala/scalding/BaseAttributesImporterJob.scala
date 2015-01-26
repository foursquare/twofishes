// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.LongWritable

class BaseAttributesImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  lines.filterNot(_.startsWith("#")).flatMap(line => {
    val parts = line.split("\t")
    if (parts.size == 6) {
      val fidOpt = StoredFeatureId.fromHumanReadableString(parts(0), Some(GeonamesNamespace))
      val adm0capOpt = Helpers.TryO { parts(1).toInt }
      val worldcityOpt = Helpers.TryO { parts(2).toInt }
      val scalerankOpt = Helpers.TryO { parts(3).toInt }
      val natscaleOpt = Helpers.TryO { parts(4).toInt }
      val labelrankOpt = Helpers.TryO { parts(5).toInt }

      (fidOpt, adm0capOpt, worldcityOpt, scalerankOpt, natscaleOpt, labelrankOpt) match {
        case (Some(fid), Some(adm0cap), Some(worldcity), Some(scalerank), Some(natscale), Some(labelrank)) => {
          val attributes = GeocodeFeatureAttributes.newBuilder
            .adm0cap(adm0cap == 1)
            .worldcity(worldcity == 1)
            .scalerank(scalerank)
            .natscale(natscale)
            .labelrank(labelrank)
            .result
          Some(new LongWritable(fid.longId), attributes)
        }
        case _ => {
          // logger.error("%s: couldn't parse StoredFeatureId and attributes".format(line))
          None
        }
      }
    } else {
      None
    }
  }).group
    .head
    .write(TypedSink[(LongWritable, GeocodeFeatureAttributes)](SpindleSequenceFileSource[LongWritable, GeocodeFeatureAttributes](outputPath)))
}
