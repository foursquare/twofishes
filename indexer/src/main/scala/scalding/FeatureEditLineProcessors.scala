// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import org.apache.hadoop.io.LongWritable
import com.foursquare.twofishes._
import com.foursquare.twofishes.util._

object FeatureEditLineProcessors {

  private def parseFeatureIdStringAsLongId(idToken: String): Option[Long] = {
    StoredFeatureId.fromHumanReadableString(idToken, Some(GeonamesNamespace)).map(_.longId)
  }

  def processIgnoreLine(line: String): Option[(LongWritable, GeocodeServingFeatureEdit)] = {
    parseFeatureIdStringAsLongId(line).map(longId => {
      val edit = GeocodeServingFeatureEdit.newBuilder
        .longId(longId)
        .editType(EditType.Remove)
        .result

      (new LongWritable(longId), edit)
    })
  }

  def processMoveLine(line: String): Option[(LongWritable, GeocodeServingFeatureEdit)] = {
    val parts = line.split("\\|")
    if (parts.size == 2) {
      val longIdOpt = parseFeatureIdStringAsLongId(parts(0))
      val latlng = parts(1).split(",")
      if (latlng.size == 2) {
        val latOpt = Helpers.TryO { latlng(0).toDouble }
        val lngOpt = Helpers.TryO { latlng(1).toDouble }

        (longIdOpt, latOpt, lngOpt) match {
          case (Some(longId), Some(lat), Some(lng)) => {
            val edit = GeocodeServingFeatureEdit.newBuilder
              .longId(longId)
              .editType(EditType.Modify)
              .center(GeocodePoint(lat, lng))
              .result

            Some(new LongWritable(longId), edit)
          }

          case _ => {
            // logger.error("%s: couldn't parse StoredFeatureId and latlng".format(line))
            None
          }
        }
      } else {
        // logger.error("%s: couldn't parse StoredFeatureId and latlng".format(line))
        None
      }
    } else {
      // logger.error("%s: couldn't parse StoredFeatureId and latlng".format(line))
      None
    }
  }

  def processNameDeleteLine(line: String): Option[(LongWritable, GeocodeServingFeatureEdit)] = {
    val parts = line.split("\\|")
    if (parts.size == 2) {
      parseFeatureIdStringAsLongId(parts(0)).map(longId => {
        val edit = GeocodeServingFeatureEdit.newBuilder
          .longId(longId)
          .editType(EditType.Modify)
          // delete name in all languages
          .namesEdits(Seq(FeatureNameListEdit(EditType.Remove, parts(1), "*", Nil)))
          .result

        (new LongWritable(longId), edit)
      })
    } else {
      // logger.error("%s: couldn't parse StoredFeatureId and name-delete".format(line))
      None
    }
  }

  def processNameTransformLine(line: String): Option[(LongWritable, GeocodeServingFeatureEdit)] = {
    val parts = line.split("[\t ]")
    val longIdOpt = parseFeatureIdStringAsLongId(parts.lift(0).getOrElse(""))
    val rest = parts.drop(1).mkString(" ")
    val restParts = rest.split("\\|")
    val langOpt = restParts.lift(0)
    val nameOpt = restParts.lift(1)
    val flagsOpt = restParts.lift(2)

    val flagsList: Seq[FeatureNameFlags] = if (flagsOpt.isEmpty) {
      Seq(FeatureNameFlags.PREFERRED)
    } else {
      flagsOpt.getOrElse("").split(",").flatMap(f => FeatureNameFlags.unapply(f)).toList
    }

    (longIdOpt, langOpt, nameOpt, flagsList) match {
      case (Some(longId), Some(lang), Some(name), head :: rest) => {
        val edit = GeocodeServingFeatureEdit.newBuilder
          .longId(longId)
          .editType(EditType.Modify)
          // always add name and rely on merging to collapse flags
          .namesEdits(Seq(
            FeatureNameListEdit(
              EditType.Add,
              name,
              lang,
              flagsList.map(flag => FeatureNameFlagsListEdit(EditType.Add, flag)))
          ))
          .result

        Some(new LongWritable(longId), edit)
      }

      case _ => {
        // logger.error("%s: couldn't parse StoredFeatureId and name-transform".format(line))
        None
      }
    }
  }
}