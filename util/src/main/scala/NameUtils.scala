// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import com.foursquare.twofishes.{FeatureName, FeatureNameFlags, GeocodeFeature}
import scala.collection.JavaConversions._

trait NameUtils {
  // Given an optional language and an abbreviation preference, find the best name
  // for a feature in the current context.
  class FeatureNameComparator(lang: Option[String], preferAbbrev: Boolean) extends Ordering[FeatureName] {
    def compare(a: FeatureName, b: FeatureName) = {
      scoreName(b) - scoreName(a)
    }

    def scoreName(name: FeatureName): Int = {
      var score = 0
      if (Option(name.flags).exists(_.contains(FeatureNameFlags.PREFERRED))) {
        score += 1
      }
      if (lang.exists(_ == name.lang)) {
        score += 2
      }
      if (Option(name.flags).exists(_.contains(FeatureNameFlags.ABBREVIATION) && preferAbbrev)) {
        score += 4
      }
      score
    }
  }

  def bestName(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean
  ): Option[FeatureName] = {
    f.names.sorted(new FeatureNameComparator(lang, preferAbbrev)).headOption
  }

  type BestNameMatch = (FeatureName, Option[String])

  def bestName(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean,
    matchedStringOpt: Option[String],
    debugLevel: Int,
    logger: TwofishesLogger
  ): Option[BestNameMatch] = {
    val ret = matchedStringOpt.flatMap(matchedString => {
      val namesNormalized = f.names.map(n => {
        (n, NameNormalizer.normalize(n.name))
      })

      val exactMatchNameCandidates = namesNormalized.filter(_._2 == matchedString).map(_._1)
      val prefixMatchNameCandidates = namesNormalized.filter(_._2.startsWith(matchedString)).map(_._1)

      val nameCandidates = if (exactMatchNameCandidates.isEmpty) {
        prefixMatchNameCandidates
      } else {
        exactMatchNameCandidates
      }

      val bestNameMatch = nameCandidates.sorted(new FeatureNameComparator(lang, preferAbbrev)).headOption
      if (debugLevel > 1) {
        logger.ifDebug("name candidates: " + nameCandidates)
        logger.ifDebug("best name match: " + bestNameMatch)
      }
      bestNameMatch.map(name => 
          (name,
            Some("<b>" + name.name.take(matchedString.size) + "</b>" + name.name.drop(matchedString.size))
      )) orElse {bestName(f, lang, preferAbbrev).map(name => {
        val normalizedName = NameNormalizer.normalize(name.name)
        val index = normalizedName.indexOf(matchedString)
        if (index > -1) {
          val before = name.name.take(index)
          val matched = name.name.drop(index).take(matchedString.size)
          val after = name.name.drop(index + matchedString.size)
          (name, Some("%s<b>%s</b>%s".format(before, matched, after)))
        } else {
          (name, None)
        }
      })}
    }) orElse { bestName(f, lang, preferAbbrev).map(n => (n, None)) }

    ret
  }
}

object NameUtils extends NameUtils
