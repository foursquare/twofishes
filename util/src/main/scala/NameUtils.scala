// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import com.foursquare.twofishes.{FeatureName, FeatureNameFlags, GeocodeFeature, YahooWoeType, util}
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import scala.io.BufferedSource
import scalaj.collection.Implicits._

object SlugBuilder {
  import NameFormatter.FormatPattern

  // TODO(blackmad): clearly a bug here with non-FEATURE patterns failing

  // do something special with CN-14 (tibet) in the parents, ditto taiwan

  val patterns = List(
    // FormatPattern("{COUNTRY+ABBR}"),
    // FormatPattern("{COUNTRY+ABBR}/{ADMIN1+ABBR}", countries = List("US", "CA")),
    // FormatPattern("{COUNTRY+ABBR}/{ADMIN1}"),
    // FormatPattern("{FEATURE}-{ADMIN1+ABBR}-{COUNTRY+ABBR}", countries = List("US", "CA")),
    // FormatPattern("{FEATURE}-{ADMIN2}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{ADMIN3}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{ADMIN2}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{ADMIN3}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{ADMIN3}-{ADMIN2}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{TOWN}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{TOWN}-{ADMIN2}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{TOWN}-{ADMIN3}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{TOWN}-{ADMIN3}-{ADMIN2}-{ADMIN1+ABBR}-{COUNTRY+ABBR}"),
    // FormatPattern("{FEATURE}-{COUNTRY+ABBR}")
    FormatPattern("{COUNTRY}"),
    FormatPattern("{COUNTRY}/{ADMIN1}", countries = List("US", "CA")),
    FormatPattern("{COUNTRY}/{ADMIN1}"),
    FormatPattern("{FEATURE}-{ADMIN1}", countries = List("US", "CA")),
    FormatPattern("{FEATURE}-{ADMIN1}-{COUNTRY}", countries = List("US", "CA")),
    FormatPattern("{FEATURE}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{ADMIN2}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{ADMIN3}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{ADMIN2}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{ADMIN3}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{ADMIN3}-{ADMIN2}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{TOWN}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{TOWN}-{ADMIN2}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{TOWN}-{ADMIN3}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{TOWN}-{ADMIN3}-{ADMIN2}-{ADMIN1}-{COUNTRY}"),
    FormatPattern("{FEATURE}-{COUNTRY}")
  )

  def normalize(s: String): String = {
    s.toLowerCase
      .replaceAll("['\u2018\u2019\\.\u2013]", "")
      .replaceAll("[\\p{Punct}&&[^-/]]", "")
      .replace(" ", "-")
  }

  def makePossibleSlugs(
    feature: GeocodeFeature,
    parents: List[GeocodeFeature]
  ): List[String] = {
    patterns.flatMap(p => NameFormatter.format(p, feature, parents, Some("en")))
      .map(normalize).map(NameNormalizer.deaccent)
  }
}

object NameFormatter {
  // patterns should look like "{TOWN}, {STATE+ABBR}"
  case class FormatPattern(
    pattern: String,
    // empty means all countries are acceptable
    countries: List[String] = Nil,
    // empty means all languages are acceptable
    languages: List[String] = Nil

  )

  def format(
    patterns: List[FormatPattern],
    feature: GeocodeFeature,
    parents: List[GeocodeFeature],
    lang: Option[String]
  ): Option[String] = {
    patterns.view.flatMap(p => format(p, feature, parents, lang)).headOption
  }

  case class WoeToken(
    woeType: YahooWoeType,
    preferAbbrev: Boolean
  )

  case class WoeTokenMatch(
    woeToken: WoeToken,
    name: String
  ) {
    def applyToString(input: String): String = {
      val replaceString = {
        if (woeToken.preferAbbrev) {
          "{%s+ABBR}".format(woeToken.woeType.toString)
        } else {
          "{%s}".format(woeToken.woeType.toString)
        }
      }

      input.replace(replaceString, name)
    }
  }

  def format(pattern: FormatPattern,
    feature: GeocodeFeature,
    parents: List[GeocodeFeature],
    lang: Option[String]
  ): Option[String] = {
    //val possibleTokens = com.foursquare.twofishes.YahooWoeType.values.asScala.map(_.name)
    if (pattern.countries.isEmpty || pattern.countries.has(feature.cc)) {
      val re = "\\{([^\\{]+)\\}".r
      val woeTokenStrings = re.findAllIn(pattern.pattern).collect{case re(m) => m}.toList
      val hasGeneralFeatureToken = woeTokenStrings.exists(_ == "FEATURE")
      val woeTokens = woeTokenStrings.filterNot(_ == "FEATURE").map(token => {
        val parts = token.split("\\+")
        val woeType = parts(0)
        val preferAbbrev = parts.lift(1).getOrElse("") == "ABBR"
        WoeToken(YahooWoeType.findByNameOrNull(woeType), preferAbbrev)
      })

      // see if we can find all the types
      val featuresToSearch = parents ++ (if (!hasGeneralFeatureToken) { List(feature) } else { Nil })
      var usedPrimaryFeature = false

      val tokenMatches = for {
        woeToken <- woeTokens
        matchFeature <- featuresToSearch.find(_.woeTypeOption.exists(_ =? woeToken.woeType))
        name <- NameUtils.bestName(matchFeature, lang, woeToken.preferAbbrev)
      } yield {
        if (matchFeature == feature) {
          usedPrimaryFeature = true
        }
        WoeTokenMatch(woeToken, name.name)
      }

      if (!usedPrimaryFeature && !hasGeneralFeatureToken) {
        None
      } else if (tokenMatches.size == woeTokens.size) {
        var finalString = pattern.pattern
        tokenMatches.foreach(tokenMatch => {
          finalString = tokenMatch.applyToString(finalString)
        })

        if (hasGeneralFeatureToken) {
          NameUtils.bestName(feature, lang, preferAbbrev = false).map(bestName =>
            finalString.replace("{FEATURE}", bestName.name)
          )
        } else {
          Some(finalString)
        }
      } else {
        // didn't match all format tokens, give up
        None
      }
    } else {
      None
    }
  }
}

object CountryConstants {
  val countryUsesStateAbbrev = Set("US", "CA", "BR", "AU", "MX")

  val countryUsesState =
    Set("GR", "CO", "MY", "TV", "IT", "MX", "PW",
      "KI", "GU", "CA", "TH", "PH", "IE", "PA",
      "AM", "BS", "MP", "AU", "CV", "KN", "VE",
      "CN", "KZ", "JP", "EG", "PG", "NI", "NG",
      "CC", "HN", "MH", "VN", "AS", "ES", "SO",
      "HM", "ID", "FM", "KY", "PF", "MN", "KR",
      "GB", "UY", "SC", "BR", "TW", "CX", "AR",
      "NR", "CL", "IQ", "SV", "NF", "IN", "VI",
      "US", "UM", "JM", "SR", "AD", "UZ", "HK")

  val countryUsesCountyAsState = Set("TW", "IE", "BE", "GB")
}

trait NameUtils {
  def countryUsesStateAbbrev(cc: String) =
    CountryConstants.countryUsesStateAbbrev.has(cc)

  def countryUsesState(cc: String) =
    CountryConstants.countryUsesState.has(cc)

  def countryUsesCountyAsState(cc: String) =
    CountryConstants.countryUsesCountyAsState.has(cc)

  private val blacklistedParentIds =
    new BufferedSource(getClass.getResourceAsStream("/blacklist_parents.txt"))
      .getLines.filterNot(_.startsWith("#"))
      .flatMap(l => StoredFeatureId.fromHumanReadableString(l, Some(GeonamesNamespace)))
      .map(_.longId)
      .toList
  def isFeatureBlacklistedforParent(id: Long) = blacklistedParentIds.has(id)

  // Given an optional language and an abbreviation preference, find the best name
  // for a feature in the current context.
  class FeatureNameScorer(lang: Option[String], preferAbbrev: Boolean) {
    def scoreName(name: FeatureName): Double = {
      var score = 0.0

      lang match {
        case Some(l) => {
          if (name.lang == l) {
            score += 40
          } else if (name.lang == "en") {
            // make english the most likely fallback if name does not exist in requested language
            score += 20
          } else {
            score += -20
          }
        }
        case _ =>
          ()
      }

      if (name.flags != null) {
        val it = name.flags.iterator
        while (it.hasNext) {
          val flag = it.next
          score += (flag match {
            case FeatureNameFlags.HISTORIC => -100
            case FeatureNameFlags.COLLOQUIAL => {
              // by itself, colloquial tends to be stupid things like "Frisco" for SF
              if (name.flags.size == 1) {
                -1
              } else {
              // With other things, it helps, especially countries
                1
              }
            }
            case FeatureNameFlags.SHORT_NAME => 11
            case FeatureNameFlags.NEVER_DISPLAY => -10000
            case FeatureNameFlags.LOW_QUALITY => -20
            case FeatureNameFlags.PREFERRED => 5
            case FeatureNameFlags.ALIAS => -1
            case FeatureNameFlags.DEACCENT => -1
            case FeatureNameFlags.ABBREVIATION => {
              if (preferAbbrev) { 1000 } else { 0 }
            }
            case FeatureNameFlags.ALT_NAME => -1
            case FeatureNameFlags.LOCAL_LANG => 5
            case _ => 0
          })
        }
      }

      score -= name.name.size * 0.0001

      score
    }
  }

  def bestNameFromList(
    f: GeocodeFeature,
    names: Seq[FeatureName],
    lang: Option[String],
    preferAbbrev: Boolean
  ): Option[FeatureName] = {
    if (preferAbbrev && f.woeTypeOption.exists(_ =? YahooWoeType.COUNTRY)) {
      names.find(n => n.name.size == 2 && Option(n.flags).exists(_.contains(FeatureNameFlags.ABBREVIATION)))
    } else {
      val modifiedPreferAbbrev = preferAbbrev &&
        f.woeTypeOption.exists(_ =? YahooWoeType.ADMIN1) &&
        countryUsesStateAbbrev(f.cc)
      val scorer = new FeatureNameScorer(lang, modifiedPreferAbbrev)
      var bestScore = 0.0
      var bestName = names.headOption
      for {
        name <- names
      } {
        val score = scorer.scoreName(name)
        if (score > bestScore) {
          bestScore = score
          bestName = Some(name)
        }
      }
      bestName
    }
  }

  def bestName(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean
  ): Option[FeatureName] = bestNameFromList(f, f.names, lang, preferAbbrev)

  type BestNameMatch = (FeatureName, Option[String])

  def bestName(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean,
    matchedStringOpt: Option[String],
    debugLevel: Int,
    logger: TwofishesLogger,
    treatPrefixMatchesEqualToExact: Boolean = false
  ): Option[BestNameMatch] = {
    val ret = matchedStringOpt.flatMap(matchedString => {
      val namesNormalized = f.names.map(n => {
        (n, NameNormalizer.normalize(n.name))
      })

      val prefixMatchNameCandidates = namesNormalized.filter(_._2.startsWith(matchedString)).map(_._1)
      val exactMatchNameCandidates = namesNormalized.filter(_._2 == matchedString).map(_._1) ++
        (if(treatPrefixMatchesEqualToExact) {
          prefixMatchNameCandidates
        } else Nil)
      val exactMatchesWithoutAirports =
        if (!f.woeTypeOption.exists(_ =? YahooWoeType.AIRPORT)) {
          exactMatchNameCandidates.filterNot(n => (n.lang == "iata" || n.lang == "icao"))
        } else {
          Nil
        }

      val nameCandidates = if (exactMatchesWithoutAirports.isEmpty) {
        if (prefixMatchNameCandidates.isEmpty) {
          exactMatchNameCandidates
        } else {
          prefixMatchNameCandidates
        }
      } else {
        exactMatchNameCandidates
      }

      def matchNormalizedStringToPrefixOfOriginal(normalized: String, original: String): String = {
        var i = 0
        original.takeWhile(s => {
          i += 1
          normalized.startsWith(NameNormalizer.normalize(original.take(i)))
        })
      }

      val modifiedPreferAbbrev = preferAbbrev &&
        f.woeTypeOption.exists(_ =? YahooWoeType.ADMIN1) &&
        countryUsesStateAbbrev(f.cc)

      val bestNameMatch = bestNameFromList(f, nameCandidates, lang, modifiedPreferAbbrev)

      if (debugLevel > 1) {
        logger.ifDebug("name candidates: %s", nameCandidates)
        logger.ifDebug("best name match: %s", bestNameMatch)
      }
      bestNameMatch.map(name => {
        // matchedString might be shorter than part of the display name it matches due to normalization
        val matchedDisplayString = matchNormalizedStringToPrefixOfOriginal(matchedString, name.name)
        (name,
          Some("<b>" + name.name.take(matchedDisplayString.size) + "</b>" + name.name.drop(matchedDisplayString.size)))
      }) orElse {bestName(f, lang, preferAbbrev).map(name => {
        val normalizedName = NameNormalizer.normalize(name.name)
        val index = normalizedName.indexOf(matchedString)
        if (index > -1) {
          // account for normalization before and inside match
          val before = matchNormalizedStringToPrefixOfOriginal(normalizedName.take(index), name.name)
          val matched = matchNormalizedStringToPrefixOfOriginal(matchedString, name.name.drop(before.size))
          val after = name.name.drop(before.size + matched.size)
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
