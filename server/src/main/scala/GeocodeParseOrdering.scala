//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.geo.country.CountryInfo
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.{GeoTools, TwofishesLogger}
import com.foursquare.twofishes.util.Lists.Implicits._
import scala.collection.mutable.HashMap
import scalaj.collection.Implicits._

// Comparator for parses, we score by a number of different features
//

case class ScoringTerm(func: GeocodeParseOrdering.ScoringFunc, multiplier: Double = 1.0)
case class ScorerArguments(req: CommonGeocodeRequestParams, parse: Parse[Sorted], primaryFeature: GeocodeServingFeature, rest: Seq[FeatureMatch])

sealed trait ScorerResponse
class EmptyScorerResponse extends ScorerResponse
case class ScorerResponseWithScoreAndMessage(score: Int, message: String) extends ScorerResponse
object ScorerResponse {
  val Empty = new EmptyScorerResponse
}

object GeocodeParseOrdering {
  type ScoringFunc = PartialFunction[ScorerArguments, ScorerResponse]

  val noOpScorer: ScoringFunc = {
    case args => ScorerResponse.Empty
  }

  val populationBoost: ScoringFunc = {
    case args =>
      ScorerResponseWithScoreAndMessage(args.primaryFeature.scoringFeatures.population, "population")
  }

  val penalizeRepeatedFeatures: ScoringFunc = {
    case args if (args.parse.hasDupeFeature) => {
      // if we have a repeated feature, downweight this like crazy
      // so st petersburg, st petersburg works, but doesn't break new york, ny
      ScorerResponseWithScoreAndMessage(-100000000, "downweighting dupe-feature parse")
    }
  }

  val promoteFeatureWithBounds: ScoringFunc = {
    case args if (args.primaryFeature.feature.geometry.boundsOption.nonEmpty) => {
      ScorerResponseWithScoreAndMessage(1000, "promoting feature with bounds")
    }
  }

  val promoteWoeHintMatch: ScoringFunc = {
    case args if (args.req.woeHint.has(args.primaryFeature.feature.woeType)) => {
      ScorerResponseWithScoreAndMessage(50000000, "woe hint matches %d".format(args.primaryFeature.feature.woeType.getValue))
    }
  }

  val penalizeIrrelevantLanguageNameMatches: ScoringFunc = {
    case args => {
      val primaryMatchLangs = (for {
        fmatch <- args.parse.headOption.toList
        nameHit <- fmatch.possibleNameHits
        locale <- nameHit.langOption
        lang = locale.split("-")(0)
      } yield lang).toList

      if (primaryMatchLangs.has("en") ||
          primaryMatchLangs.has("abbr") ||
          primaryMatchLangs.has("iata") ||
          primaryMatchLangs.has("icao") ||
          primaryMatchLangs.has("") || // a lot of aliases tend to be names without a language
          args.req.langOption.exists(lang => primaryMatchLangs.has(lang)) ||
          primaryMatchLangs.exists(lang => CountryInfo.getCountryInfo(args.primaryFeature.feature.cc).exists(_.isLocalLanguage(lang)))) {
        ScorerResponse.Empty
      } else {
        ScorerResponseWithScoreAndMessage(-100000000, "penalizing name match in irrelevant language")
      }
    }
  }

  val penalizeBadNameMatches: ScoringFunc = {
    case args => {
      val flags = (for {
        fmatch <- args.parse.headOption.toList
        nameHit <- fmatch.possibleNameHits
        flag <- nameHit.flags
      } yield flag).toList

      if (flags.nonEmpty && flags.forall(f => f =? FeatureNameFlags.NEVER_DISPLAY || f =? FeatureNameFlags.LOW_QUALITY)) {
        ScorerResponseWithScoreAndMessage(-100000000, "penalizing name match with bad flags")
      } else {
        ScorerResponse.Empty
      }
    }
  }

  val penalizeLongParses: ScoringFunc = {
    case args => {
      // prefer a more aggressive parse ... bleh
      // this prefers "mt laurel" over the town of "laurel" in "mt" (montana)
      ScorerResponseWithScoreAndMessage(-5000 * args.parse.length, "parse length boost")
    }
  }

  val promoteCountryHintMatch: ScoringFunc = {
    case args if (args.req.ccOption.exists(_ == args.primaryFeature.feature.cc)) => {
      if (args.primaryFeature.feature.woeType =? YahooWoeType.POSTAL_CODE) {
        ScorerResponseWithScoreAndMessage(10000000, "postal code country code match")
      } else {
        ScorerResponseWithScoreAndMessage(1000000, "country code match")
      }
    }
  }

  val penalizeCountryHintMismatch: ScoringFunc = {
    case args if (args.req.ccOption.exists(_ != args.primaryFeature.feature.cc)) => {
      ScorerResponseWithScoreAndMessage(-100000000, "country code mismatch")
    }
  }

  private def isWorldCity(feature: GeocodeFeature): Boolean = {
    (for {
      attributes <- feature.attributesOption
      worldcity <- attributes.worldcityOption
    } yield {
      worldcity
    }).has(true)
  }

  val penalizeNonWorldCities: ScoringFunc = {
    case args if (!isWorldCity(args.primaryFeature.feature)) => {
      ScorerResponseWithScoreAndMessage(-100000000, "not a world city")
    }
  }

  def distanceBoostForPoint(args: ScorerArguments, ll: GeocodePoint, clampPenalty: Boolean): ScorerResponse = {
    val distance = if (args.primaryFeature.feature.geometry.boundsOption.nonEmpty) {
      GeoTools.distanceFromPointToBounds(ll, args.primaryFeature.feature.geometry.boundsOrThrow)
    } else {
      GeoTools.getDistance(ll.lat, ll.lng,
        args.primaryFeature.feature.geometry.center.lat,
        args.primaryFeature.feature.geometry.center.lng)
    }

    val (bucketName, distanceBoost, woeTypeBoost) = if (distance < 5000) {
      ("<5km boost", 4000000, if (args.primaryFeature.feature.woeType =? YahooWoeType.SUBURB) 6000000 else 0)
    } else if (distance < 10000) {
      ("5-10km boost", 2000000, if (args.primaryFeature.feature.woeType =? YahooWoeType.SUBURB) 3000000 else 0)
    } else if (distance < 20000) {
      ("10-20km boost", 1000000, if (args.primaryFeature.feature.woeType =? YahooWoeType.SUBURB) 2000000 else 0)
    } else if (distance < 100000) {
      ("20-100km boost", -10000, 0)
    } else {
      (">=100km penalty",
        if (clampPenalty) {
          -100000
        } else {
          -(distance.toInt)
        },
        0)
    }

    val debugString = if (args.req.debug > 0) {
      val woeTypeBoostString = if (woeTypeBoost > 0) {
        " (BONUS %s for woeType=%s)".format(woeTypeBoost, args.primaryFeature.feature.woeType.stringValue)
      } else {
        ""
      }
      "%s : %s for being %s meters away.%s".format(
        bucketName,
        distanceBoost,
        distance.toString,
        woeTypeBoostString)
    } else {
      ""
    }
    ScorerResponseWithScoreAndMessage(distanceBoost + woeTypeBoost, debugString)
  }

  def distanceBoostForBounds(args: ScorerArguments, bounds: GeocodeBoundingBox, clampPenalty: Boolean): ScorerResponse = {
    // if you're in the bounds and the bounds are some small enough size
    // you get a uniform boost
    val bbox = GeoTools.boundingBoxToS2Rect(bounds)
    // distance in meters of the hypotenuse
    // if it's smaller than looking at 1/4 of new york state, then
    // boost everything in it by a lot
    val bboxContainsCenter =
      GeoTools.boundsContains(bounds, args.primaryFeature.feature.geometry.center)
    val bboxesIntersect =
      args.primaryFeature.feature.geometry.boundsOption.map(fBounds =>
        GeoTools.boundsIntersect(bounds, fBounds)).getOrElse(false)

    if (bbox.lo().getEarthDistance(bbox.hi()) < 200 * 1000 &&
      (bboxContainsCenter || bboxesIntersect)) {
      if (args.primaryFeature.feature.woeType =? YahooWoeType.SUBURB) {
        ScorerResponseWithScoreAndMessage(5000000, "200km bbox neighborhood intersection BONUS")
      } else {
        ScorerResponseWithScoreAndMessage(2000000, "200km bbox intersection BONUS")
      }
    } else {
      // fall back to basic distance-from-center logic
      distanceBoostForPoint(args, GeoTools.S2LatLngToPoint(bbox.getCenter), clampPenalty)
    }
  }

  def distanceToBoundsOrLatLngHint(args: ScorerArguments, clampPenalty: Boolean): ScorerResponse = {
    val llHint = args.req.llHintOption
    val boundsHint = args.req.boundsOption
    if (boundsHint.isDefined) {
      boundsHint.flatMap(bounds => {
        Some(distanceBoostForBounds(args, bounds, clampPenalty))
      }).getOrElse(ScorerResponse.Empty)
    } else {
      // Penalize far-away things
      llHint.flatMap(ll =>
        Some(distanceBoostForPoint(args, ll, clampPenalty))
      ).getOrElse(ScorerResponse.Empty)
    }
  }

  val distanceToBoundsOrLatLngHintClampedPenalty: ScoringFunc = {
    case args => {
      distanceToBoundsOrLatLngHint(args, true)
    }
  }

  val distanceToBoundsOrLatLngHintUnclampedPenalty: ScoringFunc = {
    case args => {
      distanceToBoundsOrLatLngHint(args, false)
    }
  }

  val manualBoost: ScoringFunc = {
    case args if (args.primaryFeature.scoringFeatures.boost != 0) => {
      // manual boost added at indexing time
      ScorerResponseWithScoreAndMessage(args.primaryFeature.scoringFeatures.boost, "manual boost")
    }
  }

  val usTieBreak: ScoringFunc = {
    case args if (args.primaryFeature.feature.cc == "US") => {
      // as a terrible tie break, things in the US > elsewhere
      // meant primarily for zipcodes
      ScorerResponseWithScoreAndMessage(1, "US tie-break")
    }
  }

  val penalizeCounties: ScoringFunc = {
    case args if (args.primaryFeature.feature.cc == "US" && args.primaryFeature.feature.woeType == YahooWoeType.ADMIN2) => {
      // no one likes counties
      ScorerResponseWithScoreAndMessage(-30000, "no one likes counties in the US")
    }
  }

  val penalizeUnknowns: ScoringFunc = {
    case args if (args.primaryFeature.feature.woeType == YahooWoeType.UNKNOWN) => {
      // this will catch things like http://www.geonames.org/6269134/indian-subcontinent.html
      ScorerResponseWithScoreAndMessage(-1000000000, "unknown features")
    }
  }

  val woeTypeOrderForFeature: ScoringFunc = {
    case args =>
      ScorerResponseWithScoreAndMessage(-1 * YahooWoeTypes.getOrdering(args.primaryFeature.feature.woeType), "prefer smaller interpretation")
  }

  val woeTypeOrderForParents: ScoringFunc = {
    // In autocomplete mode, prefer "tighter" interpretations
    // That is, prefer "<b>Rego Park</b>, <b>N</b>Y" to
    // <b>Rego Park</b>, NY, <b>N</b>aalagaaffeqatigiit
    //
    // getOrdering returns a smaller # for a smaller thing
    case args => {
      val parentTypes = args.rest.map(_.fmatch.feature.woeType).sortBy(YahooWoeTypes.getOrdering)
      if (parentTypes.nonEmpty) {
        ScorerResponseWithScoreAndMessage(-1 * YahooWoeTypes.getOrdering(parentTypes(0)), "prefer smaller parent interpretation")
      } else ScorerResponse.Empty
    }
  }

  val penalizeAirports: ScoringFunc = {
    case args if (args.primaryFeature.feature.woeType == YahooWoeType.AIRPORT) => {
      ScorerResponseWithScoreAndMessage(-50000000, "downweight airports in autocomplete")
    }
  }

  val commonScorers: List[ScoringTerm] = List(
    ScoringTerm(penalizeRepeatedFeatures),
    ScoringTerm(promoteFeatureWithBounds),
    ScoringTerm(promoteWoeHintMatch),
    ScoringTerm(penalizeIrrelevantLanguageNameMatches),
    ScoringTerm(penalizeBadNameMatches),
    ScoringTerm(penalizeLongParses),
    ScoringTerm(usTieBreak),
    ScoringTerm(penalizeCounties),
    ScoringTerm(penalizeUnknowns),
    ScoringTerm(woeTypeOrderForFeature),
    ScoringTerm(woeTypeOrderForParents)
  )

  val scorersForGeocode: List[ScoringTerm] = commonScorers ++ List(
    ScoringTerm(populationBoost),
    ScoringTerm(promoteCountryHintMatch),
    ScoringTerm(distanceToBoundsOrLatLngHintClampedPenalty),
    ScoringTerm(manualBoost)
  )

  val commonScorersForAutocomplete: List[ScoringTerm] = commonScorers :+ ScoringTerm(penalizeAirports)

  val scorersForAutocompleteDefault: List[ScoringTerm] = commonScorersForAutocomplete ++ List(
    ScoringTerm(populationBoost, 0.1),
    ScoringTerm(promoteCountryHintMatch, 10.0),
    ScoringTerm(distanceToBoundsOrLatLngHintClampedPenalty),
    ScoringTerm(manualBoost, 0.001)
  )

  val scorersForAutocompleteWorldCityBias: List[ScoringTerm] = commonScorersForAutocomplete ++ List(
    ScoringTerm(populationBoost, 0.1),
    ScoringTerm(penalizeNonWorldCities),
    ScoringTerm(promoteCountryHintMatch, 10.0),
    ScoringTerm(distanceToBoundsOrLatLngHintClampedPenalty, 0.0),
    ScoringTerm(manualBoost, 0.001)
  )

  val scorersForAutocompleteLocalBias: List[ScoringTerm] = commonScorersForAutocomplete ++ List(
    ScoringTerm(populationBoost, 0.001),
    ScoringTerm(promoteCountryHintMatch, 10.0),
    ScoringTerm(distanceToBoundsOrLatLngHintUnclampedPenalty),
    ScoringTerm(manualBoost, 0.00001)
  )

  val scorersForAutocompleteGlobalBias: List[ScoringTerm] = commonScorersForAutocomplete ++ List(
    ScoringTerm(populationBoost),
    ScoringTerm(promoteCountryHintMatch, 0.001),
    ScoringTerm(distanceToBoundsOrLatLngHintClampedPenalty, 0.0001),
    ScoringTerm(manualBoost)
  )

  val scorersForAutocompleteStrictLocal: List[ScoringTerm] = commonScorersForAutocomplete ++ List(
    ScoringTerm(populationBoost, 0.0),
    ScoringTerm(promoteCountryHintMatch, 0.0),
    ScoringTerm(distanceToBoundsOrLatLngHintClampedPenalty),
    ScoringTerm(manualBoost, 0.0)
  )

  val scorersForAutocompleteStrictInCountryGlobal: List[ScoringTerm] = commonScorersForAutocomplete ++ List(
    ScoringTerm(populationBoost),
    ScoringTerm(penalizeCountryHintMismatch),
    ScoringTerm(distanceToBoundsOrLatLngHintClampedPenalty, 0.0),
    ScoringTerm(manualBoost)
  )

  def maybeReplaceTopResultWithRelatedCity(parses: Seq[Parse[Sorted]]): Seq[Parse[Sorted]] = {
    // if top result is not a city and there is an identically named city that is related to it
    // in the list of parses, move the city to the top
    val (qualifyingCity, rest) = parses.partition(p => {
      // this is a city
      p.primaryFeature.fmatch.feature.woeType == YahooWoeType.TOWN &&
      parses.headOption.exists(r => {
        // this and that match same tokens in query
        r.primaryFeature.tokenStart == p.primaryFeature.tokenStart &&
        r.primaryFeature.tokenEnd == p.primaryFeature.tokenEnd &&
        // that is not a country or an ADMIN1
        r.primaryFeature.fmatch.feature.woeType != YahooWoeType.COUNTRY &&
        r.primaryFeature.fmatch.feature.woeType != YahooWoeType.ADMIN1 &&
        // this is a parent of that or the other way around
        (r.primaryFeature.fmatch.scoringFeatures.parentIds.has(p.primaryFeature.fmatch.longId) ||
         p.primaryFeature.fmatch.scoringFeatures.parentIds.has(r.primaryFeature.fmatch.longId))
      })
    })

    if (qualifyingCity.isEmpty) {
      parses
    } else {
      qualifyingCity ++ rest
    }
  }
}

class GeocodeParseOrdering(
    req: CommonGeocodeRequestParams,
    logger: TwofishesLogger,
    scorers: List[ScoringTerm] = Nil,
    scorersName: String = ""
  ) extends Ordering[Parse[Sorted]] {
  // Higher is better
  def scoreParse(parse: Parse[Sorted]): Int = {
    parse.headOption.map(primaryFeatureMatch => {
      val primaryFeature = primaryFeatureMatch.fmatch
      val rest = parse.drop(1)
      var signal = 0

      def modifySignal(value: Int, debug: String) {
        if (req.debug > 0) {
          logger.ifDebug(" -- %s: %s + %s = %s", debug, signal, value, signal + value)
          parse.addDebugLine(
            DebugScoreComponent(debug, value)
          )
        }
        signal += value
      }

      if (req.debug > 0) {
        logger.ifDebug("Scorer %s scoring %s", scorersName, parse)
      }

      val scorerArguments = ScorerArguments(req, parse, primaryFeature, rest)
      for {
        scorer <- scorers
        response = (scorer.func orElse GeocodeParseOrdering.noOpScorer)(scorerArguments)
      } {
        response match {
          case r: ScorerResponseWithScoreAndMessage =>
            modifySignal((r.score * scorer.multiplier).toInt, r.message)

          case _ =>
        }
      }

      if (req.debug > 0) {
        logger.ifDebug("final score %s", signal)
      }
      parse.setFinalScore(signal)
      signal
    }).getOrElse(0)
  }

  var scoreMap = new scala.collection.mutable.HashMap[String, Int]
  def getScore(p: Parse[Sorted]): Int = {
    val scoreKey = p.scoreKey
    if (!scoreMap.contains(scoreKey)) {
      scoreMap(scoreKey) = scoreParse(p)
    }

    scoreMap.getOrElse(scoreKey, -1)
  }

  def compare(a: Parse[Sorted], b: Parse[Sorted]): Int = {
    val scoreA = getScore(a)
    val scoreB = getScore(b)
    if (scoreA == scoreB) {
      val diff = (a.headOption.map(_.fmatch.feature.longId).getOrElse(0L) -
        b.headOption.map(_.fmatch.feature.longId).getOrElse(0L))
      // .signum is slow, we don't want the .toInt to cause weird
      // long wrapping issues, so manually do this.
      if (diff < 0) { -1 }
      else if (diff > 0) { 1 }
      else { 0 }
    } else {
      scoreB - scoreA
    }
  }
}
