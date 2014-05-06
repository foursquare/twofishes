//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.{GeoTools, StoredFeatureId, TwofishesLogger}
import com.foursquare.twofishes.util.Lists.Implicits._
import scala.collection.mutable.HashMap
import scalaj.collection.Implicits._

// Comparator for parses, we score by a number of different features
//

object GeocodeParseOrdering {
  type ScoringFunc = (CommonGeocodeRequestParams, Parse[Sorted], GeocodeServingFeature, Seq[FeatureMatch]) => Option[(Int, String)]
}

class GeocodeParseOrdering(
    store: GeocodeStorageReadService,
    req: CommonGeocodeRequestParams,
    logger: TwofishesLogger,
    extraScorers: List[GeocodeParseOrdering.ScoringFunc] = Nil
  ) extends Ordering[Parse[Sorted]] {
  // Higher is better
  def scoreParse(parse: Parse[Sorted]): Int = {
    parse.headOption.map(primaryFeatureMatch => {
      val primaryFeature = primaryFeatureMatch.fmatch
      val rest = parse.drop(1)
      var signal = primaryFeature.scoringFeatures.population

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
        logger.ifDebug("Scoring %s", parse)
      }

      // if we have a repeated feature, downweight this like crazy
      // so st petersburg, st petersburg works, but doesn't break new york, ny
      if (parse.hasDupeFeature) {
        modifySignal(-100000000, "downweighting dupe-feature parse")
      }

      if (primaryFeature.feature.geometry.boundsOption.nonEmpty) {
        modifySignal(1000, "promoting feature with bounds")
      }

      if (req.woeHint.has(primaryFeature.feature.woeType)) {
        modifySignal(50000000,
          "woe hint matches %d".format(primaryFeature.feature.woeType.getValue))
      }

      // prefer a more aggressive parse ... bleh
      // this prefers "mt laurel" over the town of "laurel" in "mt" (montana)
      modifySignal(-5000 * parse.length, "parse length boost")

      // Matching country hint is good
      if (req.ccOption.exists(_ == primaryFeature.feature.cc)) {
        if (primaryFeature.feature.woeType =? YahooWoeType.POSTAL_CODE) {
          modifySignal(10000000, "postal code country code match")
        } else {
          modifySignal(10000, "country code match")
        }
      }

      val attributes = primaryFeature.feature.attributesOption
      val scalerank = attributes.flatMap(_.scalerankOption)
      scalerank.foreach(rank => {
        if (rank > 0) {
          // modifySignal((20 - rank) * 1000000, "exponential scale rank increase")
        }
      })

      def distancePenalty(ll: GeocodePoint) {
        val distance = if (primaryFeature.feature.geometry.boundsOption.nonEmpty) {
          GeoTools.distanceFromPointToBounds(ll, primaryFeature.feature.geometry.boundsOrThrow)
        } else {
          GeoTools.getDistance(ll.lat, ll.lng,
            primaryFeature.feature.geometry.center.lat,
            primaryFeature.feature.geometry.center.lng)
        }
        val distancePenalty = (distance.toInt / 100)
        if (distance < 5000) {
          modifySignal(2000000, "5km distance BONUS for being %s meters away".format(distance))

          if (primaryFeature.feature.woeType =? YahooWoeType.SUBURB) {
              modifySignal(3000000, "5km distance neightborhood intersection BONUS")
            }
        } else {
          modifySignal(-distancePenalty, "distance penalty for being %s meters away".format(distance))
        }
      }

      val llHint = req.llHintOption
      val boundsHint = req.boundsOption
      if (boundsHint.isDefined) {
        boundsHint.foreach(bounds => {
          // if you're in the bounds and the bounds are some small enough size
          // you get a uniform boost
          val bbox = GeoTools.boundingBoxToS2Rect(bounds)
          // distance in meters of the hypotenuse
          // if it's smaller than looking at 1/4 of new york state, then
          // boost everything in it by a lot
          val bboxContainsCenter =
            GeoTools.boundsContains(bounds, primaryFeature.feature.geometry.center)
          val bboxesIntersect =
            primaryFeature.feature.geometry.boundsOption.map(fBounds =>
              GeoTools.boundsIntersect(bounds, fBounds)).getOrElse(false)

          if (bbox.lo().getEarthDistance(bbox.hi()) < 200 * 1000 &&
            (bboxContainsCenter || bboxesIntersect)) {
            modifySignal(200000, "200km bbox intersection BONUS")

            if (primaryFeature.feature.woeType =? YahooWoeType.SUBURB) {
              modifySignal(3000000, "200km bbox neightborhood intersection BONUS")
            }
          } else {
            // fall back to basic distance-from-center logic
            distancePenalty(GeoTools.S2LatLngToPoint(bbox.getCenter))
          }
        })
      } else if (llHint.isDefined) {
        // Penalize far-away things
        llHint.foreach(distancePenalty)
      }

      // manual boost added at indexing time
      if (primaryFeature.scoringFeatures.boost != 0) {
        modifySignal(primaryFeature.scoringFeatures.boost, "manual boost")
      }

      StoredFeatureId.fromLong(primaryFeature.longId).foreach(fid =>
        store.hotfixesBoosts.get(fid).foreach(boost =>
          modifySignal(boost, "hotfix boost"))
      )

      // as a terrible tie break, things in the US > elsewhere
      // meant primarily for zipcodes
      if (primaryFeature.feature.cc == "US") {
        modifySignal(1, "US tie-break")
      }

      // no one likes counties
      if (primaryFeature.feature.cc == "US" && primaryFeature.feature.woeType == YahooWoeType.ADMIN2) {
        modifySignal(-30000, "no one likes counties in the US")
      }

      // In autocomplete mode, prefer "tighter" interpretations
      // That is, prefer "<b>Rego Park</b>, <b>N</b>Y" to
      // <b>Rego Park</b>, NY, <b>N</b>aalagaaffeqatigiit
      //
      // getOrdering returns a smaller # for a smaller thing
      val parentTypes = rest.map(_.fmatch.feature.woeType).sortBy(YahooWoeTypes.getOrdering)
      if (parentTypes.nonEmpty) {
        // if (parentTypes(0) == YahooWoeType.ADMIN2 && req.autocomplete) {
        //   modifySignal( -20, "downweight county matches a lot in autocomplete mode")
        // } else {
        modifySignal( -1 * YahooWoeTypes.getOrdering(parentTypes(0)), "prefer smaller parent interpretation")
      }

      modifySignal( -1 * YahooWoeTypes.getOrdering(primaryFeature.feature.woeType), "prefer smaller interpretation")

      for {
        scorer <- extraScorers
        (value, debugStr) <- scorer(req, parse, primaryFeature, rest)
      } {
        modifySignal(value, debugStr)
      }

      if (req.debug > 0) {
        logger.ifDebug("final score %s", signal)
        parse.setFinalScore(signal)
      }
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

  def normalCompare(a: Parse[Sorted], b: Parse[Sorted]): Int = {
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

  def compare(a: Parse[Sorted], b: Parse[Sorted]): Int = {
    // logger.ifDebug("Scoring %s vs %s".format(printDebugParse(a), printDebugParse(b)))

    val aFeature = a.primaryFeature
    val bFeature = b.primaryFeature

    val useSameTokens =
      aFeature.tokenStart == bFeature.tokenStart && aFeature.tokenEnd == bFeature.tokenEnd 
    val bothNotCountries = 
      aFeature.fmatch.feature.woeType != YahooWoeType.COUNTRY && 
      bFeature.fmatch.feature.woeType != YahooWoeType.COUNTY

    val bothNotHinted = 
      // if we have a hint that we want one of the types, then let the
      // scoring happen naturally
      !req.woeHint.has(aFeature.fmatch.feature.woeType) &&
      !req.woeHint.has(bFeature.fmatch.feature.woeType)

    val shouldCheckParents = 
      useSameTokens && bothNotCountries && bothNotHinted

      // if a is a parent of b, prefer b
    if (shouldCheckParents && aFeature.fmatch.scoringFeatures.parentIds.has(bFeature.fmatch.longId)) {
        logger.ifDebug("Preferring parse (%s) because it's a child of parse (%s)", a, b)
        -1
    // if b is a parent of a, prefer a
    } else if (shouldCheckParents && bFeature.fmatch.scoringFeatures.parentIds.has(aFeature.fmatch.longId)) {
      logger.ifDebug("Preferring parse (%s) because it's a child of parse (%s)", b, a)
      1
    } else {
      normalCompare(a, b)
    }
  }
}
