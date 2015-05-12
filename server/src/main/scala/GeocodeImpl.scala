//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.geo.country.DependentCountryInfo
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.YahooWoeType._
import com.foursquare.twofishes.util.{GeoTools, NameNormalizer}
import com.foursquare.twofishes.util.Lists.Implicits._
import scala.collection.mutable.ListBuffer
import scalaj.collection.Implicits._

// TODO
// --make autocomplete faster
// --save name-hit in the index
// --start treating Parse like an object, stop using headOption
// --test autocomplete more
// --better debugging
// --add more components if we have ambiguous names

object GeocoderImplConstants {
  val betterThanAdmin1 = Set(AIRPORT, SUBURB, TOWN, ADMIN3, ADMIN2)
}

class GeocoderImpl(
  store: GeocodeStorageReadService,
  val req: GeocodeRequest,
  logger: MemoryLogger
) extends AbstractGeocoderImpl[GeocodeResponse] with GeocoderUtils {
  override val implName = "geocode"

  // ACK!!! MUTABLE STATE!!!!
  var inRetry = false

  /*
    The basic algorithm works like this
    - roughly a depth-first recursive descent parser
    - for every set of tokens from size 1->n, attempt to find a feature with a matching name in our datastore
      (at the first level, for "rego park ny", we'd try rego, rego park, rego park ny)
    - for every feature found matching one of our token sets, recursively try to geocode the remaining tokens
    - return early from a branch of our parse tree if the parse becomes invalid/inconsistent, that is,
      if the feature found does not occur in the parents of the smaller feature found
      (we would about the parse of "los angeles, new york, united states" when we'd consumed "los angeles"
       its parents were CA & US, and then consumed "new york" because NY is not in the parents of LA)

    NOTE: what I just said was a bit of a lie, we dive all the way down to the left-most end of the string
    first and then come back up to save our work. We do this so that we can save our work in the cache for future
    parses. If we take two different parse trees to reach the same set of tokens,
    say [United States] in [New York United States], we want to save that we've already explored all the possibilities
    for United States.
    - save our work in a map[int -> parses], where the int is the total number of tokens those parses consume.
       we do this because:
       if two separate parses made it to the same point in the input, we don't need to redo the work
         (contrived example: Laurel Mt United States, can both be parsed as "Laurel" in "Mt" (Montana),
          and "Laurel Mt" (Mountain), both consistent & valid parses. One of those parses would have already
         completely explored "united states" before the second one gets to it)
    - we return the entire cache, which is a little silly. The caller knows to look for the cache for the
      largest key (the longest parse), and to take all the tokens before that and make the "what" (the
      non-geocoded tokens) in the final interpretation.
   */

  // for rego park ny us
  // first try us
  // then ny us (try "ny us" and ny -> "us"
  // then park ny us ()

   def generateParses(tokens: List[String]): ParseCache = {
    val cache = new ParseCache
    cache.put(0, List(NullParse))

    (tokens.size - 1).to(0, -1).foreach(offset => {
      val subTokens = tokens.drop(offset)
      val validParses = generateParsesHelper(subTokens, offset, cache)
      val cacheKey = subTokens.size
      if (req.debug > 1) {
        logger.ifDebug("setting %d to %s", cacheKey, validParses)
      }
      cache.put(cacheKey, validParses)
    })
    cache
  }

  def buildParse(f: FeatureMatch, p: Parse[Sorted]): Option[Parse[Sorted]] = {
    val sortedParse = p.addSortedFeature(f)
    if (isValidParse(sortedParse)) {
      Some(sortedParse)
    } else {
      None
    }
  }

  def generateParsesHelper(tokens: List[String], offset: Int, cache: ParseCache): SortedParseSeq = {
    1.to(tokens.size).flatMap(i => {
      val searchStr = tokens.take(i).mkString(" ")
      val featureMatches = logger.logDuration("get-by-name", "get-by-name for %s".format(searchStr)) {
        store.getByName(searchStr)
          .filter(servingFeature => isAcceptableFeature(req, servingFeature))
          .map((f: GeocodeServingFeature) =>
            FeatureMatch(offset, offset + i, searchStr, f, f.feature.names.filter(n => NameNormalizer.normalize(n.name) == searchStr))
          )
      }
      logger.ifDebug("got %d features for %s", featureMatches.size, searchStr)

      featureMatches.foreach(fm => {
        logger.ifLevelDebug(2, fm.toString)
      })

      if ((tokens.size - i) == 0) {
        featureMatches.flatMap(f => buildParse(f, NullParse))
      } else {
        val subParses = cache.get(tokens.size - i)

        val subParsesByCountry: Map[String, SortedParseSeq] = subParses.groupBy(_.countryCode)
        val featuresByCountry: Map[String, Seq[FeatureMatch]] = featureMatches.groupBy(_.fmatch.feature.cc)

        /*
         * augment with duplicate subparses for dependent countries
         * only duplicate subparse for a dependent country code if there is a feature match but no
         * existing subparse with the same country code
         */
        val augmentedSubParsesByCountry = logger.logDuration("augmentedSubParsesByCountry", "augmentedSubParsesByCountry") (for {
            (cc, parses) <- subParsesByCountry
            dcc <- DependentCountryInfo.getDependentCountryCodesForCountry(cc)
            if (featuresByCountry.contains(dcc) && !subParsesByCountry.contains(dcc))
          } yield {
            (dcc -> parses)
          }).toMap ++ subParsesByCountry

        logger.logDuration("buildParses", "buildParses for %s".format(searchStr)) {
          val lb = new ListBuffer[Parse[Sorted]]()
          lb.sizeHint(augmentedSubParsesByCountry.size * augmentedSubParsesByCountry.size)

          var parsesBuilt = 0
          for {
            cc <- augmentedSubParsesByCountry.keys
            f <- featuresByCountry.getOrElse(cc, Nil)
            p <- augmentedSubParsesByCountry.getOrElse(cc, Nil)
            diffentWoeTypes = p.mostSpecificFeature.fmatch.feature.woeType !=? f.fmatch.feature.woeType
            sameFeatureId = p.mostSpecificFeature.fmatch.feature.longId =? f.fmatch.feature.longId
            if (diffentWoeTypes || sameFeatureId)
          } {
            parsesBuilt += 1
            buildParse(f, p).foreach(parse => lb += parse)
          }

          logger.ifDebug("built and checked %d parses, saved %d for %s",
            parsesBuilt, lb.size, searchStr
          )

          lb.toVector
        }
      }
    })
  }

  def isValidParse(parse: Parse[Sorted]): Boolean = {
    if (isValidParseHelper(parse)) {
      true
    } else if (parse.fmatches.exists(_.fmatch.feature.woeType =? YahooWoeType.POSTAL_CODE)) {
      /*
       * zipcode hack
       * zipcodes don't belong to the political hierarchy, so they don't
       * have the parents you'd expect. Also, people call zipcodes a lot
       * of things other than the official name. As a result, we're going
       * to accept a parse that includes a zipcode if it's within 200km
       * of the next smallest feature
       */
      val sorted_parse = parse.getSorted
      (for {
        first <- sorted_parse.fmatches.lift(0)
        second <- sorted_parse.fmatches.lift(1)
      } yield {
        first.fmatch.feature.woeType == YahooWoeType.POSTAL_CODE &&
        isValidParseHelper(Parse[Sorted](sorted_parse.fmatches.drop(1))) &&
        GeoTools.getDistance(
          second.fmatch.feature.geometry.center.lat,
          second.fmatch.feature.geometry.center.lng,
          first.fmatch.feature.geometry.center.lat,
          first.fmatch.feature.geometry.center.lng) < 200000
      }).getOrElse(false)
    } else {
      false
    }
  }

  def isValidParseHelper(parse: Parse[Sorted]): Boolean = {
    if (parse.size <= 1) {
      true
    } else {
      val most_specific = parse(0)
      //logger.ifDebug("most specific: " + most_specific)
      //logger.ifDebug("most specific: parents" + most_specific.fmatch.scoringFeatures.parents)
      val rest = parse.view.drop(1)

      // "pizza in cincinnati" picks cincinnati IN over cincinnati OH
      // little hack--
      // if in the US, admin1 comes earlier in the parse than city, neighborhood, reject it
      if (most_specific.fmatch.feature.cc =? "CA" || most_specific.fmatch.feature.cc =? "US") {
        for {
          stateTokenPos: Int <- parse.filter(_.fmatch.feature.woeType =? YahooWoeType.ADMIN1).minByOption(_.tokenStart).map(_.tokenStart)
          cityOrNeighborhoodTokenPos: Int <- parse.filter(f =>
            GeocoderImplConstants.betterThanAdmin1.has(f.fmatch.feature.woeType)).minByOption(_.tokenStart).map(_.tokenStart)
        } {
          if (stateTokenPos < cityOrNeighborhoodTokenPos) {
            return false
          }
        }
      }

      rest.forall(f => {
        //logger.ifDebug("checking if %s in parents".format(f.fmatch.id))
        f.fmatch.longId =? most_specific.fmatch.longId ||
        most_specific.fmatch.scoringFeatures.parentIds.has(f.fmatch.longId) ||
        most_specific.fmatch.scoringFeatures.extraRelationIds.has(f.fmatch.longId) ||
        (f.fmatch.feature.woeType =? YahooWoeType.COUNTRY &&
          DependentCountryInfo.isCountryDependentOnCountry(most_specific.fmatch.feature.cc, f.fmatch.feature.cc))
      })
    }
  }

  def deleteCommonWords(tokens: List[String], parsedPhrases: Set[String]): List[String] = {
    val commonWords = Set(
      "city", "gemeinde", "canton", "of", "county", "gmina", "stadtteil", "district", "kommune", "prefecture", "contrada",
      "stazione", "di", "oblast", "Δήμος", "д", "м", "neighborhood", "neighbourhood", "the"
    ).map(_.toLowerCase)

    // do not drop common word if it is *part of* an already matched parse phrase
    // however, do drop it if it is the *entire* parse phrase as this must be a match to a bad name
    // e.g. do not drop "city" from (kansas city) but drop it from (city)
    tokens.filterNot(t => commonWords.contains(t) && !parsedPhrases.exists(p => p.contains(t) && p != t))
  }

  def getMaxInterpretations = {
    // TODO: remove once clients are filling this
    if (req.maxInterpretations <= 0) {
      1
    } else {
      req.maxInterpretations
    }
  }

  def maybeRetryParsing(
    parses: SortedParseSeq,
    parseParams: ParseParams
  ): GeocodeResponse = {
    val parsedPhrases = parses.flatMap(p => p.map(_.phrase)).toSet
    val modifiedTokens = deleteCommonWords(parseParams.originalTokens, parsedPhrases)
    logger.ifDebug("common words deleted: %s", modifiedTokens)
    if (modifiedTokens.size != parseParams.originalTokens.size && !inRetry) {
      inRetry = true
      logger.ifDebug("RESTARTING common words query: %s", modifiedTokens)
      doGeocodeForQuery(new QueryParser(logger).parseQueryTokens(modifiedTokens))
    } else {
      responseProcessor.buildFinalParses(
        parses, parseParams, getMaxInterpretations, requestGeom)
    }
  }

  val commonParams = GeocodeRequestUtils.geocodeRequestToCommonRequestParams(req)
  val responseProcessor = new ResponseProcessor(
    commonParams,
    store,
    logger)

  def doGeocodeImpl() = {
    val query = req.queryOption.getOrElse("")
    val parseParams = new QueryParser(logger).parseQuery(query)

    doGeocodeForQuery(parseParams)
  }

  def doGeocodeForQuery(parseParams: ParseParams) = {
    val tokens = parseParams.tokens
    val hadConnector = parseParams.hadConnector

    val cache = generateParses(tokens)

    val validParseCaches: Iterable[(Int, SortedParseSeq)] =
      cache.asScala.filter(_._2.nonEmpty)

    if (validParseCaches.size > 0) {
      val longest = validParseCaches.map(_._1).max
      if (hadConnector && longest != tokens.size) {
        responseProcessor.generateResponse(Nil, requestGeom)
      } else {
        val parsesToConsider = new ListBuffer[Parse[Sorted]]

        // take a maximum of 10 interps total for now
        val maxInterpretationsToConsider = {
          if (req.maxInterpretations <= 0) {
            1
          } else {
            req.maxInterpretations * 2
          }
        }

        for {
          length <- longest.to(1, -1).toVector
          if (parsesToConsider.size < maxInterpretationsToConsider)
          (size, parses) <- validParseCaches.find(_._1 == length)
        } {
          parsesToConsider.appendAll(
            parses.sorted(new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForGeocode, "default"))
          )
        }

        if (longest != tokens.size) {
          maybeRetryParsing(parsesToConsider, parseParams)
        } else {
          responseProcessor.buildFinalParses(
            GeocodeParseOrdering.maybeReplaceTopResultWithRelatedCity(parsesToConsider), parseParams, getMaxInterpretations, requestGeom)
        }
      }
    } else {
      responseProcessor.generateResponse(Nil, requestGeom)
    }
  }
}
