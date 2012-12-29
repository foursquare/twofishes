//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.util.{GeoTools, NameUtils, TwofishesLogger, NameNormalizer}
import com.foursquare.twofishes.util.NameUtils.BestNameMatch
import com.foursquare.twofishes.Implicits._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.util.{Future, FuturePool}
import java.util.concurrent.ConcurrentHashMap
import java.util.Date
import org.bson.types.ObjectId
import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scalaj.collection.Implicits._

// TODO
// --make autocomplete faster
// --save name-hit in the index
// --start treating Parse like an object, stop using headOption
// --test autocomplete more
// --better debugging
// --add more components if we have ambiguous names

// Represents a match from a run of tokens to one particular feature
case class FeatureMatch(
  tokenStart: Int,
  tokenEnd: Int,
  phrase: String,
  fmatch: GeocodeServingFeature
)  

// Sort a list of features, smallest to biggest
object GeocodeServingFeatureOrdering extends Ordering[GeocodeServingFeature] {
  def compare(a: GeocodeServingFeature, b: GeocodeServingFeature) = {
    YahooWoeTypes.getOrdering(a.feature.woeType) - YahooWoeTypes.getOrdering(b.feature.woeType)
  }
}

// Sort a list of feature matches, smallest to biggest
object FeatureMatchOrdering extends Ordering[FeatureMatch] {
  def compare(a: FeatureMatch, b: FeatureMatch) = {
    GeocodeServingFeatureOrdering.compare(a.fmatch, b.fmatch)
  }
}


class GeocoderImpl(store: GeocodeStorageFutureReadService, req: GeocodeRequest) {
  class MemoryLogger extends TwofishesLogger {
    val startTime = new Date()

    def timeSinceStart = {
      new Date().getTime() - startTime.getTime()
    }

    val lines: ListBuffer[String] = new ListBuffer()

    def ifDebug(s: => String, level: Int = 0) {
      if (level >= 0 && req.debug >= level) {
        lines.append("%d: %s".format(timeSinceStart, s))
      }
    }

    def getLines: List[String] = lines.toList

    def toOutput(): String = lines.mkString("<br>\n");
  }

  val logger = new MemoryLogger

  // Parse = one particular interpretation of the query
  type Parse = Seq[FeatureMatch]
  // ParseSeq = multiple interpretations
  type ParseSeq = Seq[Parse]
  // ParseCache = a table to save our previous parses from the left-most
  // side of the string.
  type ParseCache = ConcurrentHashMap[Int, Future[ParseSeq]]

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

   def generateParses(tokens: List[String]): ParseCache = {
    val cache = new ParseCache
    generateParsesHelper(tokens, 0, cache)
    cache
  }

  def generateParsesHelper(tokens: List[String], offset: Int, cache: ParseCache): Future[ParseSeq] = {
    val cacheKey = tokens.size
    if (tokens.size == 0) {
      Future.value(List(Nil))
    } else {
      if (!cache.contains(cacheKey)) {
        val searchStrs: Seq[String] =
          1.to(tokens.size).map(i => tokens.take(i).mkString(" "))

        val toEndStr = tokens.mkString(" ")

        val featuresFs: Seq[Future[Seq[FeatureMatch]]] =
          for ((searchStr, i) <- searchStrs.zipWithIndex) yield {
            store.getByName(searchStr).map(_.map(f => 
              FeatureMatch(offset, offset + i, searchStr, f)
            ))
          }

         // Compute subparses in parallel (scatter)
         val subParsesFs: Seq[Future[ParseSeq]] =
           1.to(tokens.size).map(i => generateParsesHelper(tokens.drop(i), offset + i, cache))

         // Collect the results of all the features and subparses (gather)
         val featureListsF: Future[ParseSeq] = Future.collect(featuresFs)
         val subParseSeqsF: Future[Seq[ParseSeq]] = Future.collect(subParsesFs)

         val validParsesF: Future[ParseSeq] =
          for((featureLists, subParseSeqs) <- featureListsF.join(subParseSeqsF)) yield {
            val validParses: ParseSeq = (
              featureLists.zip(subParseSeqs).flatMap({case(features: Parse, subparses: ParseSeq) => {
                (for {
                  f <- features
                  val _ = logger.ifDebug("looking at %s".format(f), 3)
                  p <- subparses
                  val _ = logger.ifDebug("sub_parse: %s".format(p), 3)
                } yield {
                  val parse: Parse = p ++ List(f)
                  val sortedParse = parse.sorted(FeatureMatchOrdering)
                  if (isValidParse(sortedParse)) {
                    logger.ifDebug("VALID -- adding to %d".format(cacheKey), 4)
                    logger.ifDebug("sorted " + sortedParse, 4)
                    Some(sortedParse)
                  } else {
                    logger.ifDebug("INVALID", 4)
                    None
                  }
                }).flatten
              }})
            )
            validParses.toList
          }

        if (req.debug > 1) {
          validParsesF.onSuccess(validParses => {
            logger.ifDebug("setting %d to %s".format(cacheKey, validParses))
          })
        }
        cache(cacheKey) = validParsesF
      }
      cache(cacheKey)
    }
  }

  def isValidParse(parse: Parse): Boolean = {
    if (isValidParseHelper(parse)) {
      true
    } else {
      /*
       * zipcode hack
       * zipcodes don't belong to the political hierarchy, so they don't
       * have the parents you'd expect. Also, people call zipcodes a lot
       * of things other than the official name. As a result, we're going
       * to accept a parse that includes a zipcode if it's within 200km
       * of the next smallest feature
       */
      val sorted_parse = parse.sorted(FeatureMatchOrdering)
      (for {
        first <- sorted_parse.lift(0)
        second <- sorted_parse.lift(1)
      } yield {
        first.fmatch.feature.woeType == YahooWoeType.POSTAL_CODE &&
        isValidParseHelper(sorted_parse.drop(1)) &&
        GeoTools.getDistance(
          second.fmatch.feature.geometry.center.lat,
          second.fmatch.feature.geometry.center.lng,
          first.fmatch.feature.geometry.center.lat,
          first.fmatch.feature.geometry.center.lng) < 200000
      }).getOrElse(false)
    }
  }

  def isValidParseHelper(parse: Parse): Boolean = {
    if (parse.size <= 1) {
      true
    } else {
      val most_specific = parse(0)
      //logger.ifDebug("most specific: " + most_specific)
      //logger.ifDebug("most specific: parents" + most_specific.fmatch.scoringFeatures.parents)
      val rest = parse.drop(1)
      rest.forall(f => {
        //logger.ifDebug("checking if %s in parents".format(f.fmatch.id))
        f.fmatch.id == most_specific.fmatch.id ||
        most_specific.fmatch.scoringFeatures.parents.map(_.relatedId).contains(f.fmatch.id)
      })
    }
  }

  /**** AUTOCOMPLETION LOGIC ****/
  /*
   In an effort to make autocomplete much faster, we throw out a number of the 
   features and hacks of the primary matching logic. We assume that the query
   is being entered right-to-left, smallest-to-biggest. We also skip out on most
   of the zip-code hacks. Since zip-codes aren't really in the political hierarchy,
   they don't have all the parents one might expect them to. In the full scorer, we
   need to hydrate the full features for our name hits so that we can check the
   feature type and compare the distances as a hack for zip-code containment. 

   In this scorer, we assume the left-most strings that we match are matched to the
   smallest feature, and then we don't need to hydrate any future matches to determine
   validity, we only need to check whether the matched ids occur in the parents of
   that smallest feature

   This means that some logic is duplicated. sorry.
   */
  def buildValidParses(
      parses: ParseSeq,
      fids: Seq[ObjectId],
      offset: Int,
      i: Int,
      matchString: String): Future[ParseSeq] = {
    if (parses.size == 0) {
      logger.ifDebug("parses == 0, so accepting everything")
      store.getByObjectIds(fids).map(featureMap =>
        featureMap.map({case(oid, feature) => {
          List(FeatureMatch(offset, offset + i,
            matchString, feature))
        }}).toList
      )
    } else {
      val validParsePairs: Seq[(Parse, ObjectId)] = parses.flatMap(parse => {
        if (req.debug > 0) {
          logger.ifDebug("checking %d fids against %s".format(fids.size, parse.map(_.fmatch.id)))
          logger.ifDebug("these are the fids of my parse: %s".format(fids))
        }
        fids.flatMap(fid => {
          if (req.debug > 0) {
            logger.ifDebug("checking if %s is an unused parent of %s".format(
              fid, parse.map(_.fmatch.id)))
          }

          val isValid = parse.exists(_.fmatch.scoringFeatures.parents.asScala.map(_.relatedId).has(fid.toString)) &&
            !parse.exists(_.fmatch.id.toString == fid.toString)
          if (isValid) {
            if (req.debug > 0) {
              logger.ifDebug("HAD %s as a parent".format(fid))
            }
            Some((parse, fid))
          } else {
           // logger.ifDebug("wasn't")
           None
          }
        })
      })

      val fidsToFetch: Seq[ObjectId] = validParsePairs.map(_._2).toSet.toList
      val featuresMapF = store.getByObjectIds(fidsToFetch)

      featuresMapF.map(featureMap => {
        validParsePairs.flatMap({case(parse, fid) => {
          featureMap.get(fid).map(feature => {
            parse ++ List(FeatureMatch(offset, offset + i,
              matchString, feature))
          })
        }})
      })
    }
  }

  def generateAutoParses(tokens: List[String], spaceAtEnd: Boolean): Future[ParseSeq] = {
    generateAutoParsesHelper(tokens, 0, Nil, spaceAtEnd)
  }

  def matchName(name: FeatureName, query: String, isEnd: Boolean): Boolean = {
    if (name.flags.contains(FeatureNameFlags.PREFERRED) ||
        name.flags.contains(FeatureNameFlags.ABBREVIATION) ||
        name.flags.contains(FeatureNameFlags.LOCAL_LANG) ||
        name.flags.contains(FeatureNameFlags.ALT_NAME)) { 
      val normalizedName = NameNormalizer.normalize(name.name)
      if (isEnd) {
        normalizedName.startsWith(query)
      } else {
        normalizedName == query
      }
    } else {
      false
    }
  }

  // Yet another huge hack because I don't know what name I hit
  def filterNonPrefExactAutocompleteMatch(ids: Seq[ObjectId], phrase: String): Future[Seq[ObjectId]] = {
    store.getByObjectIds(ids).map(features => {
      features.filter(f => {
        f._2.feature.woeType == YahooWoeType.POSTAL_CODE ||
        {
          val nameMatch = bestNameWithMatch(f._2.feature, Some(req.lang), false, Some(phrase))
          nameMatch.exists(nm => 
            nm._1.flags.contains(FeatureNameFlags.PREFERRED) ||
            nm._1.flags.contains(FeatureNameFlags.ALT_NAME)
          )
        }
      }).toList.map(_._1)
    })
  }

  def generateAutoParsesHelper(tokens: List[String], offset: Int, parses: ParseSeq, spaceAtEnd: Boolean): Future[ParseSeq] = {
    if (tokens.size == 0) {
      Future.value(parses)
    } else {
      val validParses: Seq[Future[ParseSeq]] = 1.to(tokens.size).map(i => {
        val query = tokens.take(i).mkString(" ")
        val isEnd = (i == tokens.size)

        val possibleParents = parses.flatMap(_.flatMap(_.fmatch.scoringFeatures.parents))
          .map(parent => new ObjectId(parent.relatedId))

        val fidsF: Future[Seq[ObjectId]] = 
          if (parses.size == 0) {
            if (isEnd) {
              logger.ifDebug("looking at prefix: %s".format(query))
              if (spaceAtEnd) {
                Future.collect(List(
                  store.getIdsByNamePrefix(query + " "),
                  store.getIdsByName(query).flatMap(ids => 
                    filterNonPrefExactAutocompleteMatch(ids, query))
                )).map((a: Seq[Seq[ObjectId]]) => a.flatten)
              } else {
                store.getIdsByNamePrefix(query)
              }
            } else {
              store.getIdsByName(query)
            }
          } else {
            store.getByObjectIds(possibleParents).map(features => {
              features.filter(feature => {
                feature._2.feature.names.exists(n => matchName(n, query, isEnd))
              }).map(_._1).toList
            })
          }

        val nextParseF: Future[ParseSeq] = fidsF.flatMap(featureIds => {
          logger.ifDebug("%d-%d: looking at: %s (is end? %s)".format(offset, i, query, isEnd))
          logger.ifDebug("%d-%d: %d previous parses: %s".format(offset, i, parses.size,   parses))
          logger.ifDebug("%d-%d: examining %d featureIds against parse".format(offset, i, featureIds.size))
          buildValidParses(parses, featureIds, offset, i, query)
        })

        nextParseF.flatMap(nextParses => {
          if (nextParses.size == 0) {
            Future.value(Nil)
          } else {
            generateAutoParsesHelper(tokens.drop(i), offset + i, nextParses, spaceAtEnd)
          }
        })
      })
      Future.collect(validParses).map(vp => {
        vp.flatten
      })
    }
  }

  // Another delightful hack. We don't save a pointer to the specific name we matched
  // in our inverted index, instead, if we know which tokens matched this feature,
  // we look for the name that, when normalized, matches the query substring.
  // i.e. for [New York, NY, Nueva York], if we know that we matched "neuv", we
  // look for the names that started with that.
  var nameMatchMap =
    new scala.collection.mutable.HashMap[String, Option[BestNameMatch]]

  def bestNameWithMatch(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean,
    matchedStringOpt: Option[String]
  ): Option[BestNameMatch] = {
    val hashKey = "%s:%s:%s:%s".format(f.ids, lang, preferAbbrev, matchedStringOpt)
    if (!nameMatchMap.contains(hashKey)) {
      nameMatchMap(hashKey) = NameUtils.bestName(f, lang, preferAbbrev, matchedStringOpt, req.debug, logger)
    }
    nameMatchMap(hashKey)
  }

  def parseHasDupeFeature(p: Parse): Boolean = {
    p.headOption.exists(primaryFeature => {
      val rest = p.drop(1)
      rest.exists(_.fmatch.feature.ids == primaryFeature.fmatch.feature.ids)
    })
  }

  // Comparator for parses, we score by a number of different features
  // 
  class ParseOrdering extends Ordering[Parse] {
    var scoreMap = new scala.collection.mutable.HashMap[String, Int]

    // Higher is better
    def scoreParse(parse: Parse): Int = {
      parse.headOption.map(primaryFeatureMatch => {
        val primaryFeature = primaryFeatureMatch.fmatch
        val rest = parse.drop(1)
        var signal = primaryFeature.scoringFeatures.population

        def modifySignal(value: Int, debug: String) {
          if (req.debug > 0) {
            logger.ifDebug("%s: %s + %s = %s".format(
              debug, signal, value, signal + value))
          }
          signal += value
        }
      
        if (req.debug > 0) {
          logger.ifDebug("Scoring %s".format(printDebugParse(parse)))
        }

        // if we have a repeated feature, downweight this like crazy
        // so st petersburg, st petersburg works, but doesn't break new york, ny
        if (parseHasDupeFeature(parse)) {
          modifySignal(-100000000, "downweighting dupe-feature parse")
        }

        if (primaryFeature.feature.geometry.bounds != null) {
          modifySignal(1000, "promoting feature with bounds")
        }

        if (req.woeHint.contains(primaryFeature.feature.woeType)) {
          modifySignal(1000000,
            "woe hint matches %d".format(primaryFeature.feature.woeType.getValue))
        }

        // prefer a more aggressive parse ... bleh
        // this prefers "mt laurel" over the town of "laurel" in "mt" (montana)
        modifySignal(-5000 * parse.length, "parse length boost")

        // Matching country hint is good
        if (Option(req.cc).exists(_ == primaryFeature.feature.cc)) {
          modifySignal(10000, "country code match")
        }

        def distancePenalty(ll: GeocodePoint) {
          val distance = GeoTools.getDistance(ll.lat, ll.lng,
              primaryFeature.feature.geometry.center.lat,
              primaryFeature.feature.geometry.center.lng)
          val distancePenalty = (distance / 100)
          if (distance < 5000) {
            modifySignal(200000, "5km distance BONUS")          
          } else {
            modifySignal(-distancePenalty, "distance penalty")          
          }
        }

        val llHint = Option(req.ll)
        val boundsHint = Option(req.bounds)
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
              Option(primaryFeature.feature.geometry.bounds).map(fBounds =>
                GeoTools.boundsIntersect(bounds, fBounds)).getOrElse(false)

            if (bbox.lo().getEarthDistance(bbox.hi()) < 200 * 1000 &&
              (bboxContainsCenter || bboxesIntersect)) {
              modifySignal(200000, "200km bbox intersection BONUS")
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

        // as a terrible tie break, things in the US > elsewhere
        // meant primarily for zipcodes
        if (primaryFeature.feature.cc == "US") {
          modifySignal(1, "US tie-break")
        }

        // no one likes counties
        if (primaryFeature.feature.cc == "US" && primaryFeature.feature.woeType == YahooWoeType.ADMIN2) {
          modifySignal(-30000, "no one likes counties")
        }

        // In autocomplete mode, prefer "tighter" interpretations
        // That is, prefer "<b>Rego Park</b>, <b>N</b>Y" to 
        // <b>Rego Park</b>, NY, <b>N</b>aalagaaffeqatigiit
        //
        // getOrdering returns a smaller # for a smaller thing
        val parentTypes = rest.map(_.fmatch.feature.woeType).sortBy(YahooWoeTypes.getOrdering)
        if (parentTypes.nonEmpty) {
          if (parentTypes(0) == YahooWoeType.ADMIN2 && req.autocomplete) {
            modifySignal( -20, "downweight county matches a lot in autocomplete mode")
          } else {
            modifySignal( -1 * YahooWoeTypes.getOrdering(parentTypes(0)), "prefer smaller parent interpretation")
          }
        }

        modifySignal( -1 * YahooWoeTypes.getOrdering(primaryFeature.feature.woeType), "prefer smaller interpretation")

        logger.ifDebug("final score %s".format(signal))
        signal
      }).getOrElse(0)
    }

    def getScore(p: Parse): Int = {
      val scoreKey = p.map(_.fmatch.id).mkString(":")
      if (!scoreMap.contains(scoreKey)) {
        scoreMap(scoreKey) = scoreParse(p)
      }

      scoreMap.getOrElse(scoreKey, -1)
    }

    def compare(a: Parse, b: Parse): Int = {
      // logger.ifDebug("Scoring %s vs %s".format(printDebugParse(a), printDebugParse(b)))

      for {
        aFeature <- a.headOption
        bFeature <- b.headOption
      } {
        var performContainHackCheck = true
        if (req.autocomplete) {
          // NOTE(blackmad): don't remember why I wrote this and it's majorly slowing us down
          // I wrote this so that "united" didn't check if " United Industrial Park" was a child of "United States of America"
          // for the completion of "United" ... this check was here for when we're deciding between "Buenos Aires" and "Buenos Aires"
          // at different admin levels.
          //
          // val aName = bestNameWithMatch(aFeature.fmatch.feature, Some(req.lang), false, Some(aFeature.phrase))
          // val bName = bestNameWithMatch(bFeature.fmatch.feature, Some(req.lang), false, Some(bFeature.phrase))
          // performContainHackCheck = (aName == bName)
        }

        if (performContainHackCheck &&
            aFeature.tokenStart == bFeature.tokenStart && 
            aFeature.tokenEnd == bFeature.tokenEnd &&
            aFeature.fmatch.feature.woeType != YahooWoeType.COUNTRY &&
            bFeature.fmatch.feature.woeType != YahooWoeType.COUNTRY
          ) {
          // if a is a parent of b, prefer b 
          if (aFeature.fmatch.scoringFeatures.parents.asScala.map(_.relatedId).has(bFeature.fmatch.id)) {
            logger.ifDebug("Preferring %s because it's a child of %s".format(printDebugParse(a), printDebugParse(b)))
            return -1
          }
          // if b is a parent of a, prefer a
          if (bFeature.fmatch.scoringFeatures.parents.asScala.map(_.relatedId).has(aFeature.fmatch.id)) {
            logger.ifDebug("Preferring %s because it's a child of %s".format(printDebugParse(b), printDebugParse(a)))
            return 1
          }
        }
      }

      val scoreA = getScore(a)
      val scoreB = getScore(b)
      if (scoreA == scoreB) {
        (a.headOption.map(_.fmatch.feature.ids.map(_.toString).hashCode).getOrElse(0).toLong -
          b.headOption.map(_.fmatch.feature.ids.map(_.toString).hashCode).getOrElse(0).toLong).signum
      } else {
        scoreB - scoreA
      }
    }
  }

  // Modifies a GeocodeFeature that is about to be returned
  // --set 'name' to the feature's best name in the request context
  // --set 'displayName' to a string that includes names of parents in the request context
  // --filter out the total set of names to a more managable number
  // --add in parent features if needed
  def fixFeature(
      f: GeocodeFeature,
      parents: Seq[GeocodeServingFeature],
      parse: Option[Parse],
      numParentsRequired: Int = 0) {
    // set name
    val name = NameUtils.bestName(f, Some(req.lang), false).map(_.name).getOrElse("")
    f.setName(name)

    // Take the least specific parent, hopefully a state
    // Yields names like "Brick, NJ, US"
    // should already be sorted, so we don't need to do: // .sorted(GeocodeServingFeatureOrdering)
    val parentsToUse: List[GeocodeServingFeature] =
      parents
        .filter(p => p.feature.woeType != YahooWoeType.COUNTRY)
        .filter(p => {
          if (f.cc != "US" && f.cc != "CA") {
            p.feature.woeType == YahooWoeType.TOWN || 
            p.feature.woeType == YahooWoeType.SUBURB ||
            (numParentsRequired > 0)
          } else {
            true
          }
        })
        .takeRight(
          if (f.cc == "US" || f.cc == "CA") {
            numParentsRequired + 1
          } else {
            numParentsRequired
          }
        ).toList

    val countryAbbrev: Option[String] = if (f.cc != req.cc) {
      if (f.cc == "GB") {
        Some("UK")
      } else {
        Some(f.cc)
      }
    } else {
      None
    }

    var namesToUse: Seq[(com.foursquare.twofishes.FeatureName, Option[String])] = Nil

    parse.foreach(p => {
      val partsFromParse: Seq[(Option[FeatureMatch], GeocodeServingFeature)] =
        p.map(fmatch => (Some(fmatch), fmatch.fmatch))
      val partsFromParents: Seq[(Option[FeatureMatch], GeocodeServingFeature)] =
       parentsToUse.filterNot(f => partsFromParse.exists(_._2 == f))
        .map(f => (None, f))
      val partsToUse = (partsFromParse ++ partsFromParents).sortBy(_._2)(GeocodeServingFeatureOrdering)
      // logger.ifDebug("parts to use: " + partsToUse)
      var i = 0
      namesToUse = partsToUse.flatMap({case(fmatchOpt, servingFeature) => {
        val name = bestNameWithMatch(servingFeature.feature, Some(req.lang), i != 0,
          fmatchOpt.map(_.phrase))
        i += 1
        name
      }})

      // strip dupe un-matched parts, so we don't have "Istanbul, Istanbul, TR"
      // don't strip out matched parts (that's the isempty check)
      // don't strip out the main feature name (index != 0)
      namesToUse = namesToUse.zipWithIndex.filterNot({case (nameMatch, index) => {
        index != 0 && nameMatch._2.isEmpty && nameMatch._1.name == namesToUse(0)._1.name
      }}).map(_._1)
    
      var (matchedNameParts, highlightedNameParts) = 
        (namesToUse.map(_._1.name),
         namesToUse.map({case(fname, highlightedName) => {
          highlightedName.getOrElse(fname.name)
        }}))

      if (partsToUse.forall(_._2.feature.woeType != YahooWoeType.COUNTRY)) {
        matchedNameParts ++= countryAbbrev.toList
        highlightedNameParts ++= countryAbbrev.toList
      }
      f.setMatchedName(matchedNameParts.mkString(", "))
      f.setHighlightedName(highlightedNameParts.mkString(", "))
    })

    // possibly clear names
    if (!req.full) {
      val names = f.names
      f.setNames(names.filter(n => 
        Option(n.flags).exists(_.contains(FeatureNameFlags.ABBREVIATION)) ||
        n.lang == req.lang ||
        n.lang == "en" ||
        namesToUse.contains(n)
      ))
    }

    val parentNames = parentsToUse.map(p =>
      NameUtils.bestName(p.feature, Some(req.lang), true).map(_.name).getOrElse(""))
       .filterNot(parentName => {
         name == parentName
       })

    f.setDisplayName((Vector(name) ++ parentNames ++ countryAbbrev.toList).mkString(", "))
  }

  def featureDistance(f1: GeocodeFeature, f2: GeocodeFeature) = {
    GeoTools.getDistance(
      f1.geometry.center.lat,
      f1.geometry.center.lng,
      f2.geometry.center.lat,
      f2.geometry.center.lng)
  }

  def boundsContains(f1: GeocodeFeature, f2: GeocodeFeature) = {
    Option(f1.geometry.bounds).exists(bb =>
      GeoTools.boundsContains(bb, f2.geometry.center)) ||
    Option(f2.geometry.bounds).exists(bb =>
      GeoTools.boundsContains(bb, f1.geometry.center))
  }

  def parsesNear(p1: Parse, p2: Parse): Boolean = {
    (p1.headOption, p2.headOption) match {
      case (Some(sf1), Some(sf2)) => {
        (featureDistance(sf1.fmatch.feature, sf2.fmatch.feature) < 15000) ||
        boundsContains(sf1.fmatch.feature, sf2.fmatch.feature)
      }
      case _ => false
    }
  }
  
  // Two jobs
  // 1) filter out woeRestrict mismatches
  // 2) try to filter out near dupe parses (based on formatting + latlng)
  def filterParses(parses: ParseSeq, removeLowRankingParses: Boolean): ParseSeq = {
    if (req.debug > 0) {
      logger.ifDebug("have %d parses in filterParses".format(parses.size))
      parses.foreach(s => logger.ifDebug(s.toString))
    }

    var goodParses = if (req.woeRestrict.size > 0) {
      parses.filter(p =>
        p.headOption.exists(f => req.woeRestrict.contains(f.fmatch.feature.woeType))
      )
    } else if (req.autocomplete) {
      parses.filterNot(p =>
        p.headOption.exists(f =>
          f.fmatch.feature.woeType == YahooWoeType.ADMIN1 ||
          f.fmatch.feature.woeType == YahooWoeType.CONTINENT ||
          f.fmatch.feature.woeType == YahooWoeType.COUNTRY
        )
      )
    } else {
      parses
    }
    logger.ifDebug("have %d parses after filtering types/woes/restricts".format(goodParses.size))

    if (removeLowRankingParses) {
      goodParses = goodParses.filter(p => p.headOption.exists(m => {
        m.fmatch.scoringFeatures.population > 50000 || p.length > 1
      }))
      logger.ifDebug("have %d parses after removeLowRankingParses".format(goodParses.size))
    }

    goodParses = goodParses.filter(p => p.headOption.exists(m => m.fmatch.scoringFeatures.canGeocode))

    goodParses
  }

  def dedupeParses(parses: ParseSeq): ParseSeq = {
    val parseIndexToNameMatch: Map[Int, Option[BestNameMatch]] =
      parses.zipWithIndex.map({case (parse, index) => {
        (index,
          parse.headOption.flatMap(f => {
            // en is cheating, sorry
            bestNameWithMatch(f.fmatch.feature, Some("en"), false, Some(f.phrase))
          })
        )
      }}).toMap

    val parseMap: Map[String, Seq[(Parse, Int)]] = parses.zipWithIndex.groupBy({case (parse, index) => {
      parseIndexToNameMatch(index).map(_._1.name).getOrElse("")
    }})

    if (req.debug > 0) {
      parseMap.foreach({case(name, parseSeq) => {
        logger.ifDebug("have %d parses for %s".format(parseSeq.size, name))
        parseSeq.foreach(p => {
          logger.ifDebug("%s: %s".format(name, printDebugParse(p._1)))
        })
      }})
    }

    def isAliasName(index: Int): Boolean = {
      parseIndexToNameMatch(index).exists(_._1.flags.contains(
        FeatureNameFlags.ALIAS))
    }

    val dedupedMap = parseMap.mapValues(parsePairs => {
      // see if there's an earlier parse that's close enough,
      // if so, return false
      parsePairs.filterNot({case (parse, index) => {
        parsePairs.exists({case (otherParse, otherIndex) => {
          // the logic here is that an alias name shoudl lose to an unaliased name
          // if we don't have the clause in this line, we end up losing both nearby interps
          ((otherIndex < index && !(isAliasName(otherIndex) && !isAliasName(index)))
            || (!isAliasName(otherIndex) && isAliasName(index))) &&
            parsesNear(parse, otherParse) 
        }})
      }})
    })

    // We have a map of [name -> List[Parse, Int]] ... extract out the parse-int pairs
    // join them, and re-sort by the int, which was their original ordering
    dedupedMap.toList.flatMap(_._2).sortBy(_._2).map(_._1)
  }

  def generateResponse(interpretations: Seq[GeocodeInterpretation]): GeocodeResponse = {
    val resp = new GeocodeResponse()
    resp.setInterpretations(interpretations)
    if (req.debug > 0) {
      resp.setDebugLines(logger.getLines)
    }
    resp
  }

  def printDebugParse(p: Parse): String = {
    val name = p.flatMap(_.fmatch.feature.names.headOption).map(_.name).mkString(", ")
    // god forgive this line of code
    val id = p.headOption.toList.flatMap(f => Option(f.fmatch.feature.ids)).flatten.headOption.map(fid =>
      "%s:%s".format(fid.source, fid.id)).getOrElse("no:id")
    "%s %s".format(id, name)
  }

  // This function signature is gross
  // Given a set of parses, create a geocode response which has fully formed
  // versions of all the features in it (names, parents)
  def hydrateParses(
    originalTokens: Seq[String],
    tokens: Seq[String],
    connectorStart: Int,
    connectorEnd: Int,
    longest: Int,
    parses: ParseSeq): Future[GeocodeResponse] = {
    val hadConnector = connectorStart != -1

    // TODO: make this configurable
    val sortedParses = parses.sorted(new ParseOrdering).take(3)

    // sortedParses.foreach(p => {
    //   logger.ifDebug(printDebugParse(p))
    // })

    val parentIds = sortedParses.flatMap(
      _.headOption.toList.flatMap(_.fmatch.scoringFeatures.parents))
    val parentOids = parentIds.map(parent => new ObjectId(parent.relatedId))
    logger.ifDebug("parent ids: " + parentOids)

    // possible optimization here: add in features we already have in our parses and don't refetch them
    store.getByObjectIds(parentOids).map(parentMap => {
      logger.ifDebug(parentMap.toString)

      val what = if (hadConnector) {
        originalTokens.take(connectorStart).mkString(" ")
      } else {
        tokens.take(tokens.size - longest).mkString(" ")
      }
      val where = tokens.drop(tokens.size - longest).mkString(" ")
      logger.ifDebug("%d sorted parses".format(sortedParses.size))
      logger.ifDebug("%s".format(sortedParses))

      var interpretations = sortedParses.map(p => {
        val fmatch = p(0).fmatch
        val feature = p(0).fmatch.feature
        val sortedParents = p(0).fmatch.scoringFeatures.parents.flatMap(parent =>
          parentMap.get(new ObjectId(parent.relatedId))).sorted(GeocodeServingFeatureOrdering)
        fixFeature(feature, sortedParents, Some(p))
        val interp = new GeocodeInterpretation()
        interp.setWhat(what)
        interp.setWhere(where)
        interp.setFeature(feature)
        if (req.debug > 0) {
          interp.setScoringFeatures(fmatch.scoringFeatures)
        }
        if (req.full) {
          interp.setParents(sortedParents.map(parentFeature => {
            val sortedParentParents = parentFeature.scoringFeatures.parents.flatMap(parent =>
              parentMap.get(new ObjectId(parent.relatedId))).sorted
            val feature = parentFeature.feature
            fixFeature(feature, sortedParentParents, None)
            feature
          }))
        }
        interp
      })

      // Find + fix ambiguous names
      // check to see if any of our features are ambiguous, even after deduping (which
      // happens outside this function). ie, there are 3 "Cobble Hill, NY"s. Which
      // are the names we get if we only take one parent component from each.
      // Find ambiguous geocodes, tell them to take more name component
      val ambiguousInterpretationsMap: Map[String, Seq[GeocodeInterpretation]] = 
        interpretations.groupBy(_.feature.displayName).filter(_._2.size > 1)
      val ambiguousInterpretations: Iterable[GeocodeInterpretation] = 
        ambiguousInterpretationsMap.flatMap(_._2).toList
      val ambiguousIdMap: Map[String, Iterable[GeocodeInterpretation]] =
        ambiguousInterpretations.groupBy(_.feature.ids.toString)

      if (ambiguousInterpretations.size > 0) {
        logger.ifDebug("had ambiguous interpretations")
        ambiguousInterpretationsMap.foreach({case (k, v) => logger.ifDebug("have %d of %s".format(v.size, k)) })
        sortedParses.foreach(p => {
          val fmatch = p(0).fmatch
          val feature = p(0).fmatch.feature
          ambiguousIdMap.getOrElse(feature.ids.toString, Nil).foreach(interp => {
            val sortedParents = p(0).fmatch.scoringFeatures.parents
              .filter(_.relationType == GeocodeRelationType.PARENT)
              .flatMap(parent =>
              parentMap.get(new ObjectId(parent.relatedId))).sorted(GeocodeServingFeatureOrdering)
            fixFeature(interp.feature, sortedParents, Some(p), 2)
          })
        })
      }
 
      generateResponse(interpretations)
    })
  }

  def geocode(): Future[GeocodeResponse] = {
    val query = req.query

    logger.ifDebug("%s --> %s".format(query, NameNormalizer.normalize(query)))

    var originalTokens =
      NameNormalizer.tokenize(NameNormalizer.normalize(query))

    val spaceAtEnd = query.takeRight(1) == " "

    geocode(originalTokens, spaceAtEnd=spaceAtEnd)
  }

  def deleteCommonWords(tokens: List[String]): List[String] = {
    val commonWords = Set(
      "city", "gemeinde", "canton", "of", "county", "gmina", "stadtteil", "district", "kommune", "prefecture", "contrada",
      "Stazione", "di", "oblast"
    )

    tokens.filterNot(t => commonWords.contains(t))
  } 

  def geocode(originalTokens: List[String],
              isRetry: Boolean = false,
              spaceAtEnd: Boolean = false
      ): Future[GeocodeResponse] = {
    logger.ifDebug("--> %s".format(originalTokens.mkString("_|_")))

    // This is awful connector parsing
    val connectorStart = originalTokens.findIndexOf(_ == "near")
    val connectorEnd = connectorStart
    val hadConnector = connectorStart != -1

    val tryHard = hadConnector

    val tokens = if (hadConnector) {
      originalTokens.drop(connectorEnd + 1)
    } else { originalTokens }

    // Need to tune the algorithm to not explode on > 10 tokens
    // in the meantime, reject.
    if (tokens.size > 10) {
      throw new Exception("too many tokens")
    }

    def maybeRetryParsing(parses: ParseSeq, parseLength: Int) = {
      val modifiedTokens = deleteCommonWords(originalTokens)
      logger.ifDebug("common words deleted: %s".format(modifiedTokens.mkString(" ")))
      if (modifiedTokens.size != originalTokens.size && !isRetry) {
        logger.ifDebug("RESTARTING common words query: %s".format(modifiedTokens.mkString(" ")))
        geocode(modifiedTokens, isRetry=true)
      } else {
        buildFinalParses(parses, parseLength)
      }
    }

    def buildFinalParses(parses: ParseSeq, parseLength: Int) = {
      val removeLowRankingParses = (parseLength != tokens.size && parseLength == 1 && !tryHard)

      // filter out parses that are really the same feature
      val parsesByMainId = parses.groupBy(_.headOption.map(_.fmatch.id))
      val actualParses = 
        parsesByMainId
          .flatMap({case (k, v) => v.sortBy(p => {
            // prefer interpretations that are shorter and don't have reused features
            val dupeWeight = if (parseHasDupeFeature(p)) { 10 } else { 0 }
            p.size + dupeWeight
          }).headOption}).toList

      val sortedParses = actualParses.sorted(new ParseOrdering)

      val filteredParses = filterParses(sortedParses, removeLowRankingParses)
      val dedupedParses = if (req.autocomplete) {
        dedupeParses(filteredParses.take(6)).take(3)
      } else {
        dedupeParses(filteredParses.take(3))
      }
      logger.ifDebug("%d parses after deduping".format(dedupedParses.size))
      dedupedParses.foreach(p =>
        logger.ifDebug("deduped parse ids: %s".format(p.map(_.fmatch.id)))
      )

      hydrateParses(originalTokens, tokens, connectorStart, connectorEnd,
        parseLength, dedupedParses)
    }

    if (req.autocomplete) {
      generateAutoParses(tokens, spaceAtEnd).flatMap(parses => {
        if (req.debug > 1) {
          parses.foreach(p => {
            logger.ifDebug("parse ids: %s".format(p.map(_.fmatch.id)))
          })
        }
        buildFinalParses(parses, 0)
      })
    } else {
      val cache = generateParses(tokens)
      val futureCache: Iterable[Future[(Int, ParseSeq)]] = cache.map({case (k, v) => {
        Future.value(k).join(v)
      }})

      Future.collect(futureCache.toList).flatMap(cache => {
        val validParseCaches: Iterable[(Int, ParseSeq)] =
          cache.filter(_._2.nonEmpty)

        if (validParseCaches.size > 0) {
          val longest = validParseCaches.map(_._1).max
          if (hadConnector && longest != tokens.size) {
            Future.value(generateResponse(Nil))
          } else {
            val longestParses = validParseCaches.find(_._1 == longest).get._2
            if (longest != tokens.size) {
              maybeRetryParsing(longestParses, longest)
            } else {
              buildFinalParses(longestParses, longest)
            }
          }
        } else {
          Future.value(generateResponse(Nil))
        }
      })
    }
  }
}
