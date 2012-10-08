//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Implicits._
import com.twitter.util.{Future, FuturePool}
import java.util.concurrent.ConcurrentHashMap
import java.util.Date
import org.bson.types.ObjectId
import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}

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
  class MemoryLogger {
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

        validParsesF.onSuccess(validParses => {
          logger.ifDebug("setting %d to %s".format(cacheKey, validParses))
        })
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
      logger.ifDebug("most specific: " + most_specific)
      logger.ifDebug("most specific: parents" + most_specific.fmatch.scoringFeatures.parents)
      val rest = parse.drop(1)
      rest.forall(f => {
        logger.ifDebug("checking if %s in parents".format(f.fmatch.id))
        f.fmatch.id == most_specific.fmatch.id ||
        most_specific.fmatch.scoringFeatures.parents.contains(f.fmatch.id)
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
        logger.ifDebug("checking %d fids against %s".format(fids.size, parse.map(_.fmatch.id)))
        logger.ifDebug("these are the fids of my parse: %s".format(fids))
        fids.flatMap(fid => {
           logger.ifDebug("checking if %s is an unused parent of %s".format(
             fid, parse.map(_.fmatch.id)))
          val isValid = parse.exists(_.fmatch.scoringFeatures.parents.contains(fid)) &&
            !parse.exists(_.fmatch.id.toString == fid.toString)
          if (isValid) {
            logger.ifDebug("HAD %s as a parent".format(fid))
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
    val normalizedName = NameNormalizer.normalize(name.name)
    if (isEnd) {
      normalizedName.startsWith(query)
    } else {
      normalizedName == query
    }
  }

  def generateAutoParsesHelper(tokens: List[String], offset: Int, parses: ParseSeq, spaceAtEnd: Boolean): Future[ParseSeq] = {
    if (tokens.size == 0) {
      Future.value(parses)
    } else {
      val validParses: Seq[Future[ParseSeq]] = 1.to(tokens.size).map(i => {
        val query = tokens.take(i).mkString(" ")
        val isEnd = (i == tokens.size)

        val possibleParents = parses.flatMap(_.flatMap(_.fmatch.scoringFeatures.parents))
          .map(id => new ObjectId(id))

        val fidsF: Future[Seq[ObjectId]] = 
          if (parses.size == 0) {
            if (isEnd) {
              logger.ifDebug("looking at prefix: %s".format(query))
              if (spaceAtEnd) {
                Future.collect(List(
                  store.getIdsByNamePrefix(query + " "),
                  store.getIdsByName(query)
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

  // Given an optional language and an abbreviation preference, find the best name
  // for a feature in the current context.
  class FeatureNameComparator(lang: Option[String], preferAbbrev: Boolean) extends Ordering[FeatureName] {
    def compare(a: FeatureName, b: FeatureName) = {
      scoreName(b) - scoreName(a)
    }

    def scoreName(name: FeatureName): Int = {
      var score = 0
      if (name.flags.contains(FeatureNameFlags.PREFERRED)) {
        score += 1
      }
      if (lang.exists(_ == name.lang)) {
        score += 2
      }
      if (name.flags.contains(FeatureNameFlags.ABBREVIATION) && preferAbbrev) {
        score += 4
      }
      score
    }
  }

  // Another delightful hack. We don't save a pointer to the specific name we matched
  // in our inverted index, instead, if we know which tokens matched this feature,
  // we look for the name that, when normalized, matches the query substring.
  // i.e. for [New York, NY, Nueva York], if we know that we matched "neuv", we
  // look for the names that started with that.
  def bestName(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean
  ): Option[FeatureName] = {
    f.names.sorted(new FeatureNameComparator(lang, preferAbbrev)).headOption
  }

  type BestNameMatch = (FeatureName, Option[String])
  var nameMatchMap =
    new scala.collection.mutable.HashMap[String, Option[BestNameMatch]]

  def bestNameWithMatch(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean,
    matchedStringOpt: Option[String]
  ): Option[BestNameMatch] = {
    val hashKey = "%s:%s:%s:%s".format(f.ids, lang, preferAbbrev, matchedStringOpt)
    if (nameMatchMap.contains(hashKey)) {
      return nameMatchMap(hashKey)
    }

    val ret = matchedStringOpt.flatMap(matchedString => {
      val nameCandidates = f.names.filter(n => {
        val normalizedName = NameNormalizer.normalize(n.name)
        // logger.ifDebug("Does %s start with %s?".format(normalizedName, matchedString))
        normalizedName.startsWith(matchedString)
      })

      logger.ifDebug("name candidates: " + nameCandidates)
          
      val bestNameMatch = nameCandidates.sorted(new FeatureNameComparator(lang, preferAbbrev)).headOption
      logger.ifDebug("best name match: " + bestNameMatch)
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

    nameMatchMap(hashKey) = ret
    ret
  }

  // Comparator for parses, we score by a number of different features
  // 
  class ParseOrdering(llHint: GeocodePoint, ccHint: String) extends Ordering[Parse] {
    var scoreMap = new scala.collection.mutable.HashMap[String, Int]

    // Higher is better
    def scoreParse(parse: Parse): Int = {
      parse.headOption.map(primaryFeatureMatch => {
        val primaryFeature = primaryFeatureMatch.fmatch
        val rest = parse.drop(1)
        var signal = primaryFeature.scoringFeatures.population

        def modifySignal(value: Int, debug: String) {
          logger.ifDebug("%s: %s + %s = %s".format(
            debug, signal, value, signal + value))
          signal += value
        }

        logger.ifDebug("Scoring %s".format(printDebugParse(parse)))

        // if we have a repeated feature, downweight this like crazy
        // so st petersburg, st petersburg works, but doesn't break new york, ny
        if (rest.exists(_.fmatch.feature.ids == primaryFeature.feature.ids)) {
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
        if (Option(ccHint).exists(_ == primaryFeature.feature.cc)) {
          modifySignal(10000, "country code match")
        }

        // Penalize far-away things
        Option(llHint).foreach(ll => {
          val distancePenalty = (GeoTools.getDistance(ll.lat, ll.lng,
              primaryFeature.feature.geometry.center.lat,
              primaryFeature.feature.geometry.center.lng) / 100)
          modifySignal(-distancePenalty, "distance penalty")          
        })

        // manual boost added at indexing time
        if (primaryFeature.scoringFeatures.boost != 0) {
          modifySignal(primaryFeature.scoringFeatures.boost, "manual boost")
        }

        // as a terrible tie break, things in the US > elsewhere
        // meant primarily for zipcodes
        if (primaryFeature.feature.cc == "US") {
          modifySignal(1, "US tie-break")
        }

        // In autocomplete mode, prefer "tighter" interpretations
        // That is, prefer "<b>Rego Park</b>, <b>N</b>Y" to 
        // <b>Rego Park</b>, NY, <b>N</b>aalagaaffeqatigiit
        //
        // getOrdering returns a smaller # for a smaller thing
        if (req.autocomplete) {
          val parentTypes = rest.map(_.fmatch.feature.woeType).sortBy(YahooWoeTypes.getOrdering)
          if (parentTypes.nonEmpty) {
            if (parentTypes(0) != YahooWoeType.ADMIN2) {
              modifySignal( -1 * YahooWoeTypes.getOrdering(parentTypes(0)), "prefer smaller parent interpretation")
            } else {
              modifySignal( -20, "downweight county matches a big")
            }
          }
        }

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
          val aName = bestNameWithMatch(aFeature.fmatch.feature, Some(req.lang), false, Some(aFeature.phrase))
          val bName = bestNameWithMatch(bFeature.fmatch.feature, Some(req.lang), false, Some(bFeature.phrase))
          performContainHackCheck = (aName == bName)
        }

        if (performContainHackCheck &&
            aFeature.tokenStart == bFeature.tokenStart && 
            aFeature.tokenEnd == bFeature.tokenEnd) {
          // if a is a parent of b, prefer b 
          if (aFeature.fmatch.scoringFeatures.parents.contains(bFeature.fmatch.id)) {
            logger.ifDebug("Preferring %s because it's a child of %s".format(printDebugParse(a), printDebugParse(b)))
            return -1
          }
          // if b is a parent of a, prefer a
          if (bFeature.fmatch.scoringFeatures.parents.contains(aFeature.fmatch.id)) {
            logger.ifDebug("Preferring %s because it's a child of %s".format(printDebugParse(b), printDebugParse(a)))
            return 1
          }
        }
      }
      getScore(b) - getScore(a)
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
      numComponentsToTake: Int = 1) {
    // set name
    val name = bestName(f, Some(req.lang), false).map(_.name).getOrElse("")
    f.setName(name)

    // Take the least specific parent, hopefully a state
    // Yields names like "Brick, NJ, US"
    // should already be sorted, so we don't need to do: // .sorted(GeocodeServingFeatureOrdering)
    val parentsToUse: List[GeocodeServingFeature] =
      parents
        .filter(p => p.feature.woeType != YahooWoeType.COUNTRY)
        .filter(p => {
          if (f.cc != "US" && f.cc != "CA") {
            p.feature.woeType != YahooWoeType.ADMIN1
          } else {
            true
          }
        })
        .takeRight(numComponentsToTake)
        .toList


    val countryAbbrev: Option[String] = if (f.cc != req.cc) {
      Some(f.cc)
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
        n.flags.contains(FeatureNameFlags.ABBREVIATION) ||
        n.lang == req.lang ||
        n.lang == "en" ||
        namesToUse.contains(n)
      ))
    }

    val parentNames = parentsToUse.map(p =>
      bestName(p.feature, Some(req.lang), true).map(_.name).getOrElse(""))
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
    logger.ifDebug("have %d parses in filterParses".format(parses.size))
    parses.foreach(s => logger.ifDebug(s.toString))

    var goodParses = if (req.woeRestrict.size > 0) {
      parses.filter(p =>
        p.headOption.exists(f => req.woeRestrict.contains(f.fmatch.feature.woeType))
      )
    } else if (req.autocomplete) {
      parses.filterNot(p =>
        p.headOption.exists(f => f.fmatch.feature.woeType == YahooWoeType.COUNTRY || f.fmatch.feature.woeType == YahooWoeType.ADMIN1 || f.fmatch.feature.woeType == YahooWoeType.CONTINENT)
      )
    } else {
      parses
    }

    if (removeLowRankingParses) {
      goodParses = goodParses.filter(p => p.headOption.exists(m => {
        m.fmatch.scoringFeatures.population > 50000 || p.length > 1
      }))
    }
  }

  def dedupeParses(parses: ParseSeq): ParseSeq = {
    val parseMap: Map[String, Seq[(Parse, Int)]] = goodParses.zipWithIndex.groupBy({case (parse, index) => {
      parse.headOption.flatMap(f => 
        // en is cheating, sorry
        if (req.autocomplete) {
           bestNameWithMatch(f.fmatch.feature, Some("en"), false, Some(f.phrase)).map(_._1.name)
        } else {
           bestName(f.fmatch.feature, Some("en"), false).map(_.name)
        }
      ).getOrElse("")
    }})

    parseMap.foreach({case(name, parseSeq) => {
      logger.ifDebug("have %d parses for %s".format(parseSeq.size, name)) 
    }})

    val dedupedMap = parseMap.mapValues(parsePairs => {
      // see if there's an earlier parse that's close enough,
      // if so, return false
      parsePairs.filterNot({case (parse, index) => {
        parsePairs.exists({case (otherParse, otherIndex) => {
          otherIndex < index && parsesNear(parse, otherParse) 
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
    val sortedParses = parses.sorted(new ParseOrdering(req.ll, req.cc)).take(3)

    // sortedParses.foreach(p => {
    //   logger.ifDebug(printDebugParse(p))
    // })

    val parentIds = sortedParses.flatMap(
      _.headOption.toList.flatMap(_.fmatch.scoringFeatures.parents))
    val parentOids = parentIds.map(i => new ObjectId(i))
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
        val sortedParents = p(0).fmatch.scoringFeatures.parents.flatMap(id =>
          parentMap.get(new ObjectId(id))).sorted(GeocodeServingFeatureOrdering)
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
            val sortedParentParents = parentFeature.scoringFeatures.parents.flatMap(id => parentMap.get(new ObjectId(id))).sorted
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
      // val ambiguousInterpretations: Iterable[GeocodeInterpretation] = 
      //   interpretations.groupBy(_.feature.displayName).filter(_._2.size > 1).flatMap(_._2)

      // if (ambiguousInterpretations.size > 0) {
      //   sortedParses.foreach(p => {
      //     val fmatch = p(0).fmatch
      //     val feature = p(0).fmatch.feature
      //     val sortedParents = p(0).fmatch.scoringFeatures.parents.flatMap(id =>
      //       parentMap.get(new ObjectId(id))).sorted(GeocodeServingFeatureOrdering)

      //     if (feature.ids == 
      //     fixFeature(feature, sortedParents, Some(p))
      //   })
 
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
      val actualParses = 
        parses.groupBy(_.headOption.map(_.fmatch.id))
          .flatMap({case (k, v) => v.headOption}).toList

      val sortedParses = actualParses.sorted(new ParseOrdering(req.ll, req.cc))

      val filteredParses = filterParses(sortedParses, removeLowRankingParses)
      val dedupedParses = dedupeParses(filteredParses.take(3))
      dedupedParses.foreach(p =>
        logger.ifDebug("deduped parse ids: %s".format(p.map(_.fmatch.id)))
      )

      hydrateParses(originalTokens, tokens, connectorStart, connectorEnd,
        parseLength, dedupedParses)
    }

    if (req.autocomplete) {
      generateAutoParses(tokens, spaceAtEnd).flatMap(parses => {
        parses.foreach(p => {
          logger.ifDebug("parse ids: %s".format(p.map(_.fmatch.id)))
        })
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
