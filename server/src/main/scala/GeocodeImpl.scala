//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Implicits._
import com.twitter.util.{Future, FuturePool}
import java.util.concurrent.ConcurrentHashMap
import org.bson.types.ObjectId
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

// TODO
// --why does rego p give me "rego"
// --why don't I get "rego" for "rego"
// --don't fetch names unless you're > 3 chars or the completion is < 4 chars?
// --need to deal with generating the right name for the completion

object GeocodeServingFeatureOrdering extends Ordering[GeocodeServingFeature] {
  def compare(a: GeocodeServingFeature, b: GeocodeServingFeature) = {
    YahooWoeTypes.getOrdering(a.feature.woeType) - YahooWoeTypes.getOrdering(b.feature.woeType)
  }
}

class GeocoderImpl(store: GeocodeStorageFutureReadService) extends LogHelper {
  type Parse = Seq[GeocodeServingFeature]
  type ParseSeq = Seq[Parse]
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
    generateParsesHelper(tokens, cache)
    cache
  }

  def buildValidParses(parses: ParseSeq, fids: Seq[ObjectId]): Future[ParseSeq] = {
    if (parses.size == 0) {
      logger.ifTrace("parses == 0, so accepting everything")
      store.getByObjectIds(fids).map(_.map(p => List(p._2)).toList)
    } else {
      val validParsePairs: Seq[(Parse, ObjectId)] = parses.flatMap(parse => {
        println("checking %d fids against %s".format(fids.size, parse))
        fids.flatMap(fid => {
          // logger.ifTrace("checking if %s is a parent of %s".format(
          //   fid, parse))
          val isValid = parse.exists(_.scoringFeatures.parents.contains(fid))
          if (isValid) {
           // logger.ifTrace("twas")
            Some((parse, fid))
          } else {
           // logger.ifTrace("wasn't")
            None
          }
        })
      })

      val fidsToFetch: Seq[ObjectId] = validParsePairs.map(_._2).toSet.toList
      val featuresMapF = store.getByObjectIds(fidsToFetch)

      featuresMapF.map(featureMap => {
        validParsePairs.flatMap({case(parse, fid) => {
          featureMap.get(fid).map(feature => {
            parse ++ List(feature)
          })
        }})
      })
    }
  }

  def generateAutoParses(tokens: List[String]): Future[ParseSeq] = {
    generateAutoParsesHelper(tokens, Nil)
  }

  def generateAutoParsesHelper(tokens: List[String], parses: ParseSeq): Future[ParseSeq] = {
    if (tokens.size == 0) {
      Future.value(parses)
    } else {
      val validParses: Seq[Future[ParseSeq]] = 1.to(tokens.size).map(i => {
        val query = tokens.take(i).mkString(" ")
        val isEnd = (i == tokens.size)

        val fidsF: Future[Seq[ObjectId]] = if (isEnd) {
          val fids = store.getIdsByNamePrefix(query)
          // TODO(blackmad): possibly prune here + OR exact query
          fids
        } else {
          store.getIdsByName(query)
        }

        val nextParseF: Future[ParseSeq] = fidsF.flatMap(featureIds => {
          logger.ifTrace("looking at: %s (is end? %s)".format(query, isEnd))
          println("previous parses: %s".format(parses))
          println("looking for featureIds: %s".format(featureIds.size))
          buildValidParses(parses, featureIds)
        })

        nextParseF.flatMap(nextParses => {
          if (nextParses.size == 0) {
            Future.value(Nil)
          } else {
            generateAutoParsesHelper(tokens.drop(i), nextParses)
          }
        })
      })
      Future.collect(validParses).map(vp => {
        println("vp: " + vp)
        vp.flatten
      })
    }
  }

  def generateParsesHelper(tokens: List[String], cache: ParseCache): Future[ParseSeq] = {
    val cacheKey = tokens.size
    if (tokens.size == 0) {
      Future.value(List(Nil))
    } else {
      if (!cache.contains(cacheKey)) {
        val searchStrs: Seq[String] =
          1.to(tokens.size).map(i => tokens.take(i).mkString(" "))

        val toEndStr = tokens.mkString(" ")

        val featuresFs: Seq[Future[Parse]] =
          for ((searchStr, i) <- searchStrs.zipWithIndex) yield {
            store.getByName(searchStr)
          }

         // Compute subparses in parallel (scatter)
         val subParsesFs: Seq[Future[ParseSeq]] =
           1.to(tokens.size).map(i => generateParsesHelper(tokens.drop(i), cache))

         // Collect the results of all the features and subparses (gather)
         val featureListsF: Future[ParseSeq] = Future.collect(featuresFs)
         val subParseSeqsF: Future[Seq[ParseSeq]] = Future.collect(subParsesFs)

         val validParsesF: Future[ParseSeq] =
          for((featureLists, subParseSeqs) <- featureListsF.join(subParseSeqsF)) yield {
            val validParses: ParseSeq = (
              featureLists.zip(subParseSeqs).flatMap({case(features: Parse, subparses: ParseSeq) => {
                (for {
                  f <- features
                  val _ = logger.ifTrace("looking at %s".format(f))
                  p <- subparses
                  val _ = logger.ifTrace("sub_parse: %s".format(p))
                } yield {
                  val parse: Parse = Vector(f) ++ p
                  if (isValidParse(parse)) {
                    logger.ifTrace("VALID -- adding to %d".format(cacheKey))
                    logger.ifTrace("adding parse " + parse)
                    logger.ifTrace("sorted " + parse.sorted(GeocodeServingFeatureOrdering))
                    Some(parse.sorted(GeocodeServingFeatureOrdering))
                  } else {
                    logger.ifTrace("INVALID")
                    None
                  }
                }).flatten
              }})
            )
            validParses.toList
          }

        validParsesF.onSuccess(validParses => {
          logger.ifTrace("setting %d to %s".format(cacheKey, validParses))
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
      val sorted_parse = parse.sorted(GeocodeServingFeatureOrdering)
      (for {
        first <- sorted_parse.lift(0)
        second <- sorted_parse.lift(1)
      } yield {
        first.feature.woeType == YahooWoeType.POSTAL_CODE &&
        isValidParseHelper(sorted_parse.drop(1)) &&
        GeoTools.getDistance(
          second.feature.geometry.center.lat,
          second.feature.geometry.center.lng,
          first.feature.geometry.center.lat,
          first.feature.geometry.center.lng) < 200000
      }).getOrElse(false)
    }
  }

  def isValidParseHelper(parse: Parse): Boolean = {
    if (parse.size <= 1) {
      true
    } else {
      val most_specific = parse(0)
      val rest = parse.drop(1)
      rest.forall(f => {
        f.id == most_specific.id ||
        most_specific.scoringFeatures.parents.contains(f.id)
      })
    }
  }

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

   def bestName(f: GeocodeFeature, lang: Option[String], preferAbbrev: Boolean): Option[FeatureName] = {
     f.names.sorted(new FeatureNameComparator(lang, preferAbbrev)).headOption
   }
 
  class ParseOrdering(llHint: GeocodePoint, ccHint: String) extends Ordering[Parse] {
    // Higher is better
    def scoreParse(parse: Parse): Int = {
      parse.headOption.map(primaryFeature => {
        val rest = parse.drop(1)
        var signal = primaryFeature.scoringFeatures.population

        // if we have a repeated feature, downweight this like crazy
        // so st petersburg, st petersburg works, but doesn't break new york, ny
        if (rest.contains(primaryFeature)) {
          signal -= 100000000
        }

        if (primaryFeature.feature.geometry.bounds != null) {
          signal += 1000
        }

        // prefer a more aggressive parse ... bleh
        // this prefers "mt laurel" over the town of "laurel" in "mt" (montana)
        signal -= 20000 * parse.length

        // Matching country hint is good
        if (Option(ccHint).exists(_ == primaryFeature.feature.cc)) {
          signal += 10000
        }

        Option(llHint).foreach(ll => {
          signal -= (GeoTools.getDistance(ll.lat, ll.lng,
              primaryFeature.feature.geometry.center.lat,
              primaryFeature.feature.geometry.center.lng) / 100)
        })

        signal += primaryFeature.scoringFeatures.boost

        // as a terrible tie break, things in the US > elsewhere
        // meant primarily for zipcodes
        if (primaryFeature.feature.cc == "US") {
          signal += 1
        }
        signal
      }).getOrElse(0)
    }

    def compare(a: Parse, b: Parse) = {
      scoreParse(b) - scoreParse(a)
    }
  }

  // def hydrateParses(parses: Seq[Parse]): Future[ParseSeq] = {
  //   val ids: Seq[String] = parses.flatMap(_.map(_.id))
  //   val oids = ids.map(i => new ObjectId(i))
  //   store.getByObjectIds(oids).map(geocodeRecordMap =>
  //     parses.map(parse => {
  //       parse.flatMap(feature => {
  //         geocodeRecordMap.get(new ObjectId(feature.id))
  //       })
  //     })
  //   )
  // }

  def fixFeature(req: GeocodeRequest, f: GeocodeFeature, parents: Seq[GeocodeServingFeature]) {
    // set name
    val name = bestName(f, Some(req.lang), false).map(_.name).getOrElse("")
    f.setName(name)

    // possibly clear names
    if (!req.full) {
      val names = f.names
      f.setNames(names.filter(n => 
        n.flags.contains(FeatureNameFlags.ABBREVIATION) ||
        n.lang == req.lang ||
        n.lang == "en"
      ))
    }

    // Take the least specific parent, hopefully a state
    // Yields names like "Brick, NJ, US"
    val parentsToUse =
      parents
        .filter(p => p.feature.woeType != YahooWoeType.COUNTRY)
        .sorted(GeocodeServingFeatureOrdering)
        .lastOption
        .toList

    val countryAbbrev: Option[String] = if (f.cc != req.cc) {
      Some(f.cc)
    } else {
      None
    }

    val parentNames = parentsToUse.map(p =>
      bestName(p.feature, Some(req.lang), true).map(_.name).getOrElse(""))
    f.setDisplayName((Vector(name) ++ parentNames ++ countryAbbrev.toList).mkString(", "))
  }

  def featureDistance(f1: GeocodeFeature, f2: GeocodeFeature) = {
    GeoTools.getDistance(
      f1.geometry.center.lat,
      f1.geometry.center.lng,
      f2.geometry.center.lat,
      f2.geometry.center.lng)
  }

  def parseDistance(p1: Parse, p2: Parse): Int = {
    (p1.headOption, p2.headOption) match {
      case (Some(sf1), Some(sf2)) => featureDistance(sf1.feature, sf2.feature)
      case _ => 0
    }
  }

  def filterDupeParses(parses: ParseSeq): ParseSeq = {
    val parseMap: Map[String, Seq[(Parse, Int)]] = parses.zipWithIndex.groupBy({case (parse, index) => {
      parse.headOption.flatMap(servingFeature => 
         // en is cheating, sorry
         bestName(servingFeature.feature, Some("en"), false).map(_.name)
      ).getOrElse("")
    }})


    val dedupedMap = parseMap.mapValues(parsePairs => {
      // see if there's an earlier parse that's close enough,
      // if so, return false
      parsePairs.filterNot({case (parse, index) => {
        parsePairs.exists({case (otherParse, otherIndex) => {
          otherIndex < index && parseDistance(parse, otherParse) < 1000
        }})
      }})
    })

    // We have a map of [name -> List[Parse, Int]] ... extract out the parse-int pairs
    // join them, and re-sort by the int, which was their original ordering
    dedupedMap.toList.flatMap(_._2).sortBy(_._2).map(_._1)
  }

  def hydrateParses(
    req: GeocodeRequest,
    originalTokens: Seq[String],
    tokens: Seq[String],
    connectorStart: Int,
    connectorEnd: Int,
    longest: Int,
    parses: ParseSeq): Future[GeocodeResponse] = {
    val hadConnector = connectorStart != -1

    // TODO: make this configurable
    val sortedParses = parses.sorted(new ParseOrdering(req.ll, req.cc)).take(3)

    val parentIds = sortedParses.flatMap(_.headOption.toList.flatMap(_.scoringFeatures.parents))
    val parentOids = parentIds.map(i => new ObjectId(i))
    logger.ifTrace("parent ids: " + parentOids)
    store.getByObjectIds(parentOids).map(parentMap => {
      logger.ifTrace(parentMap.toString)

      val what = if (hadConnector) {
        originalTokens.take(connectorStart).mkString(" ")
      } else {
        tokens.take(tokens.size - longest).mkString(" ")
      }
      val where = tokens.drop(tokens.size - longest).mkString(" ")
      logger.ifTrace("%d sorted parses".format(sortedParses.size))
      logger.ifTrace("%s".format(sortedParses))

      new GeocodeResponse(sortedParses.map(p => {
        val feature = p(0).feature
        val sortedParents = p(0).scoringFeatures.parents.flatMap(id => parentMap.get(new ObjectId(id))).sorted(GeocodeServingFeatureOrdering)
        fixFeature(req, feature, sortedParents)
        val interp = new GeocodeInterpretation(what, where, feature)
        if (!req.autocomplete) {
          interp.setParents(sortedParents.map(parentFeature => {
            val sortedParentParents = parentFeature.scoringFeatures.parents.flatMap(id => parentMap.get(new ObjectId(id))).sorted
            val feature = parentFeature.feature
            fixFeature(req, feature, sortedParentParents)
            feature
          }))
        }
        interp
      }))
    })
  }

  def geocode(req: GeocodeRequest): Future[GeocodeResponse] = {
    val query = req.query

    logger.ifTrace("%s --> %s".format(query, NameNormalizer.normalize(query)))

    val originalTokens = NameNormalizer.tokenize(NameNormalizer.normalize(query))
    logger.ifTrace("--> %s".format(originalTokens.mkString("_|_")))

    // This is awful connector parsing
    val connectorStart = originalTokens.findIndexOf(_ == "near")
    val connectorEnd = connectorStart
    val hadConnector = connectorStart != -1

    val tokens = if (hadConnector) {
      originalTokens.drop(connectorEnd + 1)
    } else { originalTokens }

    // Need to tune the algorithm to not explode on > 10 tokens
    // in the meantime, reject.
    if (tokens.size > 10) {
      throw new Exception("too many tokens")
    }


    if (req.autocomplete) {
      generateAutoParses(tokens).flatMap(parses => {
        hydrateParses(req, originalTokens, tokens, connectorStart, connectorEnd, 0, parses)
      })
    } else {
      val cache = generateParses(tokens)
      val futureCache: Iterable[Future[(Int, ParseSeq)]] = cache.map({case (k, v) => {
        Future.value(k).join(v)
      }})

      Future.collect(futureCache.toList).flatMap(cache => {
        val validParseCaches: Iterable[(Int, ParseSeq)] = cache.filter(_._2.nonEmpty)

        if (validParseCaches.size > 0) {
          val longest = validParseCaches.map(_._1).max
          if (hadConnector && longest != tokens.size) {
            Future.value(new GeocodeResponse(Nil))
          } else {
            val longestParses = validParseCaches.find(_._1 == longest).get._2
            val sortedParses = filterDupeParses(longestParses.sorted(new ParseOrdering(req.ll, req.cc)).take(3))
            hydrateParses(req, originalTokens, tokens, connectorStart, connectorEnd, longest, longestParses)
          }
        } else {
          Future.value(new GeocodeResponse(Nil))
        }
      })
    }
  }
}
