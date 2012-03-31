//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofish

import com.foursquare.twofish.Implicits._
import com.twitter.util.{Future, FuturePool}
import java.util.concurrent.ConcurrentHashMap
import org.bson.types.ObjectId
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

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

  def generateParsesHelper(tokens: List[String], cache: ParseCache): Future[ParseSeq] = {
    val cacheKey = tokens.size
    if (tokens.size == 0) {
      Future.value(List(Nil))
    } else {
      if (!cache.contains(cacheKey)) {
        val searchStrs: Seq[String] =
          1.to(tokens.size).map(i => tokens.take(i).mkString(" "))

        val featuresFs: Seq[Future[Parse]] =
          for ((searchStr, i) <- searchStrs.zipWithIndex) yield {
            val result: Future[Parse] = store.getByName(searchStr)
            result
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

        // prefer a more aggressive parse ... bleh
        // this prefers "mt laurel" over the town of "laurel" in "mt" (montana)
        signal -= 20000 * parse.length

        // Matching country hint is good
        if (Option(ccHint).exists(_ == primaryFeature.feature.cc)) {
          signal += 100000
        }

        Option(llHint).foreach(ll => {
          signal -= GeoTools.getDistance(ll.lat, ll.lng,
              primaryFeature.feature.geometry.center.lat,
              primaryFeature.feature.geometry.center.lng)
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

  def hydrateParses(parses: Seq[Parse]): Future[ParseSeq] = {
    val ids: Seq[String] = parses.flatMap(_.map(_.id))
    val oids = ids.map(i => new ObjectId(i))
    store.getByObjectIds(oids).map(geocodeRecordMap =>
      parses.map(parse => {
        parse.flatMap(feature => {
          geocodeRecordMap.get(new ObjectId(feature.id))
        })
      })
    )
  }

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

    // set displayName
    val parentsToUse = parents.filter(p => {
      // if it's a country and it's the same country as the request
      // don't use it
      val sameCountry = p.feature.woeType == YahooWoeType.COUNTRY && p.feature.cc == req.cc
      !sameCountry
    })

    val parentNames = parentsToUse.map(p =>
      bestName(p.feature, Some(req.lang), true).map(_.name).getOrElse(""))
    f.setDisplayName((Vector(name) ++ parentNames).mkString(", "))
  }

  def geocode(req: GeocodeRequest): Future[GeocodeResponse] = {
    val query = req.query

    logger.ifTrace("%s --> %s".format(query, NameNormalizer.normalize(query)))

    val tokens = NameNormalizer.tokenize(NameNormalizer.normalize(query))
    logger.ifTrace("--> %s".format(tokens.mkString("_|_")))

    /// CONNECTOR PARSING GOES HERE
    val cache = generateParses(tokens)
    val futureCache: Iterable[Future[(Int, ParseSeq)]] = cache.map({case (k, v) => {
      Future.value(k).join(v)
    }})

    Future.collect(futureCache.toList).flatMap(cache => {
      val validParseCaches: Iterable[(Int, ParseSeq)] = cache.filter(_._2.nonEmpty)

      if (validParseCaches.size > 0) {
        val longest = validParseCaches.map(_._1).max
        val longestParses = validParseCaches.find(_._1 == longest).get._2

        hydrateParses(longestParses).flatMap(hydratedParses => {
          // TODO: make this configurable
          val sortedParses = hydratedParses.sorted(new ParseOrdering(req.ll, req.cc)).take(3)

          val parentIds = sortedParses.flatMap(_.headOption.toList.flatMap(_.scoringFeatures.parents))
          val parentOids = parentIds.map(i => new ObjectId(i))
          logger.ifTrace("parent ids: " + parentOids)
          store.getByObjectIds(parentOids).map(parentMap => {
            logger.ifTrace(parentMap.toString)

            val what = tokens.take(tokens.size - longest).mkString(" ")
            val where = tokens.drop(tokens.size - longest).mkString(" ")
            logger.ifTrace("%d sorted parses".format(sortedParses.size))

            // need to fix names here
            new GeocodeResponse(sortedParses.map(p => {
            //  p(0).setScoringFeatures(null)
              val feature = p(0).feature
              val sortedParents = p(0).scoringFeatures.parents.flatMap(id => parentMap.get(new ObjectId(id))).sorted(GeocodeServingFeatureOrdering)
              fixFeature(req, feature, sortedParents)
              val interp = new GeocodeInterpretation(what, where, feature)
              interp.setParents(sortedParents.map(parentFeature => {
                val sortedParentParents = parentFeature.scoringFeatures.parents.flatMap(id => parentMap.get(new ObjectId(id))).sorted
                val feature = parentFeature.feature
                fixFeature(req, feature, sortedParentParents)
                feature
              }))
              interp
            }))
          })
        })
      } else {
        Future.value(new GeocodeResponse(Nil))
      }
    })
  }
}
