//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofish

import com.twitter.util.{Future, FuturePool}
import scala.collection.JavaConversions._
import com.foursquare.twofish.Implicits._
import scala.collection.mutable.HashMap
import org.bson.types.ObjectId
import java.util.concurrent.ConcurrentHashMap

// TODO
// fix displayName!
// fix returned name
// document new flow

class GeocoderImpl(pool: FuturePool, _store: GeocodeStorageReadService) extends LogHelper {
  val store = new GeocodeStorageFutureReadService(_store, pool)
  type Parse = Seq[GeocodeServingFeature]
  type ParseSeq = Seq[Parse]
  type ParseList = List[Parse]
  type ParseCache = ConcurrentHashMap[Int, Future[ParseList]]

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

  def generateParsesHelper(tokens: List[String], cache: ParseCache): Future[ParseList] = {
    val cacheKey = tokens.size
    if (tokens.size == 0) {
      Future.value(List(Nil))
    } else {
      if (!cache.contains(cacheKey)) {
        val searchStrs: List[String] =
          1.to(tokens.size).map(i => tokens.take(i).mkString(" ")).toList

        val featuresFs: List[Future[Parse]] =
          for ((searchStr, i) <- searchStrs.zipWithIndex) yield {
            val result: Future[List[GeocodeServingFeature]] = store.getByName(searchStr).map(_.toList)
            result
          }

         // Compute subparses in parallel (scatter)
         val subParsesFs: List[Future[ParseList]] =
           1.to(tokens.size).map(i => generateParsesHelper(tokens.drop(i), cache)).toList

         // Collect the results of all the features and subparses (gather)
         val featureListsF: Future[ParseSeq] = Future.collect(featuresFs)
         val subparseListsF: Future[Seq[ParseList]] = Future.collect(subParsesFs)

         val validParsesF: Future[ParseList] =
          for((featureLists, subparseLists) <- featureListsF.join(subparseListsF)) yield {
            val validParses: Seq[List[GeocodeServingFeature]] = (
              featureLists.zip(subparseLists).flatMap({case(features: Parse, subparses: ParseSeq) => {
                (for {
                  f <- features.toList
                  val _ = logger.ifTrace("looking at %s".format(f))
                  p <- subparses
                  val _ = logger.ifTrace("sub_parse: %s".format(p))
                } yield {
                  val parse: List[GeocodeServingFeature] = f :: p.toList
                  if (isValidParse(parse)) {
                    logger.ifTrace("VALID -- adding to %d".format(cacheKey))
                    Some(parse.sorted)
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
      parse.sorted match {
        case first :: second :: rest => {
          first.feature.woeType == YahooWoeType.POSTAL_CODE &&
          isValidParseHelper(second :: rest) &&
          GeoTools.getDistance(
            second.feature.geometry.center.lat,
            second.feature.geometry.center.lng,
            first.feature.geometry.center.lat,
            first.feature.geometry.center.lng) < 200000
        }
        case  _ => false
      }
    }
  }

  def isValidParseHelper(parse: Parse): Boolean = {
    parse match {
      case Nil => true
      case f :: Nil => true
      case most_specific :: rest => {
        rest.forall(f => {
          f.id == most_specific.id ||
          most_specific.scoringFeatures.parents.contains(f.id)
        })
      }
    }
  }

//   class DisplayNameOrdering(lang: Option[String], preferAbbrev: Boolean) extends Ordering[DisplayName] {
//   def compare(a: DisplayName, b: DisplayName) = {
//     scoreName(b) - scoreName(a)
//   }

//   def scoreName(name: DisplayName): Int = {
//     var score = 0
//     if (name.preferred) {
//       score += 1
//     }
//     if (lang.exists(_ == name.lang)) {
//       score += 2
//     }
//     if (name.lang == "abbr" && preferAbbrev) {
//       score += 4
//     }
//     score
//   }
// }

// def bestName(lang: Option[String], preferAbbrev: Boolean): Option[DisplayName] = {
//   displayNames.sorted(new DisplayNameOrdering(lang, preferAbbrev)).headOption
// }
 
  class ParseOrdering(llHint: GeocodePoint, ccHint: String) extends Ordering[Parse] {
    // Higher is better
    def scoreParse(parse: Parse): Int = {
      parse match {
        case primaryFeature :: rest => {
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
        }
        case Nil => 0
      }
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

  def geocode(req: GeocodeRequest): Future[GeocodeResponse] = {
    val query = req.query

    logger.ifTrace("%s --> %s".format(query, NameNormalizer.normalize(query)))

    val tokens = NameNormalizer.tokenize(NameNormalizer.normalize(query))
    logger.ifTrace("--> %s".format(tokens.mkString("_|_")))

    /// CONNECTOR PARSING GOES HERE
    val cache = generateParses(tokens)
    val futureCache: Iterable[Future[(Int, ParseList)]] = cache.map({case (k, v) => {
      Future.value(k).join(v)
    }})

    Future.collect(futureCache.toList).flatMap(cache => {
      val validParseCaches: Iterable[(Int, ParseList)] = cache.filter(_._2.nonEmpty)

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
              val interp = new GeocodeInterpretation(what, where, p(0).feature)
              if (req.full) {
                val sortedParents = p(0).scoringFeatures.parents.flatMap(id => parentMap.get(new ObjectId(id))).sorted
                println("full")
                println(sortedParents)
                interp.setParents(sortedParents.map(parentFeature => {
                  parentFeature.feature
                }))
              }
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
