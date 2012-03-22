//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofish

import com.twitter.util.{Future, FuturePool}
import scala.collection.JavaConversions._
import java.util.concurrent.ConcurrentHashMap

// TODO
// oh, hi, um, make me actually event driven. great.
// import timezone server
// break out uses of MongoStorageService
// make server more configurable

class GeocoderImpl(store: GeocodeStorageFutureReadService) extends LogHelper {
  type Parse = List[GeocodeRecord]
  type ParseList = List[Parse]

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

  def generateParses(tokens: List[String]): ConcurrentHashMap[Int, Future[ParseList]] = {
    val cache = new ConcurrentHashMap[Int, Future[ParseList]]()
    generateParsesHelper(tokens, cache)
    cache
  }

  def generateParsesHelper(tokens: List[String], cache: ConcurrentHashMap[Int, Future[ParseList]]): Future[ParseList] = {
    val cacheKey = tokens.size
    if (tokens.size == 0) {
      Future.value(List(Nil))
    } else {
      if (!cache.contains(cacheKey)) {
        val searchStrs: List[String] =
          1.to(tokens.size).map(i => tokens.take(i).mkString(" ")).toList

        val featuresFs: List[Future[Seq[GeocodeRecord]]] =
          for ((searchStr, i) <- searchStrs.zipWithIndex) yield {
            logger.trace("trying: %d to %d: %s".format(0, i, searchStr))
            val result: Future[Seq[GeocodeRecord]] = store.getByName(searchStr)
            result
          }

         // Compute subparses in parallel (scatter)
         val subParsesFs: List[Future[ParseList]] =
           1.to(tokens.size).map(i => generateParsesHelper(tokens.drop(i), cache)).toList

         // Collect the results of all the features and subparses (gather)
         val featureListsF: Future[Seq[Seq[GeocodeRecord]]] = Future.collect(featuresFs)
         val subparseListsF: Future[Seq[ParseList]] = Future.collect(subParsesFs)

         val validParsesF: Future[ParseList] =
          for((featureLists, subparseLists) <- featureListsF.join(subparseListsF)) yield {
            val validParses: Seq[List[GeocodeRecord]] = (
              featureLists.zip(subparseLists).flatMap({case(features: Seq[GeocodeRecord], subparses: Seq[Parse]) => {
                (for {
                  f <- features.toList
                  val _ = logger.trace("looking at %s".format(f))
                  p <- subparses
                  val _ = logger.trace("sub_parse: %s".format(p))
                } yield {
                  val parse: List[GeocodeRecord] = f :: p
                  if (isValidParse(parse)) {
                    logger.trace("VALID -- adding to %d".format(cacheKey))
                    Some(parse.sorted)
                  } else {
                    logger.trace("INVALID")
                    None
                  }
                }).flatten
              }})
            )
            validParses.toList
          }


        validParsesF.onSuccess(validParses => {
          logger.trace("setting %d to %s".format(cacheKey, validParses))
        })
        cache(cacheKey) = validParsesF
      }
      cache(cacheKey)
    }
  }

  def isValidParse(parse: List[GeocodeRecord]): Boolean = {
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
          first.isPostalCode &&
          isValidParseHelper(second :: rest) &&
          GeoTools.getDistance(second.lat, second.lng, first.lat, first.lng) < 200000
        }
        case  _ => false
      }
    }
  }

  def isValidParseHelper(parse: List[GeocodeRecord]): Boolean = {
    parse match {
      case Nil => true
      case f :: Nil => true
      case most_specific :: rest => {
        rest.forall(f => {
          f._id == most_specific._id ||
          f.ids.exists(id => most_specific.parents.contains(id))
        })
      }
    }
  }
 
  class ParseOrdering(llHint: GeocodePoint, ccHint: String) extends Ordering[Parse] {
    // Higher is better
    def scoreParse(parse: Parse): Int = {
      parse match {
        case primaryFeature :: rest => {
          var signal = primaryFeature.population.getOrElse(0)

          // if we have a repeated feature, downweight this like crazy
          // so st petersburg, st petersburg works, but doesn't break new york, ny
          if (rest.contains(primaryFeature)) {
            signal -= 100000000
          }

          // prefer a more aggressive parse ... bleh
          // this prefers "mt laurel" over the town of "laurel" in "mt" (montana)
          signal -= 20000 * parse.length

          // Matching country hint is good
          if (Option(ccHint).exists(_ == primaryFeature.cc)) {
            signal += 100000
          }

          Option(llHint).foreach(ll => {
            signal -= GeoTools.getDistance(ll.lat, ll.lng,
                primaryFeature.lat, primaryFeature.lng)
          })

          signal += primaryFeature.boost.getOrElse(0)

          // as a terrible tie break, things in the US > elsewhere
          // meant primarily for zipcodes
          if (primaryFeature.cc == "US") {
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

        // TODO: make this configurable
        val sortedParses = longestParses.sorted(new ParseOrdering(req.ll, req.cc)).take(3)

        val parentIds = sortedParses.flatMap(_.headOption.toList.flatMap(_.parents))
        logger.trace("parent ids: " + parentIds)
        store.getByIds(parentIds).map(parents => {
          logger.trace(parents.toString)
          val parentMap = parentIds.flatMap(pid => {
            parents.find(_.ids.contains(pid)).map(p => (pid -> p))
          }).toMap

          val what = tokens.take(tokens.size - longest).mkString(" ")
          val where = tokens.drop(tokens.size - longest).mkString(" ")
          logger.trace("%d sorted parses".format(sortedParses.size))

          new GeocodeResponse(sortedParses.map(p => {
            val interp = new GeocodeInterpretation(what, where, p(0).toGeocodeFeature(parentMap, req.full, Option(req.lang)))
            if (req.full) {
              interp.setParents(p.drop(1).map(parentFeature => {
                parentFeature.toGeocodeFeature(parentMap, req.full, Option(req.lang))
              }))
            }
            interp
          }))
        })
      } else {
        Future.value(new GeocodeResponse(Nil))
      }
    })
  }
}
