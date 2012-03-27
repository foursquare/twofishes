//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofish

import com.twitter.util.{Future, FuturePool}
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import org.bson.types.ObjectId

// TODO
// oh, hi, um, make me actually event driven. great.
// import timezone server
// break out uses of MongoStorageService
// make server more configurable

class GeocoderImpl(pool: FuturePool, store: GeocodeStorageReadService) extends LogHelper {
  type FullParse = Seq[GeocodeRecord]
  type Parse = Seq[Featurelet]
  type ParseSeq = Seq[Parse]

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

  def generateParses(tokens: List[String]): HashMap[Int, ParseSeq] = {
    val cache = new HashMap[Int, ParseSeq]()
    generateParsesHelper(tokens, cache)
    cache
  }

  def generateParsesHelper(tokens: List[String], cache: HashMap[Int, ParseSeq]): ParseSeq = {
    val cacheKey = tokens.size
    if (tokens.size == 0) {
      List(Nil)
    } else {
      if (!cache.contains(cacheKey)) {
        val validParses = 
          (for (i <- 1.to(tokens.size)) yield {
            val searchStr = tokens.take(i).mkString(" ")
            logger.ifTrace("trying: %d to %d: %s".format(0, i, searchStr))
            val features = store.getByName(searchStr)
            //logger.ifTrace("have %d matches".format(features.size))

            val subParses = generateParsesHelper(tokens.drop(i), cache)

            features.flatMap(f => {
              logger.ifTrace("looking at %s".format(f))
              subParses.flatMap(p => {
                logger.ifTrace("sub_parse: %s".format(p))
                val parse = List(f) ++ p
                if (isValidParse(parse)) {
                  logger.ifTrace("VALID -- adding to %d".format(cacheKey))
                  Some(parse.sorted)
                } else {
                  logger.ifTrace("INVALID")
                  None
                }
              })
            })
          }).flatMap(a=>a)
        logger.ifTrace("setting %d to %s".format(cacheKey, validParses))
        cache(cacheKey) = validParses
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
          first.isPostalCode &&
          isValidParseHelper(second :: rest) &&
          GeoTools.getDistance(second.lat, second.lng, first.lat, first.lng) < 200000
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
          f._id == most_specific._id ||
          f.ids.exists(id => most_specific.parents.contains(id))
        })
      }
    }
  }
 
  class ParseOrdering(llHint: GeocodePoint, ccHint: String) extends Ordering[FullParse] {
    // Higher is better
    def scoreParse(parse: FullParse): Int = {
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

    def compare(a: FullParse, b: FullParse) = {
      scoreParse(b) - scoreParse(a)
    }
  }

  def hydrateParses(parses: Seq[Parse]): Seq[FullParse] = {
    val oids: Seq[ObjectId] = parses.flatMap(_.map(_._id))
    val geocodeRecordMap = store.getByObjectIds(oids).map(g => {
      (g._id -> g)
    }).toMap
    parses.map(parse => {
      parse.flatMap(featurelet => {
        geocodeRecordMap.get(featurelet._id)
      })
    })
  }

  def geocode(req: GeocodeRequest): Future[GeocodeResponse] = {
    val query = req.query

    logger.ifTrace("%s --> %s".format(query, NameNormalizer.normalize(query)))

    val tokens = NameNormalizer.tokenize(NameNormalizer.normalize(query))
    logger.ifTrace("--> %s".format(tokens.mkString("_|_")))

    /// CONNECTOR PARSING GOES HERE

    pool(generateParses(tokens)).flatMap( cache => {
      val parseSizes = cache.keys.filter(k => cache(k).nonEmpty)

      if (parseSizes.size > 0) {
        val longest = parseSizes.max
        val longestParses = cache(longest)

        val hydratedParses = hydrateParses(longestParses)

        // TODO: make this configurable
        val sortedParses = hydratedParses.sorted(new ParseOrdering(req.ll, req.cc)).take(3)

        val parentIds = sortedParses.flatMap(_.headOption.toList.flatMap(_.parents))
        logger.ifTrace("parent ids: " + parentIds)
        val parents = store.getByIds(parentIds).toList
        logger.ifTrace(parents.toList.toString)
        val parentMap = parentIds.flatMap(pid => {
          parents.find(_.ids.contains(pid)).map(p => (pid -> p))
        }).toMap

        val sortedParents = parentIds.flatMap(id => parentMap.get(id)).sorted

        val what = tokens.take(tokens.size - longest).mkString(" ")
        val where = tokens.drop(tokens.size - longest).mkString(" ")
        logger.ifTrace("%d sorted parses".format(sortedParses.size))

        pool(
          new GeocodeResponse(sortedParses.map(p => {
            val interp = new GeocodeInterpretation(what, where, p(0).toGeocodeFeature(parentMap, req.full, Option(req.lang)))
            if (req.full) {
              interp.setParents(sortedParents.map(parentFeature => {
                parentFeature.toGeocodeFeature(parentMap, req.full, Option(req.lang))
              }))
            }
            interp
          }))
        )
      } else {
        Future.value(new GeocodeResponse(Nil))
      }
    })
  }
}
