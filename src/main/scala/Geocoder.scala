//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.geocoder

import com.twitter.util.{Future, FuturePool}
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

// TODO
// construct a complete response (woetype, parents? names?)
// oh, hi, um, make me actually event driven. great.
// import timezone server
// break out uses of MongoStorageService
// make server more configurable
// tests

class GeocoderImpl(pool: FuturePool, store: GeocodeStorageService) extends LogHelper {
  type Parse = List[GeocodeRecord]
  type ParseList = List[Parse]

  def generateParses(tokens: List[String]): HashMap[Int, List[List[GeocodeRecord]]] = {
    val cache = new HashMap[Int, List[List[GeocodeRecord]]]()
    generateParsesHelper(tokens, cache)
    cache
  }

  // 1) straight port of the python logic, I realize this could be more idiomatic (cc: jliszka)
  // 2) I plan to rewrite it
  // 3) I bet that if I start from the other end of the string, on the theory that 
  //    westerners generally write [small -> big], I can prune faster
  // 4) just want to get it working for now
  def generateParsesHelper(tokens: List[String], cache: HashMap[Int, ParseList]): ParseList = {
    val cacheKey = tokens.size
    if (tokens.size == 0) {
      List(Nil)
    } else {
      if (!cache.contains(cacheKey)) {
        val validParses = 
          (for (i <- 1.to(tokens.size)) yield {
            val searchStr = tokens.take(i).mkString(" ")
            logger.info("trying: %d to %d: %s".format(0, i, searchStr))
            val features = store.getByName(searchStr).toList
            logger.info("have %d matches".format(features.size))

            val subParses = generateParsesHelper(tokens.drop(i), cache)

            features.flatMap(f => {
              logger.info("looking at %s".format(f))
              subParses.flatMap(p => {
                logger.info("sub_parse: %s".format(p))
                val parse = f :: p
                if (isValidParse(parse)) {
                  logger.info("VALID -- adding to %d".format(cacheKey))
                  Some(parse.sorted)
                } else {
                  logger.info("INVALID")
                  None
                }
              })
            })
          }).flatMap(a=>a).toList
        println("setting %d to %s".format(cacheKey, validParses))
        cache(cacheKey) = validParses
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
    val tokens = NameNormalizer.tokenize(NameNormalizer.normalize(query))
    /// CONNECTOR PARSING GOES HERE

    pool(generateParses(tokens)).flatMap( cache => {
      val parseSizes = cache.keys.filter(k => cache(k).nonEmpty)

      if (parseSizes.size > 0) {
        val longest = parseSizes.max
        val longestParses = cache(longest)

        // TODO: make this configurable
        val sortedParses = longestParses.sorted(new ParseOrdering(req.ll, req.cc)).take(3)

        val parentIds = sortedParses.flatMap(_.headOption.toList.flatMap(_.parents))
        println("parent ids: " + parentIds)
        val parents = store.getByIds(parentIds).toList
        println(parents.toList)
        val parentMap = parentIds.flatMap(pid => {
          parents.find(_.ids.contains(pid)).map(p => (pid -> p))
        }).toMap

        val what = tokens.take(tokens.size - longest).mkString(" ")
        val where = tokens.drop(tokens.size - longest).mkString(" ")
        println("%d sorted parses".format(sortedParses.size))

        pool(
          new GeocodeResponse(sortedParses.map(p => {
            new GeocodeInterpretation(what, where, p(0).toGeocodeFeature(parentMap, Option(req.lang)))
          }))
        )
      } else {
        Future.value(new GeocodeResponse())
      }
    })
  }
}