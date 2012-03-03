//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.geocoder

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

// TODO
// check if the algorithm works
// DONE? be able to return something
// DONE? implement a feature sorter
// DONE? implement an is valid method
// implement a parse sorted
// construct a complete response
// zipcode hack
// import timezone code

class GeocoderImpl(store: GeocodeStorageService) extends LogHelper {
  type Parse = List[GeocodeRecord]
  type ParseList = List[Parse]

  // 1) straight port of the python logic, I realize this could be more idiomatic (cc: jliszka)
  // 2) I plan to rewrite it
  // 3) I bet that if I start from the other end of the string, on the theory that 
  //    westerners generally write [small -> big], I can prune faster
  // 4) just want to get it working for now
  def generateParses(tokens: List[String], cache: HashMap[Int, ParseList]): ParseList = {
    val cacheKey = tokens.size
    if (tokens.isEmpty) {
      List(Nil)
    } else {
      if (!cache.contains(cacheKey)) {
        val validParses = 
          (for (i <- 1.to(tokens.size)) yield {
            val searchStr = tokens.take(i).mkString(" ")
            logger.info("trying: %d to %d: %s".format(0, i, searchStr))
            val features = store.getByName(searchStr).toList
            logger.info("have %d matches".format(features.size))

            val subParses = generateParses(tokens.drop(i), cache)

            features.flatMap(f => {
              logger.info("looking at %s".format(f))
              subParses.flatMap(p => {
                logger.info("sub_parse: %s".format(p))
                val parse = f :: p
                if (isValidParse(parse)) {
                  logger.info("VALID -- adding to %d".format(cacheKey))
                  println(parse.sorted)
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
    isValidParseHelper(parse)
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

  def geocode(req: GeocodeRequest): GeocodeResponse = {
    val query = req.query
    val tokens = NameNormalizer.tokenize(NameNormalizer.normalize(query))
    /// NEAR PARSING GOES HERE

    val cache = new HashMap[Int, List[List[GeocodeRecord]]]()
    generateParses(tokens, cache)
    println(cache.keys.max)
    val longest = cache.keys.max
    val longestParses = cache(longest)
    val sortedParses = longestParses

    // SORTING PARSES GOES HERE

    val what = tokens.drop(longest).mkString(" ")
    val where = tokens.take(longest).mkString(" ")
    println("%d sorted parses".format(sortedParses.size))
    new GeocodeResponse(sortedParses.map(p => {
      new GeocodeInterpretation(what, where, p(0).toGeocodeFeature)
    }))
  }
}