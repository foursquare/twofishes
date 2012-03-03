//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.geocoder

import scala.collection.mutable.HashMap

class GeocoderImpl(store: GeocodeStorageService) extends LogHelper {
  val cache = new HashMap[Int, List[List[GeocodeRecord]]]()

  def generateParses(tokens: List[String]): List[List[GeocodeRecord]] = {
    val cacheKey = tokens.size
    if (tokens.isEmpty) {
      Nil
    } else {
      if (!cache.contains(cacheKey)) {
        for (i <- 1.to(tokens.size)) {
          val searchStr = tokens.take(i).mkString(" ")
          logger.info("trying: %d to %d: %s".format(0, i, searchStr))
          val features = store.getByName(searchStr).toList
          logger.info("have %d matches".format(features.size))
          val subParses = generateParses(tokens.drop(i))

          val validParses = features.flatMap(f => {
            logger.info("looking at %s".format(f))
            subParses.flatMap(p => {
              logger.info("sub_parse: %s".format(p))
              val parse = f :: p
              // sort the parse
              // check if it's valid
              // if it is, keep it
              Some(parse)
            })
          })

          cache(cacheKey) = validParses
        }
      }
      cache(cacheKey)
    }
  }

  def geocode(req: GeocodeRequest) = {
    val query = req.query
    val tokens = NameNormalizer.tokenize(NameNormalizer.normalize(query))
    /// NEAR PARSING GOES HERE


  }
}