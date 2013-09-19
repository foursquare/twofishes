//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.gen._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.ostrich.stats.Stats
import scalaj.collection.Implicits._

/*
 This class is responsible for taking in a GeocodeRequest, which can either be a geocode, a slug lookup,
 or an autocomplete request, and handing it off to the appropriate logic loop.
 */

class GeocodeRequestDispatcher(
  store: GeocodeStorageReadService) {

  def geocode(req: GeocodeRequest): GeocodeResponse = {
    val logger = new MemoryLogger(req)
    Stats.incr("geocode-requests", 1)
    val finalReq = req.mutable

    finalReq.responseIncludes_=(ResponseIncludes.DISPLAY_NAME :: req.responseIncludes.toList)

    val query = req.queryOption.getOrElse("")
    val parseParams = new QueryParser(logger).parseQuery(query)

    if (req.slugOption.exists(_.nonEmpty)) {
      Stats.time("slug-geocode") {
        new SlugGeocoderImpl(store, finalReq, logger).doSlugGeocode(req.slugOption.getOrElse(""))
      }
    } else if (req.autocomplete) {
      Stats.time("autocomplete-geocode") {
        new AutocompleteGeocoderImpl(store, finalReq, logger).doAutocompleteGeocode(parseParams)
      }
    } else {
      Stats.time("geocode") {
        new GeocoderImpl(store, finalReq, logger).doNormalGeocode(parseParams)
      }
    }
  }
}
