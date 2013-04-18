//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.ostrich.stats.Stats
import scalaj.collection.Implicits._

/*
 This class is responsible for taking in a GeocodeRequest, which can either be a geocode, a slug lookup,
 or an autocomplete request, and handing it off to the appropriate logic loop.
 */

class GeocodeRequestDispatcher(
  store: GeocodeStorageReadService,
  req: GeocodeRequest) {
  val logger = new MemoryLogger(req)

  def geocode(): GeocodeResponse = {
    Stats.incr("geocode-requests", 1)

    if (req.responseIncludes == null || req.responseIncludes.isEmpty) {
      req.setResponseIncludes(List(ResponseIncludes.DISPLAY_NAME).asJava)
    } else {
      req.setResponseIncludes(
        (ResponseIncludes.DISPLAY_NAME :: req.responseIncludes.asScala.toList).asJava
      )
    }

    val query = Option(req.query).getOrElse("")
    val parseParams = new QueryParser(logger).parseQuery(query)

    if (Option(req.slug).exists(_.nonEmpty)) {
      Stats.time("slug-geocode") {
        new SlugGeocoderImpl(store, req, logger).doSlugGeocode(req.slug)
      }
    } else if (req.autocomplete) {
      Stats.time("autocomplete-geocode") {
        new AutocompleteGeocoderImpl(store, req, logger).doAutocompleteGeocode(parseParams)
      }
    } else {
      Stats.time("geocode") {
        new GeocoderImpl(store, req, logger).doNormalGeocode(parseParams)
      }
    }
  }
}
