//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.gen._
import com.foursquare.twofishes.util.Lists.Implicits._
import scalaj.collection.Implicits._

object GeocodeRequestUtils {
  def responseIncludes(req: CommonGeocodeRequestParams, include: ResponseIncludes): Boolean = {
    req.responseIncludes.has(include) ||
      req.responseIncludes.has(ResponseIncludes.EVERYTHING)
  }

  def shouldFetchPolygon(req: CommonGeocodeRequestParams) =
    responseIncludes(req, ResponseIncludes.WKB_GEOMETRY) ||
    responseIncludes(req, ResponseIncludes.WKT_GEOMETRY) ||
    responseIncludes(req, ResponseIncludes.REVGEO_COVERAGE)

  def geocodeRequestToCommonRequestParams(req: GeocodeRequest): CommonGeocodeRequestParams = {
    CommonGeocodeRequestParams.newBuilder
      .debug(req.debug)
      .woeHint(req.woeHint)
      .woeRestrict(req.woeRestrict)
      .cc(req.ccOption)
      .lang(req.langOption)
      .responseIncludes(req.responseIncludesOption)
      .allowedSources(req.allowedSourcesOption)
      .llHint(req.llOption)
      .bounds(req.boundsOption)
      .maxInterpretations(req.maxInterpretationsOption)
      .result
  }

}
