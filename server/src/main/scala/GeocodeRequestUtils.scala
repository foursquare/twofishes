//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import scala.collection.JavaConversions._
import scalaj.collection.Implicits._

object GeocodeRequestUtils {
  def responseIncludes(req: CommonGeocodeRequestParams, include: ResponseIncludes): Boolean = {
    req.responseIncludes.asScala.has(include) ||
      req.responseIncludes.asScala.has(ResponseIncludes.EVERYTHING)
  }

  def shouldFetchPolygon(req: CommonGeocodeRequestParams) = 
    responseIncludes(req, ResponseIncludes.WKB_GEOMETRY) ||
    responseIncludes(req, ResponseIncludes.WKT_GEOMETRY) ||
    responseIncludes(req, ResponseIncludes.REVGEO_COVERAGE)

  def geocodeRequestToCommonRequestParams(req: GeocodeRequest): CommonGeocodeRequestParams = {
    new CommonGeocodeRequestParams()
      .setDebug(req.debug)
      .setWoeHint(req.woeHint)
      .setWoeRestrict(req.woeRestrict)
      .setCc(req.cc)
      .setLang(req.lang)
      .setResponseIncludes(req.responseIncludes)
      .setAllowedSources(req.allowedSources)
      .setLlHint(req.ll)
      .setBounds(req.bounds)
      .setMaxInterpretations(req.maxInterpretations)
  }

}