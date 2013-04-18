//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.Implicits._
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
    val params = new CommonGeocodeRequestParams()
    params.setDebug(req.debug)
    params.setWoeHint(req.woeHint)
    params.setWoeRestrict(req.woeRestrict)
    params.setCc(req.cc)
    params.setLang(req.lang)
    params.setResponseIncludes(req.responseIncludes)
    params.setAllowedSources(req.allowedSources)
    params.setLlHint(req.ll)
    params.setBounds(req.bounds)
    params.setMaxInterpretations(req.maxInterpretations)
    params
  }

}
