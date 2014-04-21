//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.GeoTools
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import scalaj.collection.Implicits._
import com.twitter.ostrich.stats.Stats

object GeocodeRequestUtils {
  val maxRadius = 5000 // km

  def responseIncludes(req: CommonGeocodeRequestParams, include: ResponseIncludes): Boolean = {
    req.responseIncludes.has(include) ||
      req.responseIncludes.has(ResponseIncludes.EVERYTHING)
  }

  def shouldFetchPolygon(req: CommonGeocodeRequestParams) =
    responseIncludes(req, ResponseIncludes.WKB_GEOMETRY) ||
    responseIncludes(req, ResponseIncludes.WKT_GEOMETRY) ||
    responseIncludes(req, ResponseIncludes.REVGEO_COVERAGE) ||
    responseIncludes(req, ResponseIncludes.WKB_GEOMETRY_SIMPLIFIED) ||
    responseIncludes(req, ResponseIncludes.WKT_GEOMETRY_SIMPLIFIED)

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

  def makeCircle(ll: GeocodePoint, radius: Double) = {
    Stats.incr("revgeo.circleQueries")
    Stats.addMetric("revgeo.radius", radius.toInt)
    val adjustedRadius: Int  = if (radius > maxRadius) {
      Stats.incr("revgeo.radiusTruncated")
      maxRadius
    } else {
      radius.toInt
    }

    Some(GeoTools.makeCircle(ll, adjustedRadius))
  }

  def getRequestGeometry(req: GeocodeRequest): Option[Geometry] = {
    val radius = req.radiusOption.getOrElse(0)
    (req.llOption, req.boundsOption) match {
      case (Some(ll), None) if (radius > 0) => makeCircle(ll, radius)
      case (Some(ll), None) => Some(GeoTools.pointToGeometry(ll))
      case (None, Some(bounds)) => Some(GeoTools.boundsToGeometry(bounds))
      case (None, None) => None
      case (Some(ll), Some(bounds)) => throw new Exception("both bounds and ll, can't pick")
    }
  }
}
