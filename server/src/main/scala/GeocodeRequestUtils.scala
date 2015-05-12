//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.geo.quadtree.CountryRevGeo
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.GeoTools
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.ostrich.stats.Stats
import com.vividsolutions.jts.geom.Geometry
import scalaj.collection.Implicits._

object GeocodeRequestUtils {
  val maxRadius = 5000 // 5 km

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
    val llToRevGeo: Option[GeocodePoint] = req.llOption.orElse(req.boundsOption.map(_.ne))
    val ccOpt: Option[String] = req.ccOption.orElse(llToRevGeo.flatMap(ll =>
      CountryRevGeo.getNearestCountryCode(ll.lat, ll.lng)))

    CommonGeocodeRequestParams.newBuilder
      .debug(req.debug)
      .woeHint(req.woeHint)
      .woeRestrict(req.woeRestrict)
      .cc(ccOpt)
      .lang(req.langOption)
      .responseIncludes(req.responseIncludesOption)
      .allowedSources(req.allowedSourcesOption)
      .llHint(req.llOption)
      .bounds(req.boundsOption)
      .maxInterpretations(req.maxInterpretationsOption)
      .result
  }

  def makeCircle(ll: GeocodePoint, radius: Double) = {
    Stats.addMetric("incoming_radius", radius.toInt)
    val adjustedRadius: Int  = if (radius > maxRadius) {
      maxRadius
    } else {
      radius.toInt
    }

    Some(GeoTools.makeCircle(ll, adjustedRadius))
  }

  def getRequestGeometry(req: GeocodeRequest): Option[Geometry] = {
    val radius = req.radiusOption.getOrElse(0)
    (req.llOption, req.boundsOption) match {
      case (Some(ll), Some(bounds)) if (radius > 0) =>
        throw new Exception("both bounds and ll+radius, can't pick")
      case (_, Some(bounds)) =>
        Some(GeoTools.boundsToGeometry(bounds))
      case (Some(ll), None) if (radius > 0) =>
        makeCircle(ll, radius)
      case (Some(ll), None) =>
        Some(GeoTools.pointToGeometry(ll))
      case _ => None
    }
  }
}
