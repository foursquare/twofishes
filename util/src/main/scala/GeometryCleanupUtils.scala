// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import com.vividsolutions.jts.geom.Geometry
import scala.collection.JavaConverters._

object GeometryCleanupUtils {
  def cleanupGeometryCollection(geom: Geometry): Geometry = {
    val geometryCount = geom.getNumGeometries
    val polygons = for {
      i <- 0 to geometryCount - 1
      geometry = geom.getGeometryN(i)
      if (geometry.getGeometryType == "Polygon" ||
        geometry.getGeometryType == "MultiPolygon")
    } yield geometry
    geom.getFactory.buildGeometry(polygons.asJavaCollection)
  }
}