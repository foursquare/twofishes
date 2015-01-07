package com.foursquare.geo.quadtree

import java.net.URL
import java.io.FileNotFoundException
import com.foursquare.geo.quadtree.ShapefileGeo.ShapeTrieNode

object TimeZoneRevGeo {
  // use empty country if unknown
  val tzFudger = Some(new ShapefileGeo.MultiFudgerTZ())

  def loadResource(resourceName: String): URL = {
    Option(getClass.getClassLoader.getResource(resourceName)).getOrElse{
      throw new FileNotFoundException("Could not find " + resourceName +
        " resource.  Check the classpath/deps?")
    }
  }

  // use empty country if unknown
  lazy val tzNode: ShapeTrieNode = {
    val tzShapefile = loadResource("4sq_tz.shp")
    ShapefileGeo.load(tzShapefile, "TZID", None, "Unknown",
      // ie, if it's in the ocean off the edge of a country, allow it to
      // be part of the country
      alwaysCheckGeometry = false)
  }

  def getNearestTZ(geolat: Double, geolong: Double): Option[String] = {
    tzNode.getNearest(geolat, geolong, tzFudger)
  }
}
