package com.foursquare.geo.quadtree

import java.net.URL
import java.io.FileNotFoundException
import com.foursquare.geo.quadtree.ShapefileGeo.ShapeTrieNode

object CountryRevGeo {
  // use empty country if unknown
  val fudgerCC = Some(new ShapefileGeo.MultiFudgerCC("XX"))

  def loadResource(resourceName: String): URL = {
    Option(getClass.getClassLoader.getResource(resourceName)).getOrElse{
      throw new FileNotFoundException("Could not find " + resourceName +
        " resource.  Check the classpath/deps?")
    }
  }

  // use empty country if unknown
  lazy val ccNode: ShapeTrieNode = {
    val validCCs = new scala.collection.mutable.HashSet[String]
    java.util.Locale.getISOCountries.foreach(validCCs += _)
    val ccShapefile = loadResource("4sq_cc-1.1.shp")
    ShapefileGeo.load(ccShapefile, "ISO2", Some(validCCs.toSet), "XX",
      // ie, if it's in the ocean off the edge of a country, allow it to
      // be part of the country
      alwaysCheckGeometry = false)
  }

  def getNearestCountryCode(geolat: Double, geolong: Double): Option[String] = {
    ccNode.getNearest(geolat, geolong, fudgerCC)
  }
}