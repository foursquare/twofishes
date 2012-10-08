// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import java.text.Normalizer
import java.util.regex.Pattern

object Helpers {
  def tryo[T](f: => T): Option[T] = {
    try {
      Some(f)
    } catch {
      case _ => None
    }
  }

  def flattryo[T](f: => Option[T]): Option[T] = {
    try {
      f
    } catch {
      case _ => None
    }
  }
}

object NameNormalizer {
  def tokenize(s: String): List[String] = {
    s.split(" ").filterNot(_.isEmpty).toList
  }

  def normalize(s: String): String = {
    var n = s.toLowerCase
    // remove periods and quotes
    n = n.replaceAll("['\u2018\u2019\\.]", "")
    // change all other punctuation to spaces
    n = n.replaceAll("\\p{Punct}", " ")
    n
  }

  def deaccent(s: String): String = {
    val temp = Normalizer.normalize(s, Normalizer.Form.NFD);
    val pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    pattern.matcher(temp).replaceAll("");
  }
}

object GeoTools {
  val MetersPerMile: Double = 1609.344
  val RadiusInMeters: Int = 6378100 // Approximately a little less than the Earth's polar radius
  val MetersPerDegreeLatitude: Double = 111111.0
  val MetersPerDegreeLongitude: Double = 110540.0 // I'm assuming this as at the Equator

  def boundsContains(bounds: GeocodeBoundingBox, ll: GeocodePoint): Boolean = {
    val minLat = List(bounds.ne.lat, bounds.sw.lat).min
    val maxLat = List(bounds.ne.lat, bounds.sw.lat).max
    val minLng = List(bounds.ne.lng, bounds.sw.lng).min
    val maxLng = List(bounds.ne.lng, bounds.sw.lng).max

    ll.lat <= maxLat && ll.lat >= minLat &&
      ll.lng <= maxLng && ll.lng >= minLng
  }

  /**
   * @return distance in meters
   */
  def getDistance(geolat1: Double, geolong1: Double, geolat2: Double, geolong2: Double): Int = {
    val theta = geolong1 - geolong2
    val dist = math.sin(math.toRadians(geolat1)) * math.sin(math.toRadians(geolat2)) +
               math.cos(math.toRadians(geolat1)) * math.cos(math.toRadians(geolat2)) * math.cos(math.toRadians(theta))
    (RadiusInMeters * math.acos(dist)).toInt
  }
}
