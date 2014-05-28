package com.foursquare.twofishes.util

import com.foursquare.twofishes._
import com.google.common.geometry.{S2LatLng, S2LatLngRect}
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import com.vividsolutions.jts.operation.distance.DistanceOp
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.datum.DefaultEllipsoid

object GeoTools {
  val MetersPerMile: Double = 1609.344
  val RadiusInMeters: Int = 6378100 // Approximately a little less than the Earth's polar radius
  val MetersPerDegreeLatitude: Double = 111111.0
  val MetersPerDegreeLongitude: Double = 110540.0 // I'm assuming this as at the Equator

  def boundingBoxToS2Rect(bounds: GeocodeBoundingBox): S2LatLngRect = {
    S2LatLngRect.fromPointPair(
      S2LatLng.fromDegrees(bounds.ne.lat, bounds.ne.lng),
      S2LatLng.fromDegrees(bounds.sw.lat, bounds.sw.lng)
    )
  }

  def pointToS2LatLng(ll: GeocodePoint): S2LatLng = {
    S2LatLng.fromDegrees(ll.lat, ll.lng)
  }

  def S2LatLngToPoint(ll: S2LatLng): GeocodePoint = {
    GeocodePoint(ll.latDegrees, ll.lngDegrees)
  }

  def pointToGeometry(p: GeocodePoint): Geometry = {
    pointToGeometry(p.lat, p.lng)
  }

  def pointToGeometry(lat: Double, lng: Double): Geometry = {
    val geometryFactory = new GeometryFactory()
    val coord = new Coordinate(lng, lat)
    geometryFactory.createPoint(coord);
  }

  def boundsContains(bounds: GeocodeBoundingBox, ll: GeocodePoint): Boolean = {
    val rect =  boundingBoxToS2Rect(bounds)
    val point = pointToS2LatLng(ll)
    rect.contains(point)
  }

  def boundsIntersect(b1: GeocodeBoundingBox, b2: GeocodeBoundingBox): Boolean = {
    boundingBoxToS2Rect(b1).intersects(boundingBoxToS2Rect(b2))
  }

  def makeCircle(ll: GeocodePoint, radiusInMeters: Int): Geometry = {
    val calc = new GeodeticCalculator(DefaultEllipsoid.WGS84)
    calc.setStartingGeographicPoint(ll.lng, ll.lat)

    // magic? I did not write this.
    val SIDES = Math.min(100, 32 + 16 * (Math.ceil(radiusInMeters / 40).toInt / 5))

    val baseAzimuth = 360.0 / SIDES
    val coords = 0.until(SIDES).map(side => {
      val azimuth = 180 - (side * baseAzimuth)
      calc.setDirection(azimuth, radiusInMeters)
      val point = calc.getDestinationGeographicPoint()
      new Coordinate(point.getX(), point.getY())
    })

    // make it close
    val finalCoords = coords ++ coords.take(1)

    val geomFactory = new GeometryFactory()
    val ring = geomFactory.createLinearRing(finalCoords.toArray)
    geomFactory.createPolygon(ring, null)
  }

  def boundsToGeometry(bounds: GeocodeBoundingBox): Geometry = {
    val s2rect = GeoTools.boundingBoxToS2Rect(bounds)
    val geomFactory = new GeometryFactory()
    val lngLo = math.toDegrees(s2rect.lng.lo)
    val latLo = math.toDegrees(s2rect.lat.lo)
    val lngHi = math.toDegrees(s2rect.lng.hi)
    val latHi = math.toDegrees(s2rect.lat.hi)
    geomFactory.createLinearRing(Array(
      new Coordinate(lngLo, latLo),
      new Coordinate(lngHi, latLo),
      new Coordinate(lngHi, latHi),
      new Coordinate(lngLo, latHi),
      new Coordinate(lngLo, latLo)
    ))
  }

  def distanceFromPointToBounds(p: GeocodePoint, bounds: GeocodeBoundingBox): Double = {
    val point = pointToGeometry(p)
    val geom = boundsToGeometry(bounds)
    distanceFromPointToGeometry(point, geom)
  }

  def distanceFromPointToGeometry(point: Geometry, geom: Geometry): Double = {
    val nearestPoints = DistanceOp.nearestPoints(point, geom)
    getDistance(nearestPoints(0), nearestPoints(1))
  }
 
  /**
   * @return distance in meters
   */
  def getDistance(ll1: Coordinate, ll2: Coordinate): Double = {
    getDistance(ll1.y, ll1.x, ll2.y, ll2.x)
  }

  def getDistance(geolat1: Double, geolong1: Double, geolat2: Double, geolong2: Double): Int = {
    val theta = geolong1 - geolong2
    val dist = math.sin(math.toRadians(geolat1)) * math.sin(math.toRadians(geolat2)) +
               math.cos(math.toRadians(geolat1)) * math.cos(math.toRadians(geolat2)) * math.cos(math.toRadians(theta))
    (RadiusInMeters * math.acos(dist)).toInt
  }
}
