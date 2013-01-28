// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.twofishes.util

import scala.collection.JavaConversions._
import com.google.common.geometry.{S2CellId, S2LatLng, S2Polygon, S2PolygonBuilder, S2RegionCoverer, S2LatLngRect}
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import scala.collection.JavaConversions._

object GeometryUtils {
  val minS2Level = 9
  val maxS2Level = 18

   def getBytes(l: S2CellId): Array[Byte] = getBytes(l.id())

   def getBytes(l: Long): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    dos.writeLong(l)
    baos.toByteArray()
  }

   def getBytes(l: Int): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    dos.writeInt(l)
    baos.toByteArray()
  }

  def getS2CellIdForLevel(lat: Double, long: Double, s2Level: Int): S2CellId = {
    val ll = S2LatLng.fromDegrees(lat, long)
    S2CellId.fromLatLng(ll).parent(s2Level)
  }

  /**
   * Returns an `Iterable` of cells that cover a rectangle.
   */
  def rectCover(topRight: (Double,Double), bottomLeft: (Double,Double),
                minLevel: Int,
                maxLevel: Int,
                levelMod: Option[Int]):
      Seq[com.google.common.geometry.S2CellId] = {
    val topRightPoint = S2LatLng.fromDegrees(topRight._1, topRight._2)
    val bottomLeftPoint = S2LatLng.fromDegrees(bottomLeft._1, bottomLeft._2)

    val rect = S2LatLngRect.fromPointPair(topRightPoint, bottomLeftPoint)
    rectCover(rect, minLevel, maxLevel, levelMod)
  }

  def rectCover(rect: S2LatLngRect,
                minLevel: Int,
                maxLevel: Int,
                levelMod: Option[Int]):
      Seq[com.google.common.geometry.S2CellId] = {
    val coverer =  new S2RegionCoverer
    coverer.setMinLevel(minLevel)
    coverer.setMaxLevel(maxLevel)
    levelMod.foreach(m => coverer.setLevelMod(m))

    val coveringCells = new java.util.ArrayList[com.google.common.geometry.S2CellId]

    coverer.getCovering(rect, coveringCells)

    coveringCells
  }

  def s2BoundingBoxCovering(geomCollection: Geometry,
      minS2Level: Int, maxS2Level: Int) = {
    val envelope = geomCollection.getEnvelopeInternal()
    rectCover(
      topRight = (envelope.getMaxY(), envelope.getMaxX()),
      bottomLeft = (envelope.getMinY(), envelope.getMinX()),
      minLevel = minS2Level,
      maxLevel = maxS2Level,
      levelMod = None
    )
  }

  def s2Polygon(geomCollection: Geometry) = {
    val polygons: List[S2Polygon] = (for {
     i <- 0.until(geomCollection.getNumGeometries()).toList
     val geom = geomCollection.getGeometryN(i)
     if (geom.isInstanceOf[Polygon])
    } yield {
      val poly = geom.asInstanceOf[Polygon]
      val ring = poly.getExteriorRing()
      val coords = ring.getCoordinates()
      val builder = new S2PolygonBuilder()
      (coords ++ List(coords(0))).sliding(2).foreach(pair => {
        val p1 = pair(0)
        val p2 = pair(1)
        builder.addEdge(
          S2LatLng.fromDegrees(p1.y, p1.x).toPoint,
          S2LatLng.fromDegrees(p2.y, p2.x).toPoint
        )
      })
      builder.assemblePolygon()
    })
    val builder = new S2PolygonBuilder()
    polygons.foreach(p => builder.addPolygon(p))
    builder.assemblePolygon()
  }

  def s2PolygonCovering(geomCollection: Geometry, 
      minS2Level: Int,
      maxS2Level: Int,
      levelMod: Option[Int] = None) = {
    val s2poly = s2Polygon(geomCollection)
    val coverer =  new S2RegionCoverer
    coverer.setMinLevel(minS2Level)
    coverer.setMaxLevel(maxS2Level)   
    levelMod.foreach(m => coverer.setLevelMod(m))
    val coveringCells = new java.util.ArrayList[com.google.common.geometry.S2CellId]
    coverer.getCovering(s2poly, coveringCells)
    coveringCells
  }
}