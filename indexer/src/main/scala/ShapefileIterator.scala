// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.geo.shapes

import com.vividsolutions.jts.geom.Geometry
import java.io.File
import java.nio.charset.Charset
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.geojson.GeoJSONUtil
import org.geotools.geojson.feature.FeatureJSON
import org.opengis.feature.simple.SimpleFeature
import scalaj.collection.Imports._

object FsqSimpleFeatureImplicits {
  implicit def simpleFeatureToFsqSimpleFeature(f: SimpleFeature) =
    new FsqSimpleFeature(f)
}

class FsqSimpleFeature(val f: SimpleFeature) {
  val propMap = f.getProperties().asScala.flatMap(p => {
    Option(p.getValue()).map(v =>
      (p.getName().toString, v.toString)
    )
  }).toMap

  lazy val geometry: Option[Geometry] = {
    if (f.getDefaultGeometry() != null) {
      Some(f.getDefaultGeometry().asInstanceOf[Geometry] )
    } else {
      None
    }
  }

  lazy val boundingBox = {
    geometry.map(g => {
      val envelope = g.getEnvelopeInternal()
      (
        (envelope.getMaxY(), envelope.getMaxX()),
        (envelope.getMinY(), envelope.getMinX())
      )
    })
  }
}

trait ShapeIterator extends Iterator[SimpleFeature] {
  def file: File
}

class GeoJsonIterator(val file: File) extends ShapeIterator {
  def this(path: String) = this(new File(path))
  val io = new FeatureJSON()
   //urn:ogc:def:crs:OGC:1.3:CRS84
   // epsg:4326
  val source = scala.io.Source.fromFile(file)
  val data = source.mkString
  source.close()
  val iter = io.streamFeatureCollection(GeoJSONUtil.toReader(data.replace("urn:ogc:def:crs:OGC:1.3:CRS84", "epsg:4326")))


  def hasNext = iter.hasNext
  def next = iter.next
}

class ShapefileIterator(val file: File) extends ShapeIterator {
  def this(path: String) = this(new File(path))
  val shapeURL = file.toURI.toURL
  val store = new ShapefileDataStore(shapeURL)
  store.setStringCharset(Charset.forName("UTF-8"))
  val name = store.getTypeNames()(0);
  val source = store.getFeatureSource(name)
  val fsShape = source.getFeatures()
  val iter = fsShape.features()

  override def size = fsShape.size()

  def hasNext = iter.hasNext
  def next = iter.next
}
