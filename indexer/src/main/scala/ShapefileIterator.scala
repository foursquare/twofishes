// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.geo.shapes

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry}
import java.io.{File, Serializable}
import java.nio.charset.Charset
import org.geotools.data.{DataStoreFactorySpi, FileDataStore}
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.opengis.feature.simple.SimpleFeature
import org.geotools.data.simple.{SimpleFeatureIterator, SimpleFeatureSource}
import org.geotools.geometry.jts.JTSFactoryFinder
import scalaj.collection.Imports._

object FsqSimpleFeatureImplicits {
  implicit def simpleFeatureToFsqSimpleFeature(f: SimpleFeature) =
    new FsqSimpleFeature(f)
}

class FsqSimpleFeature(val f: SimpleFeature) {
  val propMap = f.getProperties().asScala.map(p => {
    (p.getName().toString, p.getValue().toString)
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

class ShapefileIterator(file: File) extends Iterator[SimpleFeature] {
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