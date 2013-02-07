// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes.Implicits._
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{Helpers, NameNormalizer, NameUtils, SlugBuilder}
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import java.io.File
import org.opengis.feature.simple.SimpleFeature
import org.bson.types.ObjectId
import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet}
import scalaj.collection.Implicits._

object PolygonLoader {
 def load(): Map[String, Geometry] = {
    val polygonDirs = List(
      new File("data/computed/polygons"),
      new File("data/private/polygons")
    )
    val polygonFiles = polygonDirs.flatMap(polygonDir => {
      if (polygonDir.exists) { polygonDir.listFiles.toList } else { Nil }
    }).sorted

    val wktReader = new WKTReader()
    polygonFiles.flatMap(f => {
      val extension = f.getName().split(".").lift(1).getOrElse("")
      val shapeFileExtensions = List("shx", "dbf", "prj", "xml")

      if (extension == "shp") {
        for {
          shp <- new ShapefileIterator(f)
          geoid <- shp.propMap.get("fs_geoid")
          geom <- shp.geometry
        } yield { (geoid -> geom) }
      } else if (shapeFileExtensions.has(extension)) {
        // do nothing, shapefile aux file
        Nil
      } else {
        scala.io.Source.fromFile(f).getLines.filterNot(_.startsWith("#")).toList.map(l => {
          val parts = l.split("\t")
          (parts(0) -> wktReader.read(parts(1))) 
        })
      }
    }).toMap
  }
}