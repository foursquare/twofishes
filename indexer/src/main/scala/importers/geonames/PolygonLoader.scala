// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes.Implicits._
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import java.io.File
import scala.collection.JavaConversions._
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
      val extension = f.getName().split("\\.").lastOption.getOrElse("")
      val shapeFileExtensions = List("shx", "dbf", "prj", "xml")

      if (extension == "shp") {
        for {
          shp <- new ShapefileIterator(f)
          geoid <- shp.propMap.get("geonameid")
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
    }).map({case (k, v) => {
      if (!k.contains(":")) {
        ("%s:%s".format(GeonamesParser.geonameIdNamespace, k), v)
      } else {
        (k, v)
      }
    }}).toMap
  }
}
