// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{FeatureNamespace, StoredFeatureId}
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import java.io.File
import scalaj.collection.Implicits._

object PolygonLoader {
 def load(store: GeocodeStorageWriteService,
          defaultNamespace: FeatureNamespace,
          writeToRecord: Boolean = false): Unit = {
    val polygonDirs = List(
      new File("data/computed/polygons"),
      new File("data/private/polygons")
    )
    val polygonFiles = polygonDirs.flatMap(polygonDir => {
      if (polygonDir.exists) { polygonDir.listFiles.toList } else { Nil }
    }).sorted

    val wktReader = new WKTReader()
    val wkbWriter = new WKBWriter()
    polygonFiles.flatMap(f => {
      println("processing %s".format(f))
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
        scala.io.Source.fromFile(f).getLines.filterNot(_.startsWith("#")).toList.flatMap(l => {
          val parts = l.split("\t")
          val geom = wktReader.read(parts(1)).buffer(0)
          if (geom.isValid) {
            Some((parts(0) -> geom))
          } else {
            println("geom is not valid for %s: %s".format(parts(0), geom))
            None
          }
        })
      }
    }).map({case (k, geom) => {
      StoredFeatureId.fromHumanReadableString(k, Some(defaultNamespace)).foreach(fid => {
        store.addPolygonToRecord(fid, wkbWriter.write(geom))
      })
    }})
  }
}
