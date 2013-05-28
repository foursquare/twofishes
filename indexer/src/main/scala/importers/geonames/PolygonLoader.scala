// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{FeatureNamespace, StoredFeatureId}
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import org.geotools.geojson.feature.FeatureJSON
import java.io.File
import scalaj.collection.Implicits._

object PolygonLoader {
  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    if (these != null) {
      (these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)).filterNot(_.isDirectory)
    } else {
      Array()
    }
  }

  def load(store: GeocodeStorageWriteService,
           defaultNamespace: FeatureNamespace): Unit = {
    val polygonDirs = List(
      new File("data/computed/polygons"),
      new File("data/private/polygons")
    )
    val polygonFiles = polygonDirs.flatMap(recursiveListFiles)

    val wktReader = new WKTReader()
    val wkbWriter = new WKBWriter()

    def updateRecord(k: String, geom: Geometry) = {
      StoredFeatureId.fromHumanReadableString(k, Some(defaultNamespace)).foreach(fid => {
        try {
          store.addPolygonToRecord(fid, wkbWriter.write(geom))
        } catch {
          case e => {
            throw new Exception("couldn't write poly to %s".format(fid), e)
          }
        }
      })
    }

    polygonFiles.foreach(f => {
      println("processing %s".format(f))
      val fparts = f.getName().split("\\.")
      val extension = fparts.lift(1).getOrElse("")
      val shapeFileExtensions = List("shx", "dbf", "prj", "xml", "cpg")

      if (extension == "json") {
        for {
          geoid <- fparts.lift(0)
          if !geoid.contains("-")
        } {
          val io = new FeatureJSON()
          val features = io.streamFeatureCollection(f)
          if (features.hasNext()) {
            val feature = features.next()
            val geom = feature.getDefaultGeometry().asInstanceOf[Geometry]
            updateRecord(geoid, geom)
          }
        }
      } else if (extension == "shp") {
        for {
          shp <- new ShapefileIterator(f)
          geoids <- shp.propMap.get("geonameid") orElse shp.propMap.get("qs_gn_id")
          geom <- shp.geometry
          geoid <- geoids.split(',')
          if !geoid.isEmpty
        } {
          try {
            updateRecord(geoid, geom)
          } catch {
            case e: Exception =>
              throw new RuntimeException("error with geoids %s".format(geoids), e)
          }
        }
      } else if (shapeFileExtensions.has(extension)) {
        // do nothing, shapefile aux file
        Nil
      } else {
        scala.io.Source.fromFile(f).getLines.filterNot(_.startsWith("#")).toList.foreach(l => {
          val parts = l.split("\t")
          val geom = wktReader.read(parts(1)).buffer(0)
          if (geom.isValid) {
            updateRecord(parts(0), geom)
          } else {
            println("geom is not valid for %s".format(parts(0)))
          }
        })
      }
    })
  }
}
