// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{FeatureNamespace, StoredFeatureId}
import com.foursquare.twofishes.util.Helpers
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.mongodb.casbah.Imports._
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, WKTReader}
import org.geotools.geojson.feature.FeatureJSON
import com.weiglewilczek.slf4s.Logging
import java.io.File
import scalaj.collection.Implicits._


object PolygonLoader extends Logging {
  val wktReader = new WKTReader()
  val wkbWriter = new WKBWriter()

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    if (these != null) {
      (these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)).filterNot(_.isDirectory)
    } else {
      Array()
    }
  }

  var recordsUpdated = 0

  def updateRecord(
    store: GeocodeStorageWriteService,
    defaultNamespace: FeatureNamespace,
    k: String,
    geom: Geometry
  ) = {
    StoredFeatureId.fromHumanReadableString(k, Some(defaultNamespace)).foreach(fid => {
      try {
        logger.debug("adding poly to %s".format(fid))
        recordsUpdated += 1
        store.addPolygonToRecord(fid, wkbWriter.write(geom))
      } catch {
        case e => {
          throw new Exception("couldn't write poly to %s".format(fid), e)
        }
      }
    })
  }

  def load(store: GeocodeStorageWriteService,
           defaultNamespace: FeatureNamespace): Unit = {
    val polygonDirs = List(
      new File("data/computed/polygons"),
      new File("data/private/polygons")
    )
    val polygonFiles = polygonDirs.flatMap(recursiveListFiles).sorted

    for {
      (f, index) <- polygonFiles.zipWithIndex
     } {
      if (index % 1000 == 0) {
        System.gc();
      }
      load(store, defaultNamespace, f)
    }
    logger.info("post processing, looking for bad poly matches")
    val polygons =
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
        .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc

    val wkbReader = new WKBReader()

    for {
      (featureRecord, index) <- polygons.zipWithIndex
      polyData <- featureRecord.polygon
    } {
      val polygon = wkbReader.read(polyData)
      val point = featureRecord.center
      if (!polygon.contains(point) && polygon.distance(point) > 0.03) {
        logger.info("bad poly on %s -- %s too far from %s".format(
          featureRecord.featureId, polygon, point))

          MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(featureRecord.featureId.longId))),
            MongoDBObject("$set" ->
              MongoDBObject(
                "polygon" -> None,
                "hasPoly" -> false
              )
            ),
            false, false)
        }
      }
  }

   def load(store: GeocodeStorageWriteService,
            defaultNamespace: FeatureNamespace,
            f: File
   ): Unit = {
    val previousRecordsUpdated = recordsUpdated
    logger.info("processing %s".format(f))
    val fparts = f.getName().split("\\.")
    val extension = fparts.lastOption.getOrElse("")
    val shapeFileExtensions = List("shx", "dbf", "prj", "xml", "cpg")

    if (extension == "json") {
      val io = new FeatureJSON()
      val features = io.streamFeatureCollection(f)
      while (features.hasNext()) {
        val feature = features.next()
        val geom = feature.getDefaultGeometry().asInstanceOf[Geometry]
        val geoid: List[String] = fparts.lift(0).flatMap(p => Helpers.TryO(p.toInt.toString)).orElse(
          feature.propMap.get("geonameid") orElse feature.propMap.get("qs_gn_id") orElse feature.propMap.get("gn_id") orElse {
            logger.error("no id on %s".format(f))
            None
          }
        ).toList.filterNot(_.isEmpty).flatMap(_.split(","))
        geoid.foreach(id => {
          updateRecord(store, defaultNamespace, id, geom)
        })
      }
      features.close()
    } else if (extension == "shp") {
      for {
        shp <- new ShapefileIterator(f)
        geoids <- shp.propMap.get("geonameid") orElse shp.propMap.get("qs_gn_id") orElse shp.propMap.get("gn_id") orElse {
          logger.error("no id on %s".format(f))
          None
        }
        geom <- shp.geometry
        geoid <- geoids.split(',')
        if !geoid.isEmpty
      } {
        try {
          updateRecord(store, defaultNamespace, geoid.replace(".0", ""), geom)
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
          updateRecord(store, defaultNamespace, parts(0), geom)
        } else {
          logger.error("geom is not valid for %s".format(parts(0)))
        }
      })
    }
    logger.info("%d records updated by %s".format(recordsUpdated - previousRecordsUpdated, f))
  }
}
