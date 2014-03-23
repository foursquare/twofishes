// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import org.opengis.feature.simple.SimpleFeature
import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.{GeoJsonIterator, ShapefileIterator, ShapeIterator}
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{FeatureNamespace, StoredFeatureId, AdHocId}
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
import com.codahale.jerkson.Json

object PolygonLoader {
  var adHocIdCounter = 1
}

class PolygonLoader(
  parser: GeonamesParser,
  store: GeocodeStorageWriteService
) extends Logging {
  val wktReader = new WKTReader()
  val wkbWriter = new WKBWriter()

  case class PolygonMappingConfig (
    nameFields: List[String],
    woeTypes: List[Any]
  ) {
    private val _woeTypes: List[YahooWoeType] = {
      woeTypes.map(_ match {
        case i: Int => Option(YahooWoeType.findByIdOrNull(i)).getOrElse(
          throw new Exception("Unknown woetype: %s".format(i)))
        case s: String => Option(YahooWoeType.findByNameOrNull(s)).getOrElse(
          throw new Exception("Unknown woetype: %s".format(s)))
        case t => throw new Exception("unknown woetype: %s".format(t))
      })
    }

    def getWoeTypes() = _woeTypes
  }

  def getMappingForFile(f: File): Option[PolygonMappingConfig] = {
    val parts = f.getPath().split("\\.")
    val file = new File(parts(0) + ".mapping.json")
    if (file.exists()) {
      Some(Json.parse[PolygonMappingConfig](new java.io.FileInputStream(file)))
    } else {
      None
    }
  }

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
        case e: Exception => {
          throw new Exception("couldn't write poly to %s".format(fid), e)
        }
      }
    })
  }

  def load(defaultNamespace: FeatureNamespace): Unit = {
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
      load(defaultNamespace, f)
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

  def maybeMatchFeature(
    feature: SimpleFeature,
    polygonMappingConfig: Option[PolygonMappingConfig]
  ): Option[String] = {
    polygonMappingConfig.flatMap(config => {
      None
    })
  }

  def maybeMakeFeature(feature: SimpleFeature): Option[String] = {
    (for {
      adminCode1 <- feature.propMap.get("adminCode1")
      countryCode <- feature.propMap.get("countryCode")
      name <- feature.propMap.get("name")
      lat <- feature.propMap.get("lat")
      lng <- feature.propMap.get("lng")
    } yield {
      val id = AdHocId(PolygonLoader.adHocIdCounter)

      val attribMap: Map[GeonamesFeatureColumns.Value, String] = Map(
        (GeonamesFeatureColumns.GEONAMEID -> id.humanReadableString),
        (GeonamesFeatureColumns.NAME -> name),
        (GeonamesFeatureColumns.LATITUDE -> lat),
        (GeonamesFeatureColumns.LONGITUDE -> lng),
        (GeonamesFeatureColumns.COUNTRY_CODE -> countryCode),
        (GeonamesFeatureColumns.ADMIN1_CODE -> adminCode1)
      ) 

      val supplementalAttrubs: Map[GeonamesFeatureColumns.Value, String]  = List(
        feature.propMap.get("adminCode2").map(c => (GeonamesFeatureColumns.ADMIN2_CODE -> c)),
        feature.propMap.get("adminCode3").map(c => (GeonamesFeatureColumns.ADMIN3_CODE -> c)),
        feature.propMap.get("adminCode4").map(c => (GeonamesFeatureColumns.ADMIN4_CODE -> c)),
        feature.propMap.get("fcode").map(c => (GeonamesFeatureColumns.FEATURE_CODE -> c)),
        feature.propMap.get("fclass").map(c => (GeonamesFeatureColumns.FEATURE_CLASS -> c))
      ).flatMap(x => x).toMap

      PolygonLoader.adHocIdCounter += 1
      val gnfeature = new GeonamesFeature(attribMap ++ supplementalAttrubs)
      println("making adhoc feature: " + id + " " + id.longId)
      parser.parseFeature(gnfeature)
      id.humanReadableString
    }).orElse({
      logger.error("no id on %s".format(feature))
      None
    })
  }

  def processFeatureIterator(
      defaultNamespace: FeatureNamespace,
      features: ShapeIterator,
      polygonMappingConfig: Option[PolygonMappingConfig]
    ) {
    val fparts = features.file.getName().split("\\.")
    for {
      feature <- features
      geom <- feature.geometry
    } {
      val geoid: List[String] = fparts.lift(0).flatMap(p => Helpers.TryO(p.toInt.toString))
        .orElse(feature.propMap.get("geonameid")
        .orElse(feature.propMap.get("qs_gn_id"))
        .orElse(feature.propMap.get("gn_id"))
        .orElse(maybeMatchFeature(feature, polygonMappingConfig))
        .orElse(maybeMakeFeature(feature))
      ).toList.filterNot(_.isEmpty).flatMap(_.split(",")).map(_.replace(".0", ""))
      geoid.foreach(id => {
        updateRecord(store, defaultNamespace, id, geom)
      })
    }
  }

  def load(defaultNamespace: FeatureNamespace, f: File): Unit = {
    val previousRecordsUpdated = recordsUpdated
    logger.info("processing %s".format(f))
    val fparts = f.getName().split("\\.")
    val extension = fparts.lastOption.getOrElse("")
    val shapeFileExtensions = List("shx", "dbf", "prj", "xml", "cpg")

    if (extension == "json" || extension == "geojson") {
      processFeatureIterator(defaultNamespace, new GeoJsonIterator(f), getMappingForFile(f))
    } else if (extension == "shp") {
      processFeatureIterator(defaultNamespace, new ShapefileIterator(f), getMappingForFile(f))
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
