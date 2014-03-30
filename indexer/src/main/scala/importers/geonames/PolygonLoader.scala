// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import org.opengis.feature.simple.SimpleFeature
import com.foursquare.geo.shapes.FsqSimpleFeature
import com.foursquare.geo.shapes.{GeoJsonIterator, ShapefileIterator, ShapeIterator}
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.foursquare.twofishes.util.{FeatureNamespace, StoredFeatureId, AdHocId, NameNormalizer}
import com.foursquare.twofishes.util.Helpers
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.mongodb.casbah.Imports._
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, WKTReader}
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.geojson.geom.GeometryJSON
import com.weiglewilczek.slf4s.Logging
import java.io.File
import scalaj.collection.Implicits._
import com.mongodb.Bytes
import java.text.Normalizer
import java.text.Normalizer.Form
import com.rockymadden.stringmetric.transform._
import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import com.rockymadden.stringmetric.phonetic.MetaphoneMetric
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.ibm.icu.text.Transliterator

object PolygonLoader {
  var adHocIdCounter = 1
}

case class PolygonMappingConfig (
  nameFields: List[String],
  woeTypes: List[List[String]],
  idField: Option[String]
) {
  private val _woeTypes: List[List[YahooWoeType]] = {
    woeTypes.map(_.map(s => Option(YahooWoeType.findByNameOrNull(s)).getOrElse(
      throw new Exception("Unknown woetype: %s".format(s)))))
  }

  def getWoeTypes() = _woeTypes
  def getAllWoeTypes() = getWoeTypes.flatten
}

class PolygonLoader(
  parser: GeonamesParser,
  store: GeocodeStorageWriteService
) extends Logging {
  val wktReader = new WKTReader()
  val wkbWriter = new WKBWriter()

  def getMappingForFile(f: File): Option[PolygonMappingConfig] = {
    implicit val formats = DefaultFormats
    val file = new File(f.getPath() + ".mapping.json")
    if (file.exists()) {
      val json = scala.io.Source.fromFile(file).getLines.mkString("")
      Some(
         parse(json).extract[PolygonMappingConfig]
      )
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

  def buildQuery(geometry: Geometry, woeTypes: List[YahooWoeType]) = {
    // we can't use the geojson this spits out because it's a string and 
    // seeming MongoDBObject has no from-string parser
    // The bounding box is easier to reason about anyway.
    // val geoJson = GeometryJSON.toString(geometry)
    val envelope = geometry.getEnvelopeInternal()
    envelope.expandBy(0.1)
    MongoDBObject(
      "loc" ->
      MongoDBObject("$geoWithin" ->
        MongoDBObject("$geometry" ->
          MongoDBObject(
            "type" -> "Polygon",
            "coordinates" -> List(List(
              List(envelope.getMinX(), envelope.getMinY()),
              List(envelope.getMaxX(), envelope.getMinY()),
              List(envelope.getMaxX(), envelope.getMaxY()),
              List(envelope.getMinX(), envelope.getMaxY()),
              List(envelope.getMinX(), envelope.getMinY())
            ))
          )
        )
      ),
      "_woeType" -> MongoDBObject("$in" -> woeTypes.map(_.getValue()))
    )
  }

  def findMatchCandidates(geometry: Geometry, woeTypes: List[YahooWoeType]) = {
    val candidateCursor = MongoGeocodeDAO.find(buildQuery(geometry, woeTypes))
    candidateCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
    candidateCursor
  }

  def removeDiacritics(text: String) = {
    NameNormalizer.normalize(text)
  }

  // really, we should apply all the normalizations to shape names that we do to 
  // geonames
  val spaceRegex = " +".r
  val transliterator = Transliterator.getInstance("Any-Latin; NFD;")
  def applyHacks(originalText: String): List[String] = {
    val text = originalText
    val translit = transliterator.transform(text)

    val names = (List(text, translit) ++ parser.doDelete(text)).toSet
    (parser.doRewrites(names.toList) ++ names).toSet.toList.map(removeDiacritics)
  }

  def isGoodEnoughNameMatch(namesFromFeature: List[String], namesFromShape: List[String]): Boolean = {
    val namesFromShape_Modified = namesFromShape.flatMap(applyHacks)
    val namesFromFeature_Modified = namesFromFeature.flatMap(applyHacks)

    val composedTransform = (filterAlpha andThen ignoreAlphaCase)

    namesFromShape_Modified.exists(ns => 
      namesFromFeature_Modified.exists(ng => {
        ng.nonEmpty && ns.nonEmpty &&
        (ng == ns ||
          // MetaphoneMetric withTransform composedTransform).compare(ns, ng).getOrElse(false) ||
          (JaroWinklerMetric withTransform composedTransform).compare(ns, ng).getOrElse(0.0) > 0.95
        )
      })
    )
  }

  def hasName(config: PolygonMappingConfig, feature: FsqSimpleFeature): Boolean = {
    config.nameFields.view.flatMap(nameField =>
      feature.propMap.get(nameField)
    ).filterNot(_.isEmpty).nonEmpty
  }

  def debugFeature(config: PolygonMappingConfig, feature: FsqSimpleFeature): String = {
    val names = config.nameFields.view.flatMap(nameField =>
      feature.propMap.get(nameField)
    ).filterNot(_.isEmpty)

    val firstName = names.headOption.getOrElse("????")

    val id = getId(config, feature)

    "%s: %s (%s)".format(id, firstName, names.toSet.filterNot(_ =? firstName).mkString(", "))
  }


  def debugFeature(feature: FsqSimpleFeature): String = {
    feature.propMap.filterNot(_._1 == "the_geom").toString
  }

  val parenNameRegex = "^(.*) \\((.*)\\)$".r
  def isAcceptableMatch(
    feature: FsqSimpleFeature,
    config: PolygonMappingConfig,
    candidate: GeocodeRecord,
    withLogging: Boolean = false
  ): Boolean = {
    val originalFeatureNames = config.nameFields.flatMap(nameField =>
      feature.propMap.get(nameField)
    ).filterNot(_.isEmpty)
    // some OSM feature names look like A (B), make that into two extra names
    // This is a terrible way to execute this transform
    val featureNames = originalFeatureNames ++ originalFeatureNames.flatMap(n => {
      parenNameRegex.findFirstIn(n).toList.flatMap(_ => {
        List(
          parenNameRegex.replaceAllIn(n, "$1"),
          parenNameRegex.replaceAllIn(n, "$2")
        )
      })
    }).flatMap(_.split(",").map(_.trim))

    val candidateNames = candidate.displayNames.map(_.name).filterNot(_.isEmpty)
    val nameMatch = isGoodEnoughNameMatch(featureNames, candidateNames)
    if (withLogging) {
      if (!nameMatch) {
        logger.info(
          "%s vs %s -- %s vs %s".format(
            candidate.featureId.humanReadableString,
            config.idField.flatMap(feature.propMap.get).getOrElse(0),
            featureNames.flatMap(applyHacks).toSet, candidateNames.flatMap(applyHacks).toSet
          )
        )
      }
    }
    nameMatch
  }

  def debugFeature(r: GeocodeRecord): String = r.debugString
  def getId(polygonMappingConfig: PolygonMappingConfig, feature: FsqSimpleFeature) = {
    polygonMappingConfig.idField.map(f => feature.propMap.get(f)).getOrElse("????")
  }

  def maybeMatchFeature(
    config: PolygonMappingConfig,
    feature: FsqSimpleFeature,
    geometry: Geometry
  ): Option[String] = Helpers.TryO {
    var candidatesSeen = 0

    if (!hasName(config, feature)) {
      logger.info("no names on " + debugFeature(config, feature) + " - " + buildQuery(geometry, config.getAllWoeTypes))
      return None
    }

    def matchAtWoeType(woeTypes: List[YahooWoeType], withLogging: Boolean = false): List[GeocodeRecord] = {
      val candidates = findMatchCandidates(geometry, woeTypes)

      candidates.filter(candidate => {
        candidatesSeen += 1
        // println(debugFeature(candidate))
        isAcceptableMatch(feature, config, candidate, withLogging)
      }).toList
    }

    val matchingFeatures: Seq[GeocodeRecord] = {
      // woeTypes are a list of lists, in descening order of preference 
      // each list is taken as equal prefence, but higher precedence than the
      // next list. If woeTypes looks like [[ADMIN3], [ADMIN2]], and we find
      // an admin3 match, we won't even bother looking for an admin2 match.
      // If it was [[ADMIN3, ADMIN2]], we would look for both, and if we
      // found two matches, take them both
      val acceptableCandidates: List[GeocodeRecord] = 
        config.getWoeTypes.view
          .map(woeTypes => matchAtWoeType(woeTypes))
          .find(_.nonEmpty).toList.flatten

      if (candidatesSeen == 0) {
        logger.info("failed couldn't find any candidates for " + debugFeature(config, feature) + " - " + buildQuery(geometry, config.getAllWoeTypes))
      } else if (acceptableCandidates.isEmpty) {
        logger.info("failed to match: %s".format(debugFeature(config, feature)))
        logger.info("%s".format(buildQuery(geometry, config.getAllWoeTypes)))
        matchAtWoeType(config.getAllWoeTypes, withLogging = true)
      } else {
        logger.info("matched %s %s to %s".format(
          debugFeature(config, feature),
          getId(config, feature),
          acceptableCandidates.map(debugFeature).mkString(", ")
        ))
      }

      acceptableCandidates
    }

    if (matchingFeatures.isEmpty) {
      None
    } else {
      val r = Some(matchingFeatures.map(_.featureId.humanReadableString).mkString(","))
      r
    }
  }

  def maybeMakeFeature(feature: FsqSimpleFeature): Option[String] = {
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
      logger.error("no id on %s".format(debugFeature(feature)))
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
      (rawFeature, index) <- features.zipWithIndex
      feature = new FsqSimpleFeature(rawFeature)
      geom <- feature.geometry
    } {
      if (index % 100 == 0) {
        println("processing feature %d".format(index))
      }
      val geoid: List[String] = fparts.lift(0).flatMap(p => Helpers.TryO(p.toInt.toString))
        .orElse(feature.propMap.get("geonameid")
        .orElse(feature.propMap.get("qs_gn_id"))
        .orElse(feature.propMap.get("gn_id"))
        .orElse(polygonMappingConfig.flatMap(config => maybeMatchFeature(config, feature, geom)))
        .orElse(maybeMakeFeature(feature))
      ).toList.filterNot(_.isEmpty).flatMap(_.split(",")).map(_.replace(".0", ""))
      println(geoid)
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
