// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.geo.shapes.{FsqSimpleFeature, GeoJsonIterator, ShapeIterator, ShapefileIterator}
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes._
import com.foursquare.twofishes.mongo.{GeocodeStorageWriteService, PolygonIndex, PolygonIndexDAO, MongoGeocodeDAO, RevGeoIndexDAO}
import com.foursquare.twofishes.util.{AdHocId, FeatureNamespace, Helpers, NameNormalizer, StoredFeatureId}
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.ibm.icu.text.Transliterator
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._
import com.mongodb.MongoException
import com.rockymadden.stringmetric.phonetic.MetaphoneMetric
import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import com.rockymadden.stringmetric.transform._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, WKTReader}
import com.weiglewilczek.slf4s.Logging
import java.io.{File, FileWriter, OutputStreamWriter, Writer}
import org.bson.types.ObjectId
import org.geotools.geojson.geom.GeometryJSON
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scalaj.collection.Implicits._

object PolygonLoader {
  var adHocIdCounter = 1
}

case class PolygonMappingConfig (
  nameFields: List[String],
  woeTypes: List[List[String]],
  idField: String,
  source: Option[String]
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
  store: GeocodeStorageWriteService,
  shouldCreateFeatures: Boolean = false
) extends Logging {
  val wktReader = new WKTReader()
  val wkbWriter = new WKBWriter()

  val mappingExtension = ".mapping.json"
  val matchExtension = ".match.tsv"

  def getMappingForFile(f: File): Option[PolygonMappingConfig] = {
    implicit val formats = DefaultFormats
    val file = new File(f.getPath() + mappingExtension)
    if (file.exists()) {
      val json = scala.io.Source.fromFile(file).getLines.mkString("")
      Some(
         parse(json).extract[PolygonMappingConfig]
      )
    } else {
      None
    }
  }

  def getMatchingForFile(
    f: File,
    defaultNamespace: FeatureNamespace
  ): Map[String, Seq[StoredFeatureId]] = {
    val file = new File(f.getPath() + matchExtension)
    if (file.exists()) {
      val lines = scala.io.Source.fromFile(file).getLines
      lines.map(line => {
        val parts = line.split("\t")
        if (parts.size != 2) {
          throw new Exception("broken line in %s:\n%s".format(file.getPath(), line))
        }
        (
          parts(0) -> parts(1).split(",").map(geoid => {
            StoredFeatureId.fromHumanReadableString(geoid, Some(defaultNamespace)).getOrElse(
              throw new Exception("broken geoid %s in file %s on line: %s".format(
                geoid, file.getPath(), line
              ))
            )
          }).toList
        )
      }).toMap
    } else {
      Map.empty
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

  def indexPolygon(
    polyId: ObjectId,
    geom: Geometry,
    source: String
  ) {
    val geomBytes = wkbWriter.write(geom)
    PolygonIndexDAO.save(PolygonIndex(polyId, geomBytes, source))
    parser.revGeoMaster ! CalculateCover(polyId, geomBytes)
  }

  def updateRecord(
    store: GeocodeStorageWriteService,
    geoids: List[StoredFeatureId],
    geom: Geometry,
    source: String
  ): Unit = {
    if (geoids.isEmpty) { return }

    val polyId = new ObjectId()
    indexPolygon(polyId, geom, source)

    for {
      geoid <- geoids
    } {
      try {
        logger.debug("adding poly %s to %s %s".format(polyId, geoid, geoid.longId))
        recordsUpdated += 1
        store.addPolygonToRecord(geoid, polyId)
      } catch {
        case e: Exception => {
          throw new Exception("couldn't write poly to %s".format(geoid), e)
        }
      }
    }
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
      PolygonIndexDAO.find(MongoDBObject())
        .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc

    val wkbReader = new WKBReader()
    for {
      (polyRecord, index) <- polygons.zipWithIndex
      featureRecord <- MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> List(polyRecord._id))))
      polyData = polyRecord.polygon
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

          PolygonIndexDAO.remove(polyRecord)
      }
    }
    logger.info("done reading in polys")
    parser.revGeoMaster ! Done
  }

  def rebuildRevGeoIndex {
    RevGeoIndexDAO.collection.drop()
    PolygonIndexDAO.find(MongoDBObject()).foreach(p => {
      parser.revGeoMaster ! CalculateCover(p._id, p.polygon)
    })
    logger.info("done reading in polys")
    parser.revGeoMaster ! Done
  }

  def buildQuery(geometry: Geometry, woeTypes: List[YahooWoeType]) = {
    // we can't use the geojson this spits out because it's a string and
    // seeming MongoDBObject has no from-string parser
    // The bounding box is easier to reason about anyway.
    // val geoJson = GeometryJSON.toString(geometry)
    val envelope = geometry.getEnvelope().buffer(0.01).getEnvelope()
    val coords = envelope.getCoordinates().toList

    MongoDBObject(
      "loc" ->
      MongoDBObject("$geoWithin" ->
        MongoDBObject("$geometry" ->
          MongoDBObject(
            "type" -> "Polygon",
            "coordinates" -> List(List(
              List(coords(0).x, coords(0).y),
              List(coords(1).x, coords(1).y),
              List(coords(2).x, coords(2).y),
              List(coords(3).x, coords(3).y),
              List(coords(0).x, coords(0).y)
            ))
          )
        )
      ),
      "_woeType" -> MongoDBObject("$in" -> woeTypes.map(_.getValue()))
    )
  }

  def findMatchCandidates(geometry: Geometry, woeTypes: List[YahooWoeType]): Iterator[GeocodeRecord] = {
    val query = buildQuery(geometry, woeTypes)
    val size = MongoGeocodeDAO.count(query)
    if (size > 10000) {
      logger.error("result set too big: %s for %s".format(size, query))
      return Iterator.empty
    } else if (size > 2000) {
      logger.info("oversize result set: %s for %s".format(size, query))
    }

    val candidateCursor = MongoGeocodeDAO.find(query)
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

  def bestNameScore(namesFromFeature: List[String], namesFromShape: List[String]): Int = {
    val namesFromShape_Modified = namesFromShape.flatMap(applyHacks)
    val namesFromFeature_Modified = namesFromFeature.flatMap(applyHacks)

    val composedTransform = (filterAlpha andThen ignoreAlphaCase)

    val scores = namesFromShape_Modified.flatMap(ns => {
      namesFromFeature_Modified.map(ng => {
        if (ng.nonEmpty && ns.nonEmpty) {
          if (ng == ns) {
            return 100
          } else {
            val jaroScore = (JaroWinklerMetric withTransform composedTransform).compare(ns, ng).getOrElse(0.0)
            jaroScore * 100
          }
        } else {
          0
        }
      })
    })

    if (scores.nonEmpty) { scores.max.toInt } else { 0 }
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

  case class PolygonMatch(
    candidate: GeocodeRecord,
    var nameScore: Int = 0
  )

  val parenNameRegex = "^(.*) \\((.*)\\)$".r
  def scoreMatch(
    feature: FsqSimpleFeature,
    config: PolygonMappingConfig,
    candidate: GeocodeRecord,
    withLogging: Boolean = false
  ): PolygonMatch = {
    val polygonMatch = PolygonMatch(candidate)

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
    polygonMatch.nameScore = bestNameScore(featureNames, candidateNames)

    if (withLogging) {
      if (!isGoodEnoughMatch(polygonMatch)) {
        logger.debug(
          "%s vs %s -- %s vs %s".format(
            candidate.featureId.humanReadableString,
            feature.propMap.get(config.idField).getOrElse("0"),
            featureNames.flatMap(applyHacks).toSet, candidateNames.flatMap(applyHacks).toSet
          )
        )
      }
    }

    polygonMatch
  }

  def debugFeature(r: GeocodeRecord): String = r.debugString
  def getId(polygonMappingConfig: PolygonMappingConfig, feature: FsqSimpleFeature) = {
    feature.propMap.get(polygonMappingConfig.idField).getOrElse("????")
  }

  def isGoodEnoughMatch(polygonMatch: PolygonMatch) = {
    polygonMatch.nameScore > 95
  }

  def maybeMatchFeature(
    config: PolygonMappingConfig,
    feature: FsqSimpleFeature,
    geometry: Geometry,
    outputMatchWriter: Option[Writer]
  ): List[StoredFeatureId] = {
    var candidatesSeen = 0

    if (!hasName(config, feature)) {
      logger.info("no names on " + debugFeature(config, feature) + " - " + buildQuery(geometry, config.getAllWoeTypes))
      return Nil
    }

    def matchAtWoeType(woeTypes: List[YahooWoeType], withLogging: Boolean = false): Seq[GeocodeRecord] = {
      try {
        val candidates = findMatchCandidates(geometry, woeTypes)
        val scores: Seq[PolygonMatch] = candidates.map(candidate => {
          candidatesSeen += 1
          // println(debugFeature(candidate))
          scoreMatch(feature, config, candidate, withLogging)
        }).filter(isGoodEnoughMatch).toList
          
        val scoresMap = scores.groupBy(_.nameScore)

        if (scoresMap.nonEmpty) {
          scoresMap(scoresMap.keys.max).map(_.candidate)
        } else {
          Nil
        }
      } catch {
        // sometimes our bounding box wraps around the world and mongo throws an error
        case e: MongoException => Nil
      }
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
        logger.debug("failed couldn't find any candidates for " + debugFeature(config, feature) + " - " + buildQuery(geometry, config.getAllWoeTypes))
      } else if (acceptableCandidates.isEmpty) {
        logger.debug("failed to match: %s".format(debugFeature(config, feature)))
        logger.debug("%s".format(buildQuery(geometry, config.getAllWoeTypes)))
        matchAtWoeType(config.getAllWoeTypes, withLogging = true)
      } else {
        logger.debug("matched %s %s to %s".format(
          debugFeature(config, feature),
          getId(config, feature),
          acceptableCandidates.map(debugFeature).mkString(", ")
        ))
      }

      acceptableCandidates
    }


    if (matchingFeatures.isEmpty) {
      Nil
    } else {
      val idStr = matchingFeatures.map(_.featureId.humanReadableString).mkString(",")
      outputMatchWriter.foreach(_.write("%s\t%s\n".format(
        feature.propMap.get(config.idField).getOrElse(throw new Exception("missing id")),
        idStr
      )))
      matchingFeatures.map(_.featureId).toList
    }
  }

  def maybeMakeFeature(feature: FsqSimpleFeature): Option[StoredFeatureId] = {
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
      logger.debug("making adhoc feature: " + id + " " + id.longId)
      parser.insertGeocodeRecords(List(parser.parseFeature(gnfeature)))
      id
    }).orElse({
      logger.error("no id on %s".format(debugFeature(feature)))
      None
    })
  }

  def processFeatureIterator(
      defaultNamespace: FeatureNamespace,
      features: ShapeIterator,
      polygonMappingConfig: Option[PolygonMappingConfig],
      matchingTable: Map[String, Seq[StoredFeatureId]]
    ) {
    val fparts = features.file.getName().split("\\.")
    lazy val outputMatchWriter = polygonMappingConfig.map(c => {
      val filename = features.file.getPath() + matchExtension
      new FileWriter(filename, true);
    })
    for {
      (rawFeature, index) <- features.zipWithIndex
      feature = new FsqSimpleFeature(rawFeature)
      geom <- feature.geometry
    } {
      if (index % 100 == 0) {
        logger.info("processing feature %d".format(index))
      }

      val fidsFromFileName = fparts.lift(0).flatMap(p => Helpers.TryO(p.toInt.toString))
        .flatMap(i => StoredFeatureId.fromHumanReadableString(i, Some(defaultNamespace))).toList

      val geoidColumns = List("geonameid", "qs_gn_id", "gn_id")
      val geoidsFromFile = geoidColumns.flatMap(feature.propMap.get)
      val fidsFromFile = geoidsFromFile
        .filterNot(_.isEmpty).flatMap(_.split(",")).map(_.replace(".0", ""))
        .flatMap(i => StoredFeatureId.fromHumanReadableString(i, Some(defaultNamespace)))

      val fids: List[StoredFeatureId] = if (fidsFromFileName.nonEmpty) {
        fidsFromFileName
      } else if (fidsFromFile.nonEmpty) {
        fidsFromFile
      } else {
        val matches: List[StoredFeatureId] = polygonMappingConfig.toList.flatMap(config => {
          val idOpt: Option[String] = feature.propMap.get(config.idField)
          val priorMatches: List[StoredFeatureId] = idOpt.toList.flatMap(id => { matchingTable.get(id).getOrElse(Nil) })
          if (priorMatches.nonEmpty) {
            priorMatches
          } else {
            maybeMatchFeature(config, feature, geom, outputMatchWriter).toList
          }
        })

        if (matches.nonEmpty) {
          matches
        } else {
          maybeMakeFeature(feature).toList
        }
      }

      val source = polygonMappingConfig.flatMap(_.source).getOrElse(features.file.getName())
      updateRecord(store, fids, geom, source)
    }
    outputMatchWriter.foreach(_.close())
  }

  def load(defaultNamespace: FeatureNamespace, f: File): Unit = {
    val previousRecordsUpdated = recordsUpdated
    logger.info("processing %s".format(f))
    val fparts = f.getName().split("\\.")
    val extension = fparts.lastOption.getOrElse("")
    val shapeFileExtensions = List("shx", "dbf", "prj", "xml", "cpg")

    if ((extension == "json" || extension == "geojson") && !f.getName().endsWith(mappingExtension)) {
      processFeatureIterator(
        defaultNamespace,
        new GeoJsonIterator(f),
        getMappingForFile(f),
        getMatchingForFile(f, defaultNamespace)
      )
      logger.info("%d records updated by %s".format(recordsUpdated - previousRecordsUpdated, f))
    } else if (extension == "shp") {
      processFeatureIterator(
        defaultNamespace,
        new ShapefileIterator(f),
        getMappingForFile(f),
        getMatchingForFile(f, defaultNamespace)
      )
      logger.info("%d records updated by %s".format(recordsUpdated - previousRecordsUpdated, f))
    } else if (shapeFileExtensions.has(extension)) {
      // do nothing, shapefile aux file
      Nil
    }
  }
}
