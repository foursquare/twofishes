// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import akka.actor.{ActorSystem, Props}
import com.foursquare.geo.quadtree.CountryRevGeo
import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes._
import com.foursquare.twofishes.mongo._
import com.foursquare.twofishes.output._
import com.foursquare.twofishes.util.{DurationUtils, GeoTools, GeonamesId, GeonamesNamespace, Helpers, NameNormalizer,
    StoredFeatureId}
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.ostrich.admin.config.AdminServiceConfig
import com.twitter.ostrich.stats.Stats
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import com.weiglewilczek.slf4s.Logging
import java.io.File
import java.util.concurrent.CountDownLatch
import org.bson.types.ObjectId
import org.json4s.NoTypeHints
import org.json4s.jackson.{JsonMethods, Serialization}
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable.{HashMap, HashSet}
import scala.io.Source
import scala.util.matching.Regex
import scalaj.collection.Implicits._

object GeonamesParser extends DurationUtils {
  var config: GeonamesImporterConfig = null

  var countryLangMap = new HashMap[String, List[String]]()
  var countryNameMap = new HashMap[String, String]()
  var adminIdMap = new HashMap[String, String]()

  val adminConfig = new AdminServiceConfig {
    httpPort = 7655
  }
  val runtime = RuntimeEnvironment(this, Array.empty)
  val admin = adminConfig()(runtime)

  lazy val naturalEarthPopulatedPlacesMap: Map[StoredFeatureId, SimpleFeature] = {
    new ShapefileIterator("data/downloaded/ne_10m_populated_places_simple.shp").flatMap(f => {
      f.propMap.get("geonameid").map(id => {
        (GeonamesId(id.toDouble.toLong) -> f)
      })
    }).toMap
  }

  def parseCountryInfo() {
    val fileSource = scala.io.Source.fromFile(new File("data/downloaded/countryInfo.txt"))
    val lines = fileSource.getLines.filterNot(_.startsWith("#"))
    lines.foreach(l => {
      val parts = l.split("\t")
      val cc = parts(0)
      val englishName = parts(4)
      val langs = parts(15).split(",").map(l => l.split("-")(0)).toList
      val geonameid = parts(16)
      countryLangMap += (cc -> langs)
      countryNameMap += (cc -> englishName)
      adminIdMap += (cc -> geonameid)
    })
  }

  def parseAdminInfoFile(filename: String) {
    // adm1, name, name, geonameid
    val fileSource = scala.io.Source.fromFile(new File(filename))
    val lines = fileSource.getLines.filterNot(_.startsWith("#"))
    lines.foreach(l => {
      val parts = l.split("\t")
      val admCode = parts(0)
      val geonameid = parts(3)
      adminIdMap += (admCode -> geonameid)
    })
  }

  val store = new MongoGeocodeStorageService()
  lazy val slugIndexer = new SlugIndexer()

  def main(args: Array[String]) {
    config = GeonamesImporterConfigParser.parse(args)
    val parser = new GeonamesParser(store, slugIndexer)

     try {
      CountryRevGeo.getNearestCountryCode(40.74, -74)
    } catch {
       case e: Exception => {
        println("caught exception in country revgeo warmup, no idea what's wrong")
        System.exit(1)
      }
    }

    if (config.reloadData) {
      MongoGeocodeDAO.collection.drop()
      NameIndexDAO.collection.drop()
      PolygonIndexDAO.collection.drop()
      RevGeoIndexDAO.collection.drop()
      S2CoveringIndexDAO.collection.drop()
      S2InteriorIndexDAO.collection.drop()
      parser.loadIntoMongo()
      writeIndexes(parser.s2CoveringLatch)
    } else {
      writeIndexes(None)
    }


    implicit val formats = Serialization.formats(NoTypeHints)
    val prettyJsonStats = Serialization.writePretty(JsonMethods.parse(Stats.get().toJson))
    logger.info(prettyJsonStats)
    logger.info("all done with parse, trying to shutdown admin server and exit")
    admin.foreach(_.shutdown())
    System.exit(0)
  }

  def makeFinalIndexes() {
    logPhase("making indexes before generating output") {
      PolygonIndexDAO.makeIndexes()
      RevGeoIndexDAO.makeIndexes()
      S2CoveringIndexDAO.makeIndexes()
      S2InteriorIndexDAO.makeIndexes()
    }
  }

  def writeIndex(args: Array[String]) {
    config = GeonamesImporterConfigParser.parse(args)
    writeIndexes(None)
  }

  // TODO(blackmad): if we aren't redoing mongo indexing
  // then add some code to see if the s2 index is 'done'
  // We should also add an option to skip reloading polys
  def writeIndexes(s2CoveringLatch: Option[CountDownLatch]) {
    makeFinalIndexes()
    val outputter = new OutputIndexes(
      config.hfileBasePath,
      config.outputPrefixIndex,
      GeonamesParser.slugIndexer.slugEntryMap,
      config.outputRevgeo,
      config.outputS2Covering,
      config.outputS2Interior
    )
    outputter.buildIndexes(s2CoveringLatch)
  }
}

// (country -> tokenlist)
case class ShortenInfo(from: Regex, to: String, flags: Int)

import GeonamesParser._
class GeonamesParser(
  store: GeocodeStorageWriteService,
  slugIndexer: SlugIndexer
) extends Logging {
  lazy val polygonLoader = new PolygonLoader(this, store, config)
  lazy val hierarchyTable = HierarchyParser.parseHierarchy(List(
    "data/downloaded/hierarchy.txt",
    "data/private/hierarchy.txt",
    "data/custom/hierarchy.txt"
  ))

  // token -> alt tokens
  lazy val rewriteTable = new TsvHelperFileParser(
    "data/custom/rewrites.txt",
    "data/private/rewrites.txt"
  ).gidMap.map({case(from, toList) => {
    (from.r, toList)
  }})

  lazy val shortensList: Map[String, List[ShortenInfo]] = {
    scala.io.Source.fromFile(new File("data/custom/shortens.txt"))
      .getLines.toList.filterNot(_.startsWith("#")).flatMap(l => {
        val parts = l.split("[\\|\t]").toList
        val countries = parts(0).split(",").toList
        val shortenParts = parts.drop(1)
        val toShortenFrom = shortenParts(0) + "\\b"
        val toShortenTo = shortenParts.lift(1).getOrElse("")
        val shortenFlags = parseFeatureNameFlags(shortenParts.lift(2))
        countries.map(cc => (cc -> ShortenInfo(toShortenFrom.r, toShortenTo, shortenFlags)))
      }).groupBy(_._1).mapValues(_.map(_._2)).toList.toMap
    }
  // geonameid -> boost value
  lazy val boostTable = new GeoIdTsvHelperFileParser(GeonamesNamespace,
    "data/custom/boosts.txt",
    "data/private/boosts.txt")

  lazy val deletesList: List[String] = scala.io.Source.fromFile(new File("data/custom/deletes.txt"))
    .getLines.toList.filterNot(_.startsWith("#"))

  // geonameid --> new center
  lazy val moveTable = new GeoIdTsvHelperFileParser(GeonamesNamespace, "data/custom/moves.txt")

  // geonameid -> name to be deleted
  lazy val nameDeleteTable = new GeoIdTsvHelperFileParser(GeonamesNamespace, "data/custom/name-deletes.txt")
  // list of geoids (geonameid:XXX) to skip indexing
  lazy val ignoreList: List[StoredFeatureId] = scala.io.Source.fromFile(new File("data/custom/ignores.txt"))
    .getLines.toList.filterNot(_.startsWith("#")).map(l => GeonamesId(l.toLong))

  // extra parents
  lazy val extraRelationsList = new GeoIdTsvHelperFileParser(GeonamesNamespace,"data/custom/extra-relations.txt")

  lazy val concordanceMap = new GeoIdTsvHelperFileParser(GeonamesNamespace,
    "data/computed/concordances.txt",
    "data/private/concordances.txt")

  val bboxDirs = List(
    new File("data/computed/bboxes/"),
    new File("data/private/bboxes/")
  )
  val bboxFiles = bboxDirs.flatMap(bboxDir => {
    if (bboxDir.exists) { bboxDir.listFiles.toList } else { Nil }
  }).sorted
  lazy val bboxTable = BoundingBoxTsvImporter.parse(bboxFiles)

  val displayBboxDirs = List(
    new File("data/computed/display_bboxes/"),
    new File("data/private/display_bboxes/")
  )
  val displayBboxFiles = displayBboxDirs.flatMap(bboxDir => {
    if (bboxDir.exists) { bboxDir.listFiles.toList } else { Nil }
  }).sorted
  lazy val displayBboxTable = BoundingBoxTsvImporter.parse(displayBboxFiles)

  val helperTables = List(boostTable)

  val system = ActorSystem("S2CoveringSystem")

  val (s2CoveringMaster, s2CoveringLatch) = if (
    config != null && (config.outputRevgeo || config.outputS2Covering || config.outputS2Interior)
  ) {
    val latch = new CountDownLatch(1)
    (Some(system.actorOf(Props(new S2CoveringMaster(latch)), name = "master")), Some(latch))
   } else {
    (None, None)
  }

  def logUnusedHelperEntries {
    helperTables.flatMap(_.logUnused).foreach(line => logger.error(line))
  }

  val wkbWriter = new WKBWriter()
  val wktReader = new WKTReader()

  def loadIntoMongo() {
    parseCountryInfo()

    if (config.importAlternateNames) {
      Helpers.duration("readAlternateNamesFile") {
        loadAlternateNames()
      }
    }

    if (!config.parseWorld) {
      val countries = config.parseCountry.split(",")
      countries.foreach(f => {
        logger.info("Parsing %s".format(f))
        parseAdminInfoFile("data/downloaded/adminCodes-%s.txt".format(f))
        parseAdminFile(
          "data/downloaded/%s.txt".format(f))

        if (config.importPostalCodes) {
          parsePostalCodeFile("data/downloaded/zip/%s.txt".format(f))
        }
      })
    } else {
      parseAdminInfoFile("data/downloaded/adminCodes.txt")
      logPhase("parse global features") {
        parseAdminFile("data/downloaded/allCountries.txt")
      }
      if (config.importPostalCodes) {
        logPhase("parse global postal codes") {
          parsePostalCodeFile("data/downloaded/zip/allCountries.txt")
        }
      }
    }

    val supplementalDirs = List(
      new File("data/computed/features"),
      new File("data/private/features")
    )
    supplementalDirs.foreach(supplementalDir =>
      if (supplementalDir.exists) {
        supplementalDir.listFiles.foreach(f => {
          logPhase("parsing supplemental file: %s".format(f)) {
            parseAdminFile(f.toString, allowBuildings=true)
          }
        })
      }
    )

    logPhase("building name indexes pre parseNameTransforms") {
      NameIndexDAO.makeIndexes()
    }

    logPhase("parseNameTransforms") {
      parseNameTransforms()
    }

    if (config.buildMissingSlugs) {
      logPhase("building missing slugs") {
        slugIndexer.buildMissingSlugs()
        slugIndexer.writeMissingSlugs(store)
      }
    }

    logPhase("building feature indexes pre polygon loading") {
      MongoGeocodeDAO.makeIndexes()
    }
    polygonLoader.load(GeonamesNamespace)
  }

  def doRewrites(names: List[String]): List[String] = {
    val nameSet = new scala.collection.mutable.HashSet[String]()
    rewriteTable.foreach({case(from, toList) => {
      names.foreach(name => {
        toList.values.foreach(to => {
          nameSet += from.replaceAllIn(name, to)
        })
      })
    }})
    nameSet ++= names.map(_.replace("ß", "ss"))
    nameSet.toList
  }

  val bigDeleteRe = {
    val re = deletesList
      .map(_ + "\\b")
      .sortBy(_.size * -1)
      .mkString("|")
    ("(?i)%s".format(re)).r
  }

  def doDelete(name: String): Option[String] = {
    val newName = bigDeleteRe.replaceAllIn(name, "")
    if (newName != name) {
      Some(fixName(newName))
    } else {
      None
    }
  }

  def createNameIndexRecords(displayNames: List[DisplayName], fid: StoredFeatureId, record: Option[GeocodeRecord]) = {
    displayNames.map(name => {
      createNameIndexRecord(name, fid, record)
    })
  }

  def addDisplayNameToNameIndex(dn: DisplayName, fid: StoredFeatureId, record: Option[GeocodeRecord]) = {
    store.addNameIndexes(List(createNameIndexRecord(dn, fid, record)))
  }

  private def isAllDigits(x: String) = x forall Character.isDigit

  private def shouldExcludeFromPrefixIndex(dn: DisplayName, woeType: YahooWoeType): Boolean = {
    // exclude because of flags
    ((dn.flags & (FeatureNameFlags.NEVER_DISPLAY.getValue | FeatureNameFlags.LOW_QUALITY.getValue)) != 0) ||
    // exclude purely numeric names of non-postalcode features
    (woeType !=? YahooWoeType.POSTAL_CODE && isAllDigits(dn.name))
  }

  private def isNameDeleted(name: String, fid: StoredFeatureId): Boolean = {
    nameDeleteTable.get(fid).exists(_ =? name)
  }

  def createNameIndexRecord(dn: DisplayName, fid: StoredFeatureId, record: Option[GeocodeRecord]) = {
    val name = NameNormalizer.normalize(dn.name).trim
    val cc: String = record.map(_.cc).getOrElse("")
    val pop: Int =
      record.flatMap(_.population).getOrElse(0) + record.flatMap(_.boost).getOrElse(0)
    val woeType: Int =
      record.map(_._woeType).getOrElse(0)
    val excludeFromPrefixIndex = shouldExcludeFromPrefixIndex(dn, YahooWoeType.findByIdOrNull(woeType))
    NameIndex(name, fid.longId, cc, pop, woeType, dn.flags, dn.lang, excludeFromPrefixIndex, dn._id)
  }

  def rewriteNames(names: List[String]): (List[String], List[String]) = {
    val deleteModifiedNames: List[String] = names.flatMap(doDelete)

    val deaccentedNames = names.map(NameNormalizer.deaccent).filterNot(n =>
      names.contains(n))

    val rewrittenNames = doRewrites(names ++ deleteModifiedNames).filterNot(n =>
      names.contains(n))

    (deaccentedNames, (deleteModifiedNames ++ rewrittenNames).distinct)
  }

  def parseFeature(feature: InputFeature): GeocodeRecord = {
    val geonameId = feature.featureId

    val ids: List[StoredFeatureId] = List(geonameId) ++
      concordanceMap.get(geonameId).flatMap(concordanceId => {
        GeonamesParser.slugIndexer.slugEntryMap(concordanceId) = (SlugEntry(geonameId.humanReadableString, 0))

        // this isn't great, because it means we need a mapping for the namespace of
        // any concordances in StoredFeatureId, so it's harder to add ad-hoc concordances to
        // external datasets
        if (concordanceId.contains(":")) {
          StoredFeatureId.fromHumanReadableString(concordanceId)
        } else { None }
    })

    val preferredEnglishAltName = alternateNamesMap.getOrElse(geonameId, Nil).find(altName =>
      altName.lang == "en" && altName.isPrefName
    )

    val hasEnglishAltName = alternateNamesMap.getOrElse(geonameId, Nil).exists(_.lang == "en")
    val hasPreferredEnglishAltName = preferredEnglishAltName.isDefined
    val hasNonPreferredEnglishAltNameIdenticalToFeatureName = alternateNamesMap.getOrElse(geonameId, Nil).exists(altName =>
      altName.lang == "en" && !altName.isPrefName && altName.name =? feature.name
    )

    var displayNames: List[DisplayName] = Nil

    // consider using the primary feature name from geonames as an english name:
    // skip: if an identical preferred english alt name exists
    // add as preferred:
    //    if no english alt name exists OR
    //    no preferred english alt name exists BUT an identical non-preferred english name exists
    // add as non-preferred otherwise
    if (!preferredEnglishAltName.exists(_.name =? feature.name)) {
      displayNames ++= processFeatureName(geonameId,
        feature.countryCode, "en", feature.name,
        isPrefName = !hasEnglishAltName ||
          (!hasPreferredEnglishAltName && hasNonPreferredEnglishAltNameIdenticalToFeatureName),
        isShortName = false,
        woeType = feature.featureClass.woeType
      )
    }

    if (feature.featureClass.woeType == YahooWoeType.COUNTRY) {
      countryNameMap.get(feature.countryCode).foreach(name =>
        displayNames ::=
          DisplayName("en", name, FeatureNameFlags.PREFERRED.getValue() | FeatureNameFlags.COLLOQUIAL.getValue())
      )
    }

    feature.asciiname.foreach(asciiname => {
      val (deaccentedPrimary, _) = rewriteNames(List(feature.name))
      if (feature.name != asciiname &&
          asciiname.nonEmpty &&
          // do not add if this name has been deleted
          !isNameDeleted(asciiname, geonameId) &&
          // or if this is the same as the primary name deaccented and the primary name has been deleted
          !(deaccentedPrimary.has(asciiname) && isNameDeleted(feature.name, geonameId))) {
        displayNames ::=
          DisplayName("en", asciiname,
            FeatureNameFlags.DEACCENT.getValue)
      }
    })

    if (feature.featureClass.woeType.getValue == YahooWoeType.COUNTRY.getValue) {
      displayNames ::= DisplayName("abbr", feature.countryCode, 0)
    }

    // Build names
    val alternateNames = alternateNamesMap.getOrElse(geonameId, Nil)

    val altNames = alternateNames.flatMap(altName => {
      processFeatureName(geonameId,
        feature.countryCode, altName.lang, altName.name,
        isPrefName=altName.isPrefName,
        isShortName=altName.isShortName,
        isColloquial=altName.isColloquial,
        isHistoric=altName.isHistoric,
        woeType = feature.featureClass.woeType)
    })
    val (deaccentedFeatureNames, nonDeaccentedFeatureNames) = altNames.partition(n => (n.flags & FeatureNameFlags.DEACCENT.getValue) > 0)
    val nonDeaccentedNames: Set[String] = nonDeaccentedFeatureNames.map(_.name).toSet
    displayNames ++= nonDeaccentedFeatureNames
    displayNames ++= deaccentedFeatureNames.filterNot(n => nonDeaccentedNames.has(n.name))


    // the admincode is the internal geonames admin code, but is very often the
    // same short name for the admin area that is actually used in the country
    if (feature.featureClass.isAdmin1 || feature.featureClass.isAdmin2 || feature.featureClass.isAdmin3) {
      displayNames ++= feature.adminCode.toList.flatMap(code => {
        if (!isAllDigits(code)) {
          Some(DisplayName("abbr", code, FeatureNameFlags.ABBREVIATION.getValue))
        } else {
          Some(DisplayName("", code, FeatureNameFlags.NEVER_DISPLAY.getValue))
        }
      })
    }

    def fixParent(p: String): Option[String] = {
      adminIdMap.get(p) orElse {
        //println("missing admin lookup for %s".format(p))
        None
      }
    }

    // Build parents
    val extraParents: List[StoredFeatureId] =
      feature.extraColumns.get("parents").toList.flatMap(_.split(",").toList).flatMap(pStr =>
        StoredFeatureId.fromHumanReadableString(pStr))
    val parents: List[StoredFeatureId] =
      feature.parents.flatMap(fixParent).map(p => GeonamesId(p.toLong))
    val hierarchyParents: List[StoredFeatureId] =
      hierarchyTable.getOrElse(geonameId, Nil).filterNot(p => parents.has(p))

    val allParents: List[StoredFeatureId] = extraParents ++ parents ++ hierarchyParents

    val boost: Option[Int] =
      feature.extraColumns.get("boost").map(_.toInt) orElse
        boostTable.get(geonameId).headOption.flatMap(boost =>
          TryO { boost.toInt }
        )

    val bbox = feature.extraColumns.get("bbox").flatMap(bboxStr => {
      // west, south, east, north
      val parts = bboxStr.split(",").map(_.trim)
      parts.toList match {
        case w :: s :: e :: n :: Nil => {
          Some(BoundingBox(Point(n.toDouble, e.toDouble), Point(s.toDouble, w.toDouble)))
        }
        case _ => {
          logger.error("malformed bbox: " + bboxStr)
          None
        }
      }
    }) orElse bboxTable.get(geonameId)

    var lat = feature.latitude
    var lng = feature.longitude

    val latlngs = moveTable.get(geonameId)
    if (latlngs.size > 0) {
      lat = latlngs(0).toDouble
      lng = latlngs(1).toDouble
    }

    val canGeocode = feature.extraColumns.get("canGeocode").map(_.toInt).getOrElse(1) > 0

    val slug: Option[String] = slugIndexer.getBestSlug(geonameId)

    if (slug.isEmpty &&
      List(YahooWoeType.TOWN, YahooWoeType.SUBURB, YahooWoeType.COUNTRY, YahooWoeType.ADMIN1, YahooWoeType.ADMIN2).has(feature.featureClass.woeType)) {
      slugIndexer.missingSlugList.add(geonameId.humanReadableString)
    }

    var attributesSet = false
    lazy val attributesBuilder = {
      attributesSet = true
      GeocodeFeatureAttributes.newBuilder
    }

    naturalEarthPopulatedPlacesMap.get(geonameId).map(sfeature => {
      sfeature.propMap.get("adm0cap").foreach(v =>
        attributesBuilder.adm0cap(v.toDouble.toInt == 1)
      )
      sfeature.propMap.get("worldcity").foreach(v =>
        attributesBuilder.worldcity(v.toDouble.toInt == 1)
      )
      sfeature.propMap.get("scalerank").foreach(v =>
        attributesBuilder.scalerank(v.toInt)
      )
      sfeature.propMap.get("natscale").foreach(v =>
        attributesBuilder.natscale(v.toInt)
      )
      sfeature.propMap.get("labelrank").foreach(v =>
        attributesBuilder.labelrank(v.toInt)
      )
    })

    if (feature.featureClass.isAdmin1Capital) {
      attributesBuilder.adm1cap(true)
    }

    feature.population.foreach(pop =>
      attributesBuilder.population(pop)
    )

    feature.extraColumns.get("sociallyRelevant").map(v =>
      attributesBuilder.sociallyRelevant(v.toBoolean)
    )

    feature.extraColumns.get("neighborhoodType").map(v =>
      attributesBuilder.neighborhoodType(NeighborhoodType.findByNameOrNull(v))
    )

    attributesBuilder.urls(displayNames.filter(_.lang =? "link").map(_.name))

    val extraRelations = extraRelationsList.get(geonameId).map(_.split(",").toList.map(_.toLong)).flatten

    case class PolygonRecord(geom: Geometry) {
      val id = new ObjectId()
    }

    // I hate this code, let's please deprecate this codepath
    val polygonOpt = feature.extraColumns.get("geometry").map(polygon => {
      wktReader.read(polygon)
    })

    val polygonRecordOpt = if (feature.featureClass.woeType == YahooWoeType.POI
     && config.revgeoIndexPoints) {
      Some(PolygonRecord(GeoTools.pointToGeometry(lat, lng)))
    } else {
      polygonOpt.map(poly => PolygonRecord(poly))
    }

    // combine flags of duplicate names in the same language
    val finalDisplayNames = displayNames.groupBy(dn => (dn.lang, dn.name)).toList
      .map({case ((lang, name), displayNames) => DisplayName(
        lang = lang,
        name = name,
        flags = displayNames.foldLeft(0)((f, dn) => f | dn.flags))})

    val record = GeocodeRecord(
      _id = geonameId.longId,
      names = Nil,
      cc = feature.countryCode,
      _woeType = feature.featureClass.woeType.getValue,
      lat = lat,
      lng = lng,
      parents = allParents.map(_.longId),
      population = feature.population,
      displayNames = finalDisplayNames,
      boost = boost,
      boundingbox = bbox,
      displayBounds = displayBboxTable.get(geonameId),
      canGeocode = canGeocode,
      slug = slug,
      // hasPoly = polygonExtraEntry.isDefined,
      extraRelations = extraRelations,
      ids = ids.map(_.longId),
      polyId = polygonRecordOpt.map(_.id).getOrElse(GeocodeRecord.dummyOid),
      hasPoly = polygonRecordOpt.isDefined
    )

    polygonRecordOpt.foreach(polygonRecord => {
      polygonLoader.indexPolygon(
        polygonRecord.id,
        polygonRecord.geom,
        "self_point"
      )
    })

    if (attributesSet) {
      record.setAttributes(Some(attributesBuilder.result))
    }

    record
  }

  def parseAdminFile(filename: String, allowBuildings: Boolean = false) {
    parseFromFile(filename, (index: Int, line: String) =>
      GeonamesFeature.parseFromAdminLine(index, line), "features", allowBuildings)
  }

  def parsePostalCodeFile(filename: String) {
    parseFromFile(filename, (index: Int, line: String) =>
      GeonamesFeature.parseFromPostalCodeLine(index, line), "postal codes")
  }

  private def shouldTakeFeature(f: InputFeature, allowBuildings: Boolean): Boolean = {
    f.shouldIndex &&
    !ignoreList.contains(f.featureId) &&
    (!f.featureClass.isBuilding || config.shouldParseBuildings || allowBuildings)
  }

  private def parseFromFile(filename: String,
    lineProcessor: (Int, String) => Option[GeonamesFeature],
    typeName: String,
    allowBuildings: Boolean = false) {

    var processed = 0
    val lines = scala.io.Source.fromFile(new File(filename), "UTF-8").getLines

    val groupSize = 2000
    for {
      (lineGroup, groupIndex) <- lines.grouped(groupSize).zipWithIndex
    } {
      val processed = groupIndex * groupSize
      if (processed % 10000 == 0) {
        logger.info("imported %d %s so far".format(processed, typeName))
      }

      val recordsToInsert = lineGroup.zipWithIndex.flatMap({case (line, index) => {
        val realIndex = groupIndex * groupSize + index
        lineProcessor(realIndex, line).filter(f => shouldTakeFeature(f, allowBuildings)).map(line => {
          Stats.time("parse.line." + typeName) {
            parseFeature(line)
          }
        })
      }}).toList

      Stats.time("parse.insert" + typeName + "." + groupSize) {
        insertGeocodeRecords(recordsToInsert)
      }
    }
  }

  def insertGeocodeRecords(recordsToInsert: List[GeocodeRecord]) {
    store.insert(recordsToInsert)

    val displayNamesToInsert = recordsToInsert.flatMap(r =>
      createNameIndexRecords(r.displayNames, r.featureId, Some(r))
    )
    store.addNameIndexes(displayNamesToInsert)
  }

  var alternateNamesMap = new HashMap[StoredFeatureId, List[AlternateNameEntry]]
  def loadAlternateNames() {
    val altDirs = List(
      new File("data/computed/alternateNames/"),
      new File("data/private/alternateNames/")
    )
    val files: List[String] = List("data/downloaded/alternateNames.txt") ++ altDirs.flatMap(altDir => {
        if (altDir.exists) { altDir.listFiles.toList.map(_.toString) } else { Nil }
    }).sorted

    alternateNamesMap = AlternateNamesReader.readAlternateNamesFiles(files)
  }

  val spaceRe = " +".r
  def fixName(s: String) = spaceRe.replaceAllIn(s, " ").trim

  // TODO(rahul): actually use flags
  def doShorten(cc: String, name: String): List[String] = {
    val shortens = shortensList.getOrElse("*", Nil) ++
      shortensList.getOrElse(cc, Nil)

    val candidates = shortens.flatMap(shorten => {
      val newName = shorten.from.replaceAllIn(name, shorten.to)
      if (newName != name) {
        Some(fixName(newName))
      } else {
        None
      }
    })

    candidates.sortBy(_.size).headOption.toList
  }

  def hackName(
    lang: String,
    name: String,
    cc: String,
    woeType: YahooWoeType
  ): List[String] = {
    // HACK(blackmad): TODO(blackmad): move these to data files
    if (woeType == YahooWoeType.ADMIN1 && cc == "JP" && (lang == "en" || lang == "")) {
      List(name + " Prefecture")
    } else if (woeType == YahooWoeType.TOWN && cc == "TW" && (lang == "en" || lang == "")) {
      List(name + " County")
    // Region Lima -> Lima Region
    } else if (woeType == YahooWoeType.ADMIN1 && cc == "PE" && name.startsWith("Region")) {
      List(name.replace("Region", "").trim + " Region")
    } else {
      Nil
    }
  }

  def isLocalLang(lang: String, cc: String) =
    countryLangMap.getOrElse(cc, Nil).contains(lang)

  def processFeatureName(
    fid: StoredFeatureId,
    cc: String,
    lang: String,
    name: String,
    isPrefName: Boolean = false,
    isShortName: Boolean = false,
    isColloquial: Boolean = false,
    isHistoric: Boolean = false,
    woeType: YahooWoeType): List[DisplayName] = {
    if (lang != "post" && !isNameDeleted(name, fid)) {
      val originalNames = List(name)
      val hackedNames = hackName(lang, name, cc, woeType)
      val (deaccentedNames, allModifiedNames) = rewriteNames(originalNames)
      val shortenedNames = doShorten(cc, name)

      def buildDisplayName(name: String, flags: Int) = {
        DisplayName(lang, name, flags)
      }

      def processNameList(names: List[String], flags: Int): List[DisplayName] = {
        names.map(n => {
          var finalFlags = flags
          if (isLocalLang(lang, cc)) {
            finalFlags |= FeatureNameFlags.LOCAL_LANG.getValue
          }
          if (isHistoric) {
            finalFlags |= FeatureNameFlags.HISTORIC.getValue
          }
          if (isColloquial) {
            finalFlags |= FeatureNameFlags.COLLOQUIAL.getValue
          }

          buildDisplayName(n, finalFlags)
        })
      }

      val originalFlags = {
        val prefFlag = if (isPrefName) {
          FeatureNameFlags.PREFERRED.getValue
        } else {
          0
        }

        val shortFlag = if (isShortName) {
          FeatureNameFlags.SHORT_NAME.getValue
        } else {
          0
        }

        shortFlag | prefFlag
      }

      processNameList(originalNames, originalFlags) ++
      processNameList(shortenedNames, originalFlags | FeatureNameFlags.SHORT_NAME.getValue) ++
      processNameList(deaccentedNames, originalFlags | FeatureNameFlags.DEACCENT.getValue) ++
      processNameList(allModifiedNames, originalFlags | FeatureNameFlags.ALIAS.getValue) ++
      processNameList(hackedNames, originalFlags | FeatureNameFlags.ALIAS.getValue)
    } else {
      Nil
    }
  }

  def parseNameTransforms(): Unit = {
    // geonameid -> lang|prefName|[optional flags]
      val nameTransformsDirs = List(
        new File("data/custom/name-transforms"),
        new File("data/private/name-transforms")
      )
      val files = nameTransformsDirs.flatMap(dir => {
        if (dir.exists) { dir.listFiles } else { Nil }
      })
      files.foreach(file => {
        val lines = scala.io.Source.fromFile(file).getLines
        parseNameTransforms(lines, file.toString)
    })
  }

  private def parseFeatureNameFlags(
    flagsString: Option[String],
    default: List[FeatureNameFlags] = Nil
  ): Int = {
    val flags: List[FeatureNameFlags] = if (flagsString.isEmpty) {
      default
    } else {
      flagsString.getOrElse("").split(",").map(f => FeatureNameFlags.unapply(f).getOrElse(
        throw new Exception("couldn't parse name flag: %s".format(f))
      )).toList
    }

    var flagsMask = 0
    flags.foreach(f => flagsMask = flagsMask | f.getValue())
    flagsMask
  }

  def parseNameTransforms(lines: Iterator[String], filename: String = "n/a"): Unit =  {
    for {
      (line, lineIndex) <- lines.zipWithIndex
      if (!line.startsWith("#") && line.nonEmpty)
      parts = line.split("[\t ]").toList
      idString <- parts.lift(0)
      featureId <- StoredFeatureId.fromHumanReadableString(idString, Some(GeonamesNamespace))
      rest = parts.drop(1).mkString(" ")
      lang <- rest.split("\\|").lift(0)
      name <- rest.split("\\|").lift(1)
      originalFlags = rest.split("\\|").lift(2)
    } {
      val flagsMask = parseFeatureNameFlags(originalFlags, List(FeatureNameFlags.PREFERRED))

      val records = store.getById(featureId).toList
      records match {
        case Nil => logger.error("no match for id %s".format(idString))
        case record :: Nil => {
          val flagsMaskComputed = flagsMask | (if (isLocalLang(lang, record.cc)) {
            FeatureNameFlags.LOCAL_LANG.getValue
          } else {
            0
          })
          val newName = DisplayName(lang, name, flagsMaskComputed)

          // all display names have already been deduped and their flags combined
          // name transform can therefore have at most one display name dupe
          // combine flags with that dupe, if it exists
          var merged = false
          val mergedNames = record.displayNames.map(dn => {
            if (dn.lang =? lang && dn.name =? name) {
              logger.info("merged display name %s with name transform: id %s, lang %s, name %s, flags %d".format(dn, idString, lang, name, flagsMaskComputed))
              merged = true
              DisplayName(dn.lang, dn.name, dn.flags | flagsMaskComputed)
            } else {
              dn
            }
          })

          // repeat merge for names in name index
          if (merged) {
            val normalizedName = NameNormalizer.normalize(name).trim
            val nameRecords = store.getNameIndexByIdLangAndName(featureId, lang, normalizedName).toList
            nameRecords match {
              case Nil => logger.error("display names and name index out of sync for id %s, lang %s, name %s".format(idString, lang, name))
              case nameRecord :: dupes => {
                // dupes can rarely creep into the name index when display names are not exact dupes
                // but their normalized forms are, e.g. "LA", "L.A." both normalize to "la"
                // in this case, use the first name's flags to update all names
                val newFlags = nameRecord.flags | flagsMaskComputed
                store.updateFlagsOnNameIndexByIdLangAndName(featureId, lang, normalizedName, newFlags)
              }
            }
          } else {
            addDisplayNameToNameIndex(newName, featureId, Some(record))
          }

          // if we're trying to put in a new preferred name, kill all the other preferred names in the same language
          val modifiedNames: List[DisplayName] = mergedNames.map(dn => {
            if (dn.lang =? lang &&
                dn.name !=? name &&
                (flagsMaskComputed & FeatureNameFlags.PREFERRED.getValue) != 0
            ) {
              DisplayName(dn.lang, dn.name, dn.flags & ~FeatureNameFlags.PREFERRED.getValue())
            } else {
              dn
            }
          })

          val newNames = modifiedNames ++
            (if (merged) {
              Nil
            } else {
              List(newName)
            })
          store.setRecordNames(featureId, newNames)
        }
        case list => logger.error("multiple matches for id %s -- %s".format(idString, list))
      }
    }
  }
}
