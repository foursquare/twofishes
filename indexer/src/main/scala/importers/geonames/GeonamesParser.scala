// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes._
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.{GeonamesId, GeonamesNamespace, Helpers, NameNormalizer, StoredFeatureId}
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import java.io.{File, FileWriter, PrintStream}
import org.bson.types.ObjectId
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable.{HashMap, HashSet}
import scala.io.Source
import scalaj.collection.Implicits._
import com.weiglewilczek.slf4s.Logging

// TODO
// stop using string representations of "a:b" featureids everywhere, PLEASE
// please, I'm begging you, be more disciplined about featureids in the parser

object GeonamesParser {
  var config: GeonamesImporterConfig = null

  var countryLangMap = new HashMap[String, List[String]]()
  var countryNameMap = new HashMap[String, String]()
  var adminIdMap = new HashMap[String, String]()

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

    if (config.reloadData) {
      loadIntoMongo()
    }
    writeIndexes()
  }

  def writeIndex(args: Array[String]) {
    config = GeonamesImporterConfigParser.parse(args)
    writeIndexes()
  }

  def writeIndexes() {
    val writer = new FileWriter(new File(config.hfileBasePath, "provider_mapping.txt"))
    config.providerMapping.foreach({case (k, v) => {
      writer.write("%s\t%s\n".format(k, v))
    }})
    writer.close()

    val outputter = new OutputIndexes(config.hfileBasePath, config.outputPrefixIndex, GeonamesParser.slugIndexer.slugEntryMap, config.outputRevgeo)
    outputter.buildIndexes()
  }

  def loadIntoMongo() {
    val parser = new GeonamesParser(store, slugIndexer, config.providerMapping.toMap)

    parseCountryInfo()

    if (config.importAlternateNames) {
      Helpers.duration("readAlternateNamesFile") {
        parser.loadAlternateNames()
      }
    }

    if (!config.parseWorld) {
      val countries = config.parseCountry.split(",")
      countries.foreach(f => {
        parser.logger.info("Parsing %s".format(f))
        parseAdminInfoFile("data/computed/adminCodes-%s.txt".format(f))
        parser.parseAdminFile(
          "data/downloaded/%s.txt".format(f))

        if (config.importPostalCodes) {
          parser.parsePostalCodeFile("data/downloaded/zip/%s.txt".format(f))
        }
      })
    } else {
      parseAdminInfoFile("data/computed/adminCodes.txt")
      parser.parseAdminFile(
        "data/downloaded/allCountries.txt")
      if (config.importPostalCodes) {
        parser.parsePostalCodeFile("data/downloaded/zip/allCountries.txt")
      }
    }

    val supplementalDirs = List(
      new File("data/supplemental/"),
      new File("data/private/features")
    )
    supplementalDirs.foreach(supplementalDir =>
      if (supplementalDir.exists) {
        supplementalDir.listFiles.foreach(f => {
          println("parsing supplemental file: %s".format(f))
          parser.parseAdminFile(f.toString, allowBuildings=true)
        })
      }
    )

    parser.parsePreferredNames()

    if (config.buildMissingSlugs) {
      println("building missing slugs")
      slugIndexer.buildMissingSlugs()
      slugIndexer.writeMissingSlugs(store)
    }

    PolygonLoader.load(store, GeonamesNamespace)
  }
}

import GeonamesParser._
class GeonamesParser(
  store: GeocodeStorageWriteService,
  slugIndexer: SlugIndexer,
  providerMapping: Map[String, Int]
) extends Logging {
  lazy val hierarchyTable = HierarchyParser.parseHierarchy(List(
    "data/downloaded/hierarchy.txt",
    "data/private/hierarchy.txt",
    "data/custom/hierarchy.txt"
  ))

  // token -> alt tokens
  lazy val rewriteTable = new TsvHelperFileParser("data/custom/rewrites.txt",
    "data/private/rewrites.txt")
  // tokenlist
  lazy val deletesList: List[String] = scala.io.Source.fromFile(new File("data/custom/deletes.txt"))
    .getLines.toList.filterNot(_.startsWith("#"))
  // geonameid -> boost value
  lazy val boostTable = new GeoIdTsvHelperFileParser(GeonamesNamespace, "data/custom/boosts.txt",
    "data/private/boosts.txt")

  // geonameid -> alias
  val aliasFiles: List[String] = List("data/custom/aliases.txt", "data/private/aliases.txt")
  lazy val aliasTable = new GeoIdTsvHelperFileParser(GeonamesNamespace, aliasFiles:_*)

  // geonameid --> new center
  lazy val moveTable = new GeoIdTsvHelperFileParser(GeonamesNamespace, "data/custom/moves.txt")

  // geonameid -> name to be deleted
  lazy val nameDeleteTable = new GeoIdTsvHelperFileParser(GeonamesNamespace, "data/custom/name-deletes.txt")
  // geonameid -> name to be demoted
  lazy val nameDemoteTable = new GeoIdTsvHelperFileParser(GeonamesNamespace, "data/custom/name-demotes.txt")
  // list of geoids (geonameid:XXX) to skip indexing
  lazy val ignoreList: List[StoredFeatureId] = scala.io.Source.fromFile(new File("data/custom/ignores.txt"))
    .getLines.toList.filterNot(_.startsWith("#")).map(l => GeonamesId(l.toLong))

  lazy val concordanceMap = new GeoIdTsvHelperFileParser(GeonamesNamespace, "data/computed/concordances.txt")

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

  val helperTables = List(rewriteTable, boostTable, aliasTable)

  def logUnusedHelperEntries {
    helperTables.flatMap(_.logUnused).foreach(line => logger.error(line))
  }

  val wkbWriter = new WKBWriter()
  val wktReader = new WKTReader()

  def doRewrites(names: List[String]): List[String] = {
    val nameSet = new scala.collection.mutable.HashSet[String]()
    rewriteTable.gidMap.foreach({case(from, toList) => {
      names.foreach(name => {
        toList.values.foreach(to => {
          nameSet += name.replaceAll(from, to)
        })
      })
    }})
    nameSet.toList
  }

  def doDelete(name: String): List[String] = {
    deletesList.flatMap(delete => {
      val newName = name.replaceAll(delete + "\\b", "").split(" ").filterNot(_.isEmpty).mkString(" ")
      if (newName != name) {
        Some(newName)
      } else {
        None
      }
    })
  }

  def doDeletes(names: List[String]) = {
    val nameSet = new scala.collection.mutable.HashSet() ++ names.toSet
    // val newNameSet = new scala.collection.mutable.HashSet() ++ names.toSet
    deletesList.foreach(delete => {
      nameSet.foreach(name => {
        nameSet += name.replace(delete, "").split(" ").filterNot(_.isEmpty).mkString(" ")
      })
    })
    nameSet.toList.filterNot(_.isEmpty)
  }

  def addDisplayNameToNameIndex(dn: DisplayName, fid: StoredFeatureId, record: Option[GeocodeRecord]) {
    val name = NameNormalizer.normalize(dn.name)

    if (nameDeleteTable.get(fid).exists(_ == dn.name)) {
      return
    }

    var flags = dn.flags
    if (nameDemoteTable.get(fid).exists(_ == dn.name)) {
      flags | FeatureNameFlags.LOW_QUALITY.getValue
    }

    val pop: Int =
      record.flatMap(_.population).getOrElse(0) + record.flatMap(_.boost).getOrElse(0)
    val woeType: Int =
      record.map(_._woeType).getOrElse(0)
    val nameIndex = NameIndex(name, fid.longId, pop, woeType, dn.flags, dn.lang, dn._id)
    store.addNameIndex(nameIndex)
  }

  def rewriteNames(names: List[String]): (List[String], List[String]) = {
    val deleteModifiedNames: List[String] = names.flatMap(doDelete)

    val deaccentedNames = names.map(NameNormalizer.deaccent).filterNot(n =>
      names.contains(n))

    val rewrittenNames = doRewrites((names ++ deleteModifiedNames)).filterNot(n =>
      names.contains(n))

    (deaccentedNames, (deleteModifiedNames ++ rewrittenNames).distinct)
  }

  def parseFeature(feature: GeonamesFeature): GeocodeRecord = {
    // Build ids
    val geonameId = feature.geonameid.flatMap(id => {
      StoredFeatureId.fromHumanReadableString(id, defaultNamespace = Some(GeonamesNamespace))
    }).get

    val ids: List[StoredFeatureId] = List(geonameId) ++
      concordanceMap.get(geonameId).flatMap(id =>
        if (id.contains(":")) {
          StoredFeatureId.fromHumanReadableString(id)
        } else { None }
      )

    val preferredEnglishAltName = alternateNamesMap.getOrElse(geonameId, Nil).find(altName =>
      altName.lang == "en" && altName.isPrefName
    )

    val hasPreferredEnglishAltName = preferredEnglishAltName.isDefined

    // If we don't have an altname with lang=en marked as preferred, then assume that
    // the primary name geonames gives us is english preferred
    var displayNames: List[DisplayName] = Nil

    if (!preferredEnglishAltName.exists(_.name =? feature.name)) {
      displayNames ++= processFeatureName(
        feature.countryCode, "en", feature.name,
        isPrefName = !hasPreferredEnglishAltName,
        isShortName = false
      )
    }

    if (feature.featureClass.woeType == YahooWoeType.COUNTRY) {
      countryNameMap.get(feature.countryCode).foreach(name =>
        displayNames ::=
          DisplayName("en", name, FeatureNameFlags.PREFERRED.getValue() | FeatureNameFlags.COLLOQUIAL.getValue())
      )
    }

    feature.asciiname.foreach(asciiname => {
      if (feature.name != asciiname && asciiname.nonEmpty) {
        displayNames ::=
          DisplayName("en", asciiname,
            FeatureNameFlags.DEACCENT.getValue)
      }
    })

    if (feature.featureClass.woeType.getValue == YahooWoeType.COUNTRY.getValue) {
      displayNames ::= DisplayName("abbr", feature.countryCode, 0)
    }

    // Build names
    val aliasedNames: List[String] = aliasTable.get(geonameId)

    // val allNames = feature.allNames ++ aliasedNames
    // val allModifiedNames = rewriteNames(allNames)
    // val normalizedNames = (allNames ++ allModifiedNames).map(n => NameNormalizer.normalize(n))
    // normalizedNames.toSet.toList.filterNot(_.isEmpty)

    aliasedNames.foreach(n =>
      displayNames ::= DisplayName("en", n, FeatureNameFlags.ALT_NAME.getValue)
    )

    val englishName = preferredEnglishAltName.getOrElse(feature.name)
    val alternateNames = alternateNamesMap.getOrElse(geonameId, Nil).filterNot(n =>
      (n.name == englishName) && (n.lang != "en")
    )
    displayNames ++= alternateNames.flatMap(altName => {
      processFeatureName(
        feature.countryCode, altName.lang, altName.name, altName.isPrefName, altName.isShortName)
    })

    // the admincode is the internal geonames admin code, but is very often the
    // same short name for the admin area that is actually used in the country
    def isAllDigits(x: String) = x forall Character.isDigit

    if (feature.featureClass.isAdmin1 || feature.featureClass.isAdmin2) {
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

    val polygonExtraEntry: Option[Geometry] = feature.extraColumns.get("geometry").map(polygon => {
      wktReader.read(polygon)
    })

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

    val record = GeocodeRecord(
      _id = geonameId.longId,
      ids = ids.map(_.longId),
      names = Nil,
      cc = feature.countryCode,
      _woeType = feature.featureClass.woeType.getValue,
      lat = lat,
      lng = lng,
      parents = allParents.map(_.longId),
      population = feature.population,
      displayNames = displayNames,
      boost = boost,
      boundingbox = bbox,
      displayBounds = displayBboxTable.get(geonameId),
      canGeocode = canGeocode,
      slug = slug,
      polygon = polygonExtraEntry.map(wkbWriter.write),
      hasPoly = polygonExtraEntry.map(e => true)
    )

    if (attributesSet) {
      record.setAttributes(Some(attributesBuilder.result))
    }

    store.insert(record)

    displayNames.foreach(n =>
      addDisplayNameToNameIndex(n, geonameId, Some(record))
    )

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

  private def parseFromFile(filename: String,
    lineProcessor: (Int, String) => Option[GeonamesFeature],
    typeName: String,
    allowBuildings: Boolean = false) {

    var processed = 0
    val numThreads = 5
    val workers = 0.until(numThreads).toList.map(offset => {
      val lines = scala.io.Source.fromFile(new File(filename), "UTF-8").getLines

      lines.zipWithIndex.foreach({case (line, index) => {
        if (index % numThreads == offset) {
          if (processed % 10000 == 0) {
            logger.info("imported %d %s so far".format(processed, typeName))
          }
          processed += 1
          val feature = lineProcessor(index, line)
          feature.foreach(f => {
            if (
              !f.featureClass.isStupid &&
              !(f.name.contains(", Stadt") && f.countryCode == "DE") &&
              !f.geonameid.exists(ignoreList.contains) &&
              (!f.featureClass.isBuilding || config.shouldParseBuildings || allowBuildings)) {
              parseFeature(f)
            }
          })
        }
      }})
    })
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

  def processFeatureName(
    cc: String,
    lang: String,
    name: String,
    isPrefName: Boolean,
    isShortName: Boolean): List[DisplayName] = {
    if (lang != "post") {
      val originalNames = List(name)
      val (deaccentedNames, allModifiedNames) = rewriteNames(originalNames)

      def buildDisplayName(name: String, flags: Int) = {
        DisplayName(lang, name, flags)
      }

      def isLocalLang(lang: String) =
        countryLangMap.getOrElse(cc, Nil).contains(lang)

      def processNameList(names: List[String], flags: Int): List[DisplayName] = {
        names.map(n => {
          var finalFlags = flags
          if (isLocalLang(lang)) {
            finalFlags |= FeatureNameFlags.LOCAL_LANG.getValue
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
      processNameList(deaccentedNames, originalFlags | FeatureNameFlags.DEACCENT.getValue) ++
      processNameList(allModifiedNames, originalFlags | FeatureNameFlags.ALIAS.getValue)
    } else {
      Nil
    }
  }

  def parsePreferredNames() {
    // geonameid -> lang|prefName
    val filename = "data/custom/names.txt"
    val lines = scala.io.Source.fromFile(new File(filename)).getLines

    lines.foreach(line => {
      val parts = line.split("[\t ]").toList

      for {
        gid <- parts.lift(0)
        val rest = parts.drop(1).mkString(" ")
        lang <- rest.split("\\|").lift(0)
        name <- rest.split("\\|").lift(1)
      } {
        val records = store.getById(GeonamesId(gid.toLong)).toList
        records match {
          case Nil => logger.error("no match for id %s".format(gid))
          case record :: Nil => {
            var foundName = false
            val modifiedNames: List[DisplayName] = record.displayNames.map(dn => {
              if (dn.lang == lang) {
                if (dn.name == name) {
                  foundName = true
                  val newName = DisplayName(dn.lang, dn.name,
                    (dn.flags | FeatureNameFlags.PREFERRED.getValue()) &  ~FeatureNameFlags.ALIAS.getValue())

                  newName
                } else {
                  val newName = DisplayName(dn.lang, dn.name, dn.flags & ~FeatureNameFlags.PREFERRED.getValue())
                  newName
                }
              } else {
                dn
              }
            })

            val newNames = modifiedNames ++ (
              if (foundName) { Nil } else {
                List(DisplayName(lang, name, FeatureNameFlags.PREFERRED.getValue))
              }
            )

            store.setRecordNames(GeonamesId(gid.toLong), newNames)
          }
          case list => logger.error("multiple matches for id %s -- %s".format(gid, list))
        }
      }
    })
  }
}
