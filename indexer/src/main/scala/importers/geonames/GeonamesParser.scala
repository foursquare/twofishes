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

// TODO
// stop using string representations of "a:b" featureids everywhere, PLEASE

object GeonamesParser {
  val geonameIdNamespace = "geonameid"

  var config: GeonamesImporterConfig = null

  var countryLangMap = new HashMap[String, List[String]]() 
  var countryNameMap = new HashMap[String, String]() 
  var adminIdMap = new HashMap[String, String]() 

  var hasPolygonList = new HashSet[String]

  lazy val naturalEarthPopulatedPlacesMap: Map[Int, SimpleFeature] = {
    new ShapefileIterator("data/downloaded/ne_10m_populated_places_simple.shp").flatMap(f => {
      f.propMap.get("geonameid").map(id => {
        (id.toDouble.toInt, f)
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
    config = new GeonamesImporterConfig(args)
    loadIntoMongo()
    writeHFileOutput()
  }

  def writeIndex(args: Array[String]) {
    config = new GeonamesImporterConfig(args)
    writeHFileOutput()
  }

  def writeHFileOutput() {
    val outputter = new OutputHFile(config.hfileBasePath, config.outputPrefixIndex, slugIndexer.slugEntryMap)
    outputter.process()
    if (config.outputRevgeo) {
      outputter.buildRevGeoIndex()
    }
  }

  def loadIntoMongo() {
    val parser = new GeonamesParser(store, slugIndexer)

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
          parser.parsePostalCodeFile(
          "data/downloaded/zip/%s.txt".format(f),
          true)
        }
      })
    } else {
      parseAdminInfoFile("data/computed/adminCodes.txt")
      parser.parseAdminFile(
        "data/downloaded/allCountries.txt")
      if (config.importPostalCodes) {
        parser.parsePostalCodeFile(
          "data/downloaded/zip/allCountries.txt", false)
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
  }
}

import GeonamesParser._
class GeonamesParser(store: GeocodeStorageWriteService, slugIndexer: SlugIndexer) extends SimplePrintLogger {
  def objectIdFromLong(n: Long) = {
    val bytes = BigInt(n).toByteArray
    val arr = bytes.reverse.padTo(12, 0: Byte).reverse
    new ObjectId(arr)
  }

  lazy val hierarchyTable = HierarchyParser.parseHierarchy(List(
    "data/downloaded/hierarchy.txt",
    "data/private/hierarchy.txt",
    "data/custom/hierarchy.txt"
  ))

  // token -> alt tokens
  lazy val rewriteTable = new TsvHelperFileParser("data/custom/rewrites.txt",
    "data/private/rewrites.txt")
  // tokenlist
  lazy val deletesList: List[String] = scala.io.Source.fromFile(new File("data/custom/deletes.txt")).getLines.toList
  // geonameid -> boost value
  lazy val boostTable = new TsvHelperFileParser("data/custom/boosts.txt",
    "data/private/boosts.txt")
  // geonameid -> alias
  lazy val aliasTable = new TsvHelperFileParser("data/custom/aliases.txt",
    "data/private/aliases.txt")
  // geonameid --> new center
  lazy val moveTable = new TsvHelperFileParser("data/custom/moves.txt")
  // geonameid -> polygon
  lazy val polygonTable: Map[String, Geometry] = PolygonLoader.load()
  // geonameid -> name to be deleted
  lazy val nameDeleteTable = new TsvHelperFileParser("data/custom/name-deletes.txt")

  val bboxDirs = List(
    new File("data/computed/bboxes/"),
    new File("data/private/bboxes/")
  )
  val bboxFiles = bboxDirs.flatMap(bboxDir => {
    if (bboxDir.exists) { bboxDir.listFiles.toList } else { Nil }
  }).sorted
  lazy val bboxTable = BoundingBoxTsvImporter.parse(bboxFiles)

  val helperTables = List(rewriteTable, boostTable, aliasTable)

  def logUnusedHelperEntries {
    helperTables.foreach(_.logUnused)
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

    if (nameDeleteTable.get(fid.toString).exists(_ == dn.name)) {
      return
    }

    val pop: Int = 
      record.flatMap(_.population).getOrElse(0) + record.flatMap(_.boost).getOrElse(0)
    val woeType: Int = 
      record.map(_._woeType).getOrElse(0)
    val nameIndex = NameIndex(name, fid.toString,
      pop, woeType, dn.flags, dn.lang, dn._id)
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
    val geonameId = feature.geonameid.map(id => {
      if (id.contains(":")) {
        val parts = id.split(":")
        StoredFeatureId(parts(0), parts(1))
      } else {
        StoredFeatureId(geonameIdNamespace, id)
      }
    })

    val ids: List[StoredFeatureId] = List(geonameId).flatMap(a => a)

    var displayNames: List[DisplayName] = processFeatureName(
      feature.countryCode, "en", feature.name, true, false)

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
            FeatureNameFlags.DEACCENT.getValue | FeatureNameFlags.PREFERRED.getValue)
      }
    })

    if (feature.featureClass.woeType.getValue == YahooWoeType.COUNTRY.getValue) {
      displayNames ::= DisplayName("abbr", feature.countryCode, 0)
    }

    // Build names
    val aliasedNames: List[String] = feature.geonameid.toList.flatMap(gid => {
      aliasTable.get(gid)
    })

    // val allNames = feature.allNames ++ aliasedNames
    // val allModifiedNames = rewriteNames(allNames)
    // val normalizedNames = (allNames ++ allModifiedNames).map(n => NameNormalizer.normalize(n))
    // normalizedNames.toSet.toList.filterNot(_.isEmpty)

    aliasedNames.foreach(n =>
      displayNames ::= DisplayName("en", n, FeatureNameFlags.ALT_NAME.getValue)
    )

    displayNames ++= alternateNamesMap.getOrElse(geonameId.get.toString, Nil).flatMap(altName => {
      processFeatureName(
        feature.countryCode, altName.lang, altName.name, altName.isPrefName, altName.isShortName)
    })

    def fixParent(p: String): Option[String] = {
      adminIdMap.get(p) orElse {
        //println("missing admin lookup for %s".format(p))
        None
      }
    }

    // Build parents
    val extraParents: List[String] = feature.extraColumns.get("parents").toList.flatMap(_.split(",").toList)
    val parents: List[String] =
      feature.parents.flatMap(fixParent).map(p => StoredFeatureId(geonameIdNamespace, p))
    var allParents: List[String] = extraParents ++ parents
    val hierarchyParents = hierarchyTable.getOrElse(feature.geonameid.getOrElse(""), Nil).filterNot(p =>
      parents.has(p)).map(pid => "%s:%s".format(geonameIdNamespace, pid))
    allParents = allParents ++ hierarchyParents

    val boost: Option[Int] =
      feature.extraColumns.get("boost").map(_.toInt) orElse
      feature.geonameid.flatMap(gid => {
        boostTable.get(gid).headOption.flatMap(boost =>
          TryO { boost.toInt }
        )
      }) 

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
    }) orElse bboxTable.get(geonameId.get.toString)

    var lat = feature.latitude
    var lng = feature.longitude

    feature.geonameid.foreach(gid => {
      val latlngs = moveTable.get(gid)
      if (latlngs.size > 0) {
        lat = latlngs(0).toDouble
        lng = latlngs(1).toDouble
      }
    })

    val canGeocode = feature.extraColumns.get("canGeocode").map(_.toInt).getOrElse(1) > 0

    val polygonExtraEntry: Option[Geometry] = feature.extraColumns.get("geometry").map(polygon => {
      wktReader.read(polygon)
    })
    val polygonTableEntry: Option[Geometry] = polygonTable.get(geonameId.get.toString)
    val polygon: Option[Array[Byte]] = (polygonTableEntry orElse polygonExtraEntry).map(geom =>
      wkbWriter.write(geom))

    val slug: Option[String] = geonameId.flatMap(gid => {
      slugIndexer.idToSlugMap.get(gid.toString)
    })
    if (slug.isEmpty &&
      List(YahooWoeType.TOWN, YahooWoeType.SUBURB, YahooWoeType.COUNTRY, YahooWoeType.ADMIN1, YahooWoeType.ADMIN2).has(feature.featureClass.woeType)) {
      geonameId.foreach(gid => slugIndexer.missingSlugList.add(gid.toString))
    }

    val geonameIntId = TryO { feature.geonameid.getOrElse("-1").toInt } 

    var attributesSet = false
    lazy val attributes = {
      attributesSet = true
      new GeocodeFeatureAttributes()
    }

    geonameIntId.flatMap(naturalEarthPopulatedPlacesMap.get).map(sfeature => {
      sfeature.propMap.get("adm0cap").foreach(v => 
        attributes.setAdm0cap(v.toDouble.toInt == 1)
      )
      sfeature.propMap.get("scalerank").foreach(v => 
        attributes.setScalerank(v.toInt)
      )
      sfeature.propMap.get("natscale").foreach(v => 
        attributes.setNatscale(v.toInt)
      )
      sfeature.propMap.get("labelrank").foreach(v => 
        attributes.setLabelrank(v.toInt)
      )
    })

    if (feature.featureClass.isAdmin1Capital) {
      attributes.setAdm1cap(true)
    }

    feature.population.foreach(pop =>
      attributes.setPopulation(pop)
    )

    feature.extraColumns.get("sociallyRelevant").map(v => 
      attributes.setSociallyRelevant(v.toBoolean)
    )


    val objectId = (
      for {
        fid <- geonameId
        if fid.namespace == geonameIdNamespace
        idInt <- Helpers.TryO(fid.id.toInt)
      } yield {
        // let's call geonames data
        objectIdFromLong((1 << 32) + idInt)
      }
    ).getOrElse(new ObjectId())

    val record = GeocodeRecord(
      _id = objectId,
      ids = ids,
      names = Nil,
      cc = feature.countryCode,
      _woeType = feature.featureClass.woeType.getValue,
      lat = lat,
      lng = lng,
      parents = allParents,
      population = feature.population,
      displayNames = displayNames,
      boost = boost,
      boundingbox = bbox,
      canGeocode = canGeocode,
      slug = slug,
      polygon = polygon,
      hasPoly = polygon.map(p => true)
    )

    if (attributesSet) {
      record.setAttributes(Some(attributes))
    }

    store.insert(record)

    displayNames.foreach(n =>
      addDisplayNameToNameIndex(n, geonameId.get, Some(record))
    )

    record
  }

  def parseAdminFile(filename: String, allowBuildings: Boolean = false) {
    parseFromFile(filename, (index: Int, line: String) => 
      GeonamesFeature.parseFromAdminLine(index, line), "features", allowBuildings)
  }

  def parsePostalCodeFile(filename: String, countryFile: Boolean) {
    parseFromFile(filename, (index: Int, line: String) => 
      GeonamesFeature.parseFromPostalCodeLine(index, line), "postal codes")
  }

  private def parseFromFile(filename: String,
    lineProcessor: (Int, String) => Option[GeonamesFeature],
    typeName: String,
    allowBuildings: Boolean = false) {
    val lines = scala.io.Source.fromFile(new File(filename), "UTF-8").getLines
    lines.zipWithIndex.foreach({case (line, index) => {
      if (index % 10000 == 0) {
        logger.info("imported %d %s so far".format(index, typeName))
      }
      val feature = lineProcessor(index, line)
      feature.foreach(f => {
        if (
          !f.featureClass.isStupid &&
          !(f.name.contains(", Stadt") && f.countryCode == "DE") &&
          (!f.featureClass.isBuilding || config.shouldParseBuildings || allowBuildings)) {
          parseFeature(f)
        }
      })
    }})
  }

  var alternateNamesMap = new HashMap[String, List[AlternateNameEntry]]
  def loadAlternateNames() {
    alternateNamesMap = AlternateNamesReader.readAlternateNamesFile(
      "data/downloaded/alternateNames.txt")
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

      val originalFlags = if (isPrefName) {
        FeatureNameFlags.PREFERRED.getValue
      } else { 0 }
      
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
      val parts = line.split("\t").toList
      for {
        gid <- parts.lift(0)
        lang <- parts.lift(1).flatMap(_.split("\\|").lift(0))
        name <- parts.lift(1).flatMap(_.split("\\|").lift(1))
      } {
        val records = store.getById(StoredFeatureId(geonameIdNamespace, gid)).toList
        records match {
          case Nil => logger.error("no match for id %s".format(gid))
          case record :: Nil => {
            var foundName = false
            val modifiedNames: List[DisplayName] = record.displayNames.map(dn => {
              if (dn.lang == lang) {
                if (dn.name == name) {
                  foundName = true
                  DisplayName(dn.lang, dn.name, dn.flags | FeatureNameFlags.PREFERRED.getValue())
                } else {
                  DisplayName(dn.lang, dn.name, dn.flags ^ FeatureNameFlags.PREFERRED.getValue())
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

            store.setRecordNames(StoredFeatureId(geonameIdNamespace, gid), newNames)
          }
        }
      }
    })
  }
}
