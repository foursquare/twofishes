// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes.util.{NameNormalizer, SlugBuilder, NameUtils, Helpers}
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.Implicits._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import org.geotools.geometry.jts.JTSFactoryFinder
import scala.collection.JavaConversions._
import java.io.File
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scalaj.collection.Implicits._

// TODO
// stop using string representations of "a:b" featureids everywhere, PLEASE

object GeonamesParser {
  val geonameIdNamespace = "geonameid"
  val geonameAdminIdNamespace = "gadminid"

  var config: GeonamesImporterConfig = null

  var countryLangMap = new HashMap[String, List[String]]() 
  var countryNameMap = new HashMap[String, String]() 

  case class SlugEntry(
    id: String,
    score: Int,
    deprecated: Boolean = false,
    permanent: Boolean = false
  ) {
    override def toString(): String = {
      "%s\t%s\t%s".format(id, score, deprecated)
    }
  }

  val idToSlugMap = new HashMap[String, String]
  val slugEntryMap = new HashMap[String, SlugEntry]
  var missingSlugList = new HashSet[String]
  var hasPolygonList = new HashSet[String]

  def readSlugs() {
    // step 1 -- load existing slugs into ... memory?
    val files = List(
      new File("data/computed/slugs.txt"),
      new File("data/private/slugs.txt")
    )
    files.foreach(file => 
      if (file.exists) {
        val fileSource = scala.io.Source.fromFile(file)
        val lines = fileSource.getLines.toList.filterNot(_.startsWith("#"))
        lines.map(l => {
          val parts = l.split("\t")
          val slug = parts(0)
          val id = parts(1)
          val score = parts(2).toInt
          slugEntryMap(slug) = SlugEntry(id, score, deprecated = false, permanent = true)
          idToSlugMap(id) = slug
        })
      }
    )
    println("read %d slugs".format(slugEntryMap.size))
  }

  // TODO: not in love with this talking directly to mongo, please fix
  import com.novus.salat._
  import com.novus.salat.global._
  import com.novus.salat.annotations._
  import com.novus.salat.dao._
  import com.mongodb.casbah.Imports._
  val parentMap = new HashMap[String, Option[GeocodeFeature]]

  def findFeature(fid: String): Option[GeocodeServingFeature] = {
    val ret = MongoGeocodeDAO.findOne(MongoDBObject("ids" -> fid))
      .map(_.toGeocodeServingFeature)
    if (ret.isEmpty) {
      println("couldn't find %s".format(fid))
    }
    ret
  }

  def findParent(fid: String): Option[GeocodeFeature] = {
    parentMap.getOrElseUpdate(fid, findFeature(fid).map(_.feature))
  }

  def calculateSlugScore(f: GeocodeServingFeature): Int = {
    f.scoringFeatures.boost + f.scoringFeatures.population
  }

  def matchSlugs(id: String, servingFeature: GeocodeServingFeature, possibleSlugs: List[String]): Boolean = {
    // println("trying to generate a slug for %s".format(id))
    possibleSlugs.foreach(slug => {
      // println("possible slug: %s".format(slug))
      val existingSlug = slugEntryMap.get(slug)
      val score = calculateSlugScore(servingFeature)
      existingSlug match {
        case Some(existing) => {
          if (!existing.permanent && score > existing.score) {
            val evictedId = existingSlug.get.id
            // println("evicting %s and recursing".format(evictedId))
            slugEntryMap(slug) = SlugEntry(id, score, deprecated = false, permanent = false)
            buildSlug(evictedId)
            return true
          }
        }
        case _ => {
          // println("picking %s".format(slug))
          slugEntryMap(slug) = SlugEntry(id, score, deprecated = false, permanent = false)
          return true
        }
      } 
    })
    // println("failed to find any slug")
    return false
  }

  def buildSlug(id: String) {
    for {
      servingFeature <- findFeature(id)
      if (servingFeature.scoringFeatures.population > 0 || servingFeature.scoringFeatures.boost > 0)
    } {
      val parents = servingFeature.scoringFeatures.parents.asScala.flatMap(
        p => findParent(p)).toList
      var possibleSlugs = SlugBuilder.makePossibleSlugs(servingFeature.feature, parents)

      // if a city is bigger than 2 million people, we'll attempt to use the bare city name as the slug
      if (servingFeature.scoringFeatures.population > 2000000) {
        possibleSlugs = NameUtils.bestName(servingFeature.feature, Some("en"), false).toList.map(n => SlugBuilder.normalize(n.name)) ++ possibleSlugs

      }

      if (!matchSlugs(id, servingFeature, possibleSlugs) && possibleSlugs.nonEmpty) {
        var extraDigit = 1
        var slugFound = false
        while (!slugFound) {
          slugFound = matchSlugs(id, servingFeature, possibleSlugs.map(s => "%s-%d".format(s, extraDigit)))
          extraDigit += 1
        }
      }
    }
  }

  def buildMissingSlugs() {
    println("building missing slugs for %d fetures".format(missingSlugList.size))
    // step 2 -- compute slugs for records without
    for {
      (id, index) <- missingSlugList.zipWithIndex
    } {
      if (index % 10000 == 0) {
        println("built %d of %d slugs".format(index, missingSlugList.size))
      }
      buildSlug(id)
    }

    // step 3 -- write new slug file
    println("writing new slug map for %d features".format(slugEntryMap.size))
    val p = new java.io.PrintWriter(new File("data/computed/slugs.txt"))
    slugEntryMap.keys.toList.sorted.foreach(slug => 
     p.println("%s\t%s".format(slug, slugEntryMap(slug)))
    )
    p.close()
  }

  def writeMissingSlugs(store: GeocodeStorageWriteService) {
    for {
      (id, index) <- missingSlugList.zipWithIndex
      slug <- idToSlugMap.get(id)
      val parts = id.split(":")
      fid1 <- parts.lift(0)
      fid2 <- parts.lift(1)
    } {
      if (index % 10000 == 0) {
        println("flushed %d of %d slug to mongo".format(index, missingSlugList.size))
      }
      val fid = StoredFeatureId(fid1, fid2)
      store.addSlugToRecord(fid, slug)
    }
  }

  def parseCountryInfo() {
    val fileSource = scala.io.Source.fromFile(new File("data/downloaded/countryInfo.txt"))
    val lines = fileSource.getLines.filterNot(_.startsWith("#"))
    lines.foreach(l => {
      val parts = l.split("\t")
      val cc = parts(0)
      val englishName = parts(4)
      val langs = parts(15).split(",").map(l => l.split("-")(0)).toList
      countryLangMap += (cc -> langs)
      countryNameMap += (cc -> englishName)
    })
  }

  def main(args: Array[String]) {
    val store = new MongoGeocodeStorageService()
    val parser = new GeonamesParser(store)
    config = new GeonamesImporterConfig(args)

    Helpers.duration("readSlugs") { readSlugs() }
    parseCountryInfo()

    if (config.importAlternateNames) {
      Helpers.duration("readAlternateNamesFile") {
        parser.readAlternateNamesFile(
          "data/downloaded/alternateNames.txt")
      }
    }

    if (!config.parseWorld) {
      val countries = config.parseCountry.split(",")
      countries.foreach(f => {
        parser.logger.info("Parsing %s".format(f))
        parser.parseAdminFile(
          "data/downloaded/%s.txt".format(f))

        if (config.importPostalCodes) {
          parser.parsePostalCodeFile(
          "data/downloaded/zip/%s.txt".format(f),
          true)
        }
      })
    } else {
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
      buildMissingSlugs()
      writeMissingSlugs(store)  
    }

    new OutputHFile(config.hfileBasePath, config.outputPrefixIndex).process()
  }
}

import GeonamesParser._

class GeonamesParser(store: GeocodeStorageWriteService) {
  object logger {
    def error(s: String) { println("**ERROR** " + s)}
    def info(s: String) { println(s)}
  }

  lazy val hierarchyTable = HierarchyParser.parseHierarchy(List(
    "data/downloaded/hierarchy.txt",
    "data/private/hierarchy.txt",
    "data/custom/hierarchy.txt"
  ))

  // token -> alt tokens
  val rewriteTable = new TsvHelperFileParser("data/custom/rewrites.txt",
    "data/private/rewrites.txt")
  // tokenlist
  val deletesList: List[String] = scala.io.Source.fromFile(new File("data/custom/deletes.txt")).getLines.toList
  // geonameid -> boost value
  val boostTable = new TsvHelperFileParser("data/custom/boosts.txt",
    "data/private/boosts.txt")
  // geonameid -> alias
  val aliasTable = new TsvHelperFileParser("data/custom/aliases.txt",
    "data/private/aliases.txt")
  // geonameid --> new center
  val moveTable = new TsvHelperFileParser("data/custom/moves.txt")
  // geonameid -> polygon
  val polygonDirs = List(
    new File("data/computed/polygons"),
    new File("data/private/polygons")
  )
  val polygonFiles = polygonDirs.flatMap(polygonDir => {
    if (polygonDir.exists) { polygonDir.listFiles.toList } else { Nil }
  }).sorted
  val polygonTable: Map[String, String] = polygonFiles.flatMap(f => {
    scala.io.Source.fromFile(f).getLines.filterNot(_.startsWith("#")).toList.map(l => {
      val parts = l.split("\t")
      (parts(0) -> parts(1))
    })  
  }).toMap
  // geonameid -> name to be deleted
  val nameDeleteTable = new TsvHelperFileParser("data/custom/name-deletes.txt")

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
    val adminId = feature.adminId.map(id => StoredFeatureId(geonameAdminIdNamespace, id))
    val geonameId = feature.geonameid.map(id => {
      if (id.contains(":")) {
        val parts = id.split(":")
        StoredFeatureId(parts(0), parts(1))
      } else {
        StoredFeatureId(geonameIdNamespace, id)
      }
    })

    val ids: List[StoredFeatureId] = List(adminId, geonameId).flatMap(a => a)

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

    // Build parents
    val extraParents: List[String] = feature.extraColumns.get("parents").toList.flatMap(_.split(",").toList)
    val parents: List[String] = feature.parents.map(p => StoredFeatureId(geonameAdminIdNamespace, p))
    var allParents: List[String] = extraParents ++ parents
    val hierarchyParents = hierarchyTable.getOrElse(feature.geonameid.getOrElse(""), Nil).filterNot(p =>
      parents.has(p)).map(pid => "%s:%s".format(geonameIdNamespace, pid))
    allParents = allParents ++ hierarchyParents

    val boost: Option[Int] = feature.geonameid.flatMap(gid => {
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

    val polygonExtraEntry: Option[String] = feature.extraColumns.get("geometry")
    val polygonTableEntry: Option[String] = polygonTable.get(geonameId.get.toString)
    val polygon: Option[Array[Byte]] = for {
      gid <- geonameId
      polygon <- polygonTableEntry orElse polygonExtraEntry
    } yield {
      val wktReader = new WKTReader()
      val wkbWriter = new WKBWriter()
      val geom = wktReader.read(polygon)
      wkbWriter.write(geom)
    }

    val slug: Option[String] = geonameId.flatMap(gid => {
      idToSlugMap.get(gid.toString)
    })
    if (slug.isEmpty &&
      List(YahooWoeType.TOWN, YahooWoeType.SUBURB, YahooWoeType.COUNTRY, YahooWoeType.ADMIN1, YahooWoeType.ADMIN2).has(feature.featureClass.woeType)) {
      geonameId.foreach(gid => missingSlugList.add(gid.toString))
    }

    val record = GeocodeRecord(
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

  case class AlternateNameEntry(
    nameId: String,
    lang: String,
    name: String,
    isPrefName: Boolean,
    isShortName: Boolean
  )
  val alternateNamesMap = new HashMap[String, List[AlternateNameEntry]]
  def readAlternateNamesFile(filename: String) {
    val lines = scala.io.Source.fromFile(new File(filename)).getLines
    lines.zipWithIndex.foreach({case (line, index) => {
      if (index % 100000 == 0) {
        logger.info("imported %d alternateNames so far".format(index))
      }

      val parts = line.split("\t").toList
      if (parts.size < 4) {
          logger.error("line %d didn't have 5 parts: %s -- %s".format(index, line, parts.mkString(",")))
        } else {
          val nameid = parts(0)
          val geonameid = parts(1)
          val lang = parts(2)
          val name = parts(3)
          val isPrefName = parts.lift(4).exists(_ == "1")
          val isShortName = parts.lift(5).exists(_ == "1")

          val fid = StoredFeatureId(geonameIdNamespace, geonameid)
          val names = alternateNamesMap.getOrElseUpdate(fid.toString, Nil)
          alternateNamesMap(fid.toString) = AlternateNameEntry(
            nameId = nameid,
            name = name,
            lang = lang,
            isPrefName = isPrefName,
            isShortName = isShortName
          ) :: names
      }
    }})
  }

  // def parseAlternateNamesFile(filename: String) {
  //   val lines = scala.io.Source.fromFile(new File(filename)).getLines
  //   lines.zipWithIndex.foreach({case (line, index) => {
  //     if (index % 10000 == 0) {
  //       logger.info("imported %d alternateNames so far".format(index))
  //     }

  //     parseAlternateNamesLine(line, index)
  //   }})
  // }

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

  // def parseAlternateNamesLine(line: String, index: Int) {
  //   val parts = line.split("\t").toList
  //   if (parts.size < 4) {
  //       logger.error("line %d didn't have 5 parts: %s -- %s".format(index, line, parts.mkString(",")))
  //     } else {
  //       val geonameid = parts(1)
  //       val lang = parts(2)
  //       val name = parts(3)
  //       val isPrefName = parts.lift(4).exists(_ == "1")
  //       val isShortName = parts.lift(5).exists(_ == "1")

  //       val fid = StoredFeatureId(geonameIdNamespace, geonameid)
  //       val record = store.getById(fid).toList.headOption
  //       val names = processFeatureName(
  //         record.map(_.cc).getOrElse("XX"), lang, name, isPrefName, isShortName)
  //       names.foreach(dn => {
  //         addDisplayNameToNameIndex(dn, fid, record)
  //         store.addNameToRecord(dn, fid)
  //       })
  //   }
  // }

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
