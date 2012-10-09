// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes._
import com.foursquare.twofishes.Helpers._
import com.foursquare.twofishes.Implicits._
import java.io.File
import scala.collection.mutable.HashMap

object GeonamesParser {
  val geonameIdNamespace = "geonameid"
  val geonameAdminIdNamespace = "gadminid"

  var config: GeonamesImporterConfig = null

  var countryLangMap = new HashMap[String, List[String]]()

  def parseCountryInfo() {
    val fileSource = scala.io.Source.fromFile(new File("data/downloaded/countryInfo.txt"))
    val lines = fileSource.getLines.filterNot(_.startsWith("#"))
    lines.foreach(l => {
      val parts = l.split("\t")
      val cc = parts(0)
      val langs = parts(15).split(",").map(l => l.split("-")(0)).toList
      countryLangMap += (cc -> langs)
    })
  }

  def main(args: Array[String]) {
    val store = new MongoGeocodeStorageService()
    val parser = new GeonamesParser(store)
    config = new GeonamesImporterConfig(args)



    if (!config.parseWorld) {
      parser.parseAdminFile(
        "data/downloaded/%s.txt".format(config.parseCountry))

      if (config.importPostalCodes) {
        parser.parsePostalCodeFile(
        "data/downloaded/zip/%s.txt".format(config.parseCountry),
        true)
      }
    } else {
      parser.parseAdminFile(
        "data/downloaded/allCountries.txt")
      if (config.importPostalCodes) {
        parser.parsePostalCodeFile(
          "data/downloaded/zip/allCountries.txt", false)
      }
    }

    new File("data/supplemental/").listFiles.foreach(f => {
      println("parsing supplemental file: %s".format(f))
      parser.parseAdminFile(f.toString, allowBuildings=true)
    })

    if (config.importAlternateNames) {
      parser.parseAlternateNamesFile(
        "data/downloaded/alternateNames.txt")
    }

    parser.parsePreferredNames()

    if (config.importBoundingBoxes) {
      new File(config.boundingBoxDirectory).listFiles.toList.sorted.foreach(f => {
        println("parsing bounding box file: %s".format(f))
        new BoundingBoxTsvImporter(store).parse(f.toString)
      })
    }

    new OutputHFile(config.hfileBasePath).process()

  }
}

import GeonamesParser._

class GeonamesParser(store: GeocodeStorageWriteService) {
  object logger {
    def error(s: String) { println("**ERROR** " + s)}
    def info(s: String) { println(s)}
  }

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

  def addDisplayNameToNameIndex(dn: DisplayName, fid: StoredFeatureId, record: GeocodeRecord) = {
    val name = NameNormalizer.normalize(dn.name)
    val nameIndex = NameIndex(name, fid.toString,
      record.population.getOrElse(0) + record.boost.getOrElse(0),
      record._woeType, dn.flags, dn.lang, dn._id)
    store.addNameIndex(nameIndex)
  }

  def rewriteNames(allNames: List[String]): (List[String], List[String]) = {
    val deleteModifiedNames: List[String] = allNames.flatMap(doDelete)

    val deaccentedNames = allNames.map(NameNormalizer.deaccent).filterNot(n =>
      allNames.contains(n))

    val rewrittenNames = doRewrites(allNames).filterNot(n =>
      allNames.contains(n))

    (deaccentedNames, deleteModifiedNames ++ rewrittenNames)
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

    var displayNames = List(
      DisplayName("en", feature.name, FeatureNameFlags.PREFERRED.getValue)
    )

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
      displayNames ::= DisplayName("en", n, FeatureNameFlags.ALIAS.getValue)
    )

    // Build parents
    val extraParents: List[String] = feature.extraColumns.get("parents").toList.flatMap(_.split(",").toList)
    val parents: List[String] = feature.parents.map(p => StoredFeatureId(geonameAdminIdNamespace, p))
    val allParents = parents ++ extraParents

    val boost: Option[Int] = feature.geonameid.flatMap(gid => {
      boostTable.get(gid).headOption.flatMap(boost =>
        tryo { boost.toInt }
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
    })

    var lat = feature.latitude
    var lng = feature.longitude

    feature.geonameid.foreach(gid => {
      val latlngs = moveTable.get(gid)
      if (latlngs.size > 0) {
        lat = latlngs(0).toDouble
        lng = latlngs(1).toDouble
      }
    })

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
      boundingbox = bbox
    )

    store.insert(record)

    displayNames.foreach(n =>
      addDisplayNameToNameIndex(n, geonameId.get, record)
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

  def parseAlternateNamesFile(filename: String) {
    val lines = scala.io.Source.fromFile(new File(filename)).getLines
    lines.zipWithIndex.foreach({case (line, index) => {
      if (index % 10000 == 0) {
        logger.info("imported %d alternateNames so far".format(index))
      }

      val parts = line.split("\t").toList
      if (parts.size < 4) {
          logger.error("line %d didn't have 5 parts: %s -- %s".format(index, line, parts.mkString(",")))
        } else {
          val geonameid = parts(1)
          val lang = parts(2)
          val name = parts(3)
          val isPrefName = parts.lift(4).exists(_ == "1")
          val isShortName = parts.lift(5).exists(_ == "1")

          if (lang != "post") {
            val originalNames = List(name)
            val (deaccentedNames, allModifiedNames) = rewriteNames(originalNames)
            val fid = StoredFeatureId(geonameIdNamespace, geonameid)

            def buildDisplayName(name: String, flags: Int) = {
              DisplayName(lang, name, flags)
            }

            store.getById(fid).toList.headOption.foreach(record => {
              def processNameList(names: List[String], flags: Int) = {
                names.foreach(n => {
                  var finalFlags = flags
                  if (countryLangMap.getOrElse(record.cc, Nil).contains(lang)) {
                    finalFlags &= FeatureNameFlags.LOCAL_LANG.getValue
                  }
                  val dn = buildDisplayName(name, finalFlags)
                  addDisplayNameToNameIndex(dn, fid, record)
                  store.addNameToRecord(dn, fid)
                })
              }

              val originalFlags = if (isPrefName) {
                FeatureNameFlags.PREFERRED.getValue
              } else { 0 }
              
              processNameList(originalNames, originalFlags)
              processNameList(deaccentedNames, originalFlags | FeatureNameFlags.DEACCENT.getValue)
              processNameList(allModifiedNames, originalFlags | FeatureNameFlags.ALIAS.getValue)
            })
          }
        }
    }})
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
