// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder.geonames

import com.foursquare.geocoder._
import com.foursquare.geocoder.Implicits._
import java.io.File

// TODO
// displayNames
// rewrites
// (n/s/e/w)
// boosts
// aliases
// prefnames
// zipcode parser

object GeonamesParser {
  def run() {
    if (!GeonamesImporterConfig.parseWorld) {
      new GeonamesParser(new MongoGeocodeStorageService()).parseFromFile(
        "data/downloaded/%s.txt".format(GeonamesImporterConfig.parseCountry))

      if (GeonamesImporterConfig.importAlternateNames) {
        new GeonamesParser(new MongoGeocodeStorageService()).parseAlternateNamesFile(
          "data/downloaded/alternateNames.txt")
      }
    }
  }
}

class GeonamesParser(store: GeocodeStorageService) extends LogHelper {
  val geonameIdNamespace = "geonameid"
  val geonameAdminIdNamespace = "gadminid"

  def parseFeature(feature: GeonamesFeature) {
    // Build ids
    val adminId = feature.adminId.map(id => FeatureId(geonameAdminIdNamespace, id))
    val geonameId = feature.geonameid.map(id => FeatureId(geonameIdNamespace, id))
    val ids: List[FeatureId] = List(adminId, geonameId).flatMap(a => a)

    // Build names
    val normalizedNames = feature.allNames.map(n => NameNormalizer.normalize(n))
    val deaccentedNames = feature.allNames.map(n => NameNormalizer.deaccent(n))
    val names = normalizedNames ++ deaccentedNames.toSet.toList

     // Build parents
     val parents = feature.parents.map(p => FeatureId(geonameAdminIdNamespace, p))

    val record = GeocodeRecord(
      ids = ids,
      names = names,
      cc = feature.countryCode,
      woeType = Some(feature.featureClass.woeType),
      lat = feature.latitude,
      lng = feature.longitude,
      parents = parents,
      population = feature.population,
      displayNames = List(DisplayName("en", feature.name, false))
    )

    store.insert(record)
  }

  def parseFromFile(filename: String) {
    val lines = scala.io.Source.fromFile(new File(filename)).getLines
    lines.zipWithIndex.foreach({case (line, index) => {
      if (index % 1000 == 0) {
        logger.info("imported %d features so far".format(index))
      }
      val feature = GeonamesFeature.parseFromAdminLine(index, line)
      feature.foreach(f => {
        if (!f.featureClass.isBuilding || GeonamesImporterConfig.shouldParseBuildings) {
          parseFeature(f)
        }
      })
    }})
  }

  def parseAlternateNamesFile(filename: String) {
    val lines = scala.io.Source.fromFile(new File(filename)).getLines
    lines.zipWithIndex.foreach({case (line, index) => {
      if (index % 1000 == 0) {
        logger.info("imported %d alternateNames so far".format(index))
      }

      val parts = line.split("\t").toList
      if (parts.size < 4) {
          logger.error("line %d didn't have 5 parts: %s -- %s".format(index, line, parts.mkString(",")))
        } else {
          val geonameid = parts(1)
          val lang = parts(2)
          val altName = parts(3)
          val isPrefName = parts.lift(4).exists(_ == "1")
          val isShortName = parts.lift(5).exists(_ == "1")

          val name = DisplayName(lang, altName, isPrefName)
          store.addNameToRecord(name, FeatureId(geonameIdNamespace, geonameid))
        }
    }})
  }
}