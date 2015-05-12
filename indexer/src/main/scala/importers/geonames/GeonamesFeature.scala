// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.YahooWoeType
import com.foursquare.twofishes.util.{GeonamesNamespace, GeonamesZip, StoredFeatureId}
import com.foursquare.twofishes.util.Helpers._
import com.weiglewilczek.slf4s.Logging


object GeonamesFeatureColumns extends Enumeration {
   type GeonamesFeatureColumns = Value
   val GEONAMEID, PLACE_NAME, NAME, ASCIINAME, ALTERNATENAMES, LATITUDE, LONGITUDE,
      FEATURE_CLASS, FEATURE_CODE, COUNTRY_CODE, CC2, ADMIN1_CODE, ADMIN2_CODE, ADMIN3_CODE,
      ADMIN4_CODE, ADMIN1_NAME, ADMIN2_NAME, ADMIN3_NAME, POPULATION, ELEVATION, GTOPO30, TIMEZONE,
      MODIFICATION_DATE, ACCURACY, EXTRA = Value
}

import GeonamesFeatureColumns._

object GeonamesFeature extends Logging {
  val adminColumns = List(
    GEONAMEID,
    NAME,
    ASCIINAME,
    ALTERNATENAMES,
    LATITUDE,
    LONGITUDE,
    FEATURE_CLASS,
    FEATURE_CODE,
    COUNTRY_CODE,
    CC2,
    ADMIN1_CODE,
    ADMIN2_CODE,
    ADMIN3_CODE,
    ADMIN4_CODE,
    POPULATION,
    ELEVATION,
    GTOPO30,
    TIMEZONE,
    MODIFICATION_DATE
  )

  val postalCodeColumns = List(
    COUNTRY_CODE,
    NAME, // really, postal code
    PLACE_NAME,
    ADMIN1_NAME,
    ADMIN1_CODE,
    ADMIN2_NAME,
    ADMIN2_CODE,
    ADMIN3_NAME,
    ADMIN3_CODE,
    LATITUDE,
    LONGITUDE,
    ACCURACY
  )

  def parseFromPostalCodeLine(index: Int, line: String): Option[GeonamesFeature] = {
    // HACK
    val newLine = if (line.split("\t").size == 11) {
      (line.split("\t").toList ++ List("n/a")).mkString("\t")
    } else {
      line
    }

    def cb(in: Map[GeonamesFeatureColumns.Value, String]) = {
      try {
        Some(in ++ List(
          (GeonamesFeatureColumns.FEATURE_CLASS -> "Z"),
          (GeonamesFeatureColumns.GEONAMEID ->
            (new GeonamesZip(in(COUNTRY_CODE), in(NAME))).humanReadableString),
           // hack to ensure US postal codes are always in prefix index
          (GeonamesFeatureColumns.POPULATION ->
            (if (in(COUNTRY_CODE) =? "US") {
              1000
            } else {
              0
            }).toString)
        ))
      } catch {
        case e: Exception =>
          logger.error("Exception when handling '%s': %s".format(in, e))
          None
      }
    }

    parseLine(index, newLine, postalCodeColumns, cb)
  }

  def parseFromAdminLine(index: Int, line: String): Option[GeonamesFeature] = {
    def cb(in: Map[GeonamesFeatureColumns.Value, String]) = Some(in)
    parseLine(index, line, adminColumns, cb)
  }

  def parseLine(index: Int, line: String,
      columns: List[GeonamesFeatureColumns.Value],
      modifyCallback: Map[GeonamesFeatureColumns.Value, String] => Option[Map[GeonamesFeatureColumns.Value, String]]
      ): Option[GeonamesFeature] = {
    val parts = line.split("\t")
    if (parts.size < columns.size) {
      logger.error("line %d has too few columns. Has %d, needs %d (%s)".format(
        index, parts.size, columns.size, parts.mkString(",")))
      None
    } else {
      modifyCallback(columns.zip(parts).toMap) match {
        case None =>
          logger.error("CALLBACK ELIDED LINE: %s".format(line))
          None

        case Some(colMapVal) =>
          var colMap = colMapVal

          if (parts.size > columns.size) {
            colMap += (EXTRA -> parts.drop(columns.size).mkString("\t"))
          }

          val feature = new GeonamesFeature(colMap)
          if (feature.isValid) {
            Some(feature)
          } else {
            logger.error("INVALID: %s".format(line))
            None
          }
      }
    }
  }
}

object AdminLevel extends Enumeration {
  type AdminLevel = Value
  val COUNTRY, ADM1, ADM2, ADM3, ADM4, OTHER = Value
}

import AdminLevel._

trait FeatureClass {
  def woeType: YahooWoeType

  def isAdmin1 = woeType == YahooWoeType.ADMIN1
  def isAdmin2 = woeType == YahooWoeType.ADMIN2
  def isAdmin3 = woeType == YahooWoeType.ADMIN3

  def isAdmin1Capital: Boolean = false

  def isBuilding: Boolean = woeType == YahooWoeType.POI
}

// http://www.geonames.org/export/codes.html
// I added Z for zipcodes. It's missing from the geonames hierarchy.
class GeonamesFeatureClass(featureClass: Option[String], featureCode: Option[String]) extends FeatureClass {
  override def isBuilding = featureClass.exists(_ == "S")
  def isPopulatedPlace = featureClass.exists(_ == "P")
  def isPostalCode = featureClass.exists(_ == "Z")
  def isSuburb = featureCode.exists(_.contains("PPLX"))
  def isCity = featureCode.exists(_.contains("PPL")) ||
    (isPopulatedPlace && !isSuburb)
  def isIsraeliSettlement = featureCode.exists(_.contains("STLMT"))
  def isIsland = featureCode.exists(_.contains("ISL"))
  def isCountry = featureCode.exists(_.contains("PCL"))
  def isAdmin = adminLevel != OTHER
  def isAirport = featureCode.exists(_.startsWith("AIR"))
  override def isAdmin1Capital = featureCode.exists(_ == "PPLA")
  val stupidCodes = List(
    "RGNE", // economic region
    "PPLQ", // abandoned (historical) place
    "WLL", "WLLS", "WLLQ" // wells!
  )
  def isContinent = featureCode.exists(_ == "CONT")
  def isStupid = featureCode.exists(fc => stupidCodes.contains(fc))
  def isPark = featureCode.exists(_ == "PRK")
  def isAdmin4 = featureCode.exists(_ == "ADM4")

  def woeType: YahooWoeType = {
    if (isIsland) {
      YahooWoeType.ISLAND
    } else if (isSuburb) {
      YahooWoeType.SUBURB
    } else if (isContinent) {
      YahooWoeType.CONTINENT
    } else if (isPark) {
      YahooWoeType.PARK
    } else if (isBuilding) {
      YahooWoeType.POI
    } else if (isCountry) {
      YahooWoeType.COUNTRY
    } else if (isPostalCode) {
      YahooWoeType.POSTAL_CODE
    } else if (isCity) {
      YahooWoeType.TOWN
    } else if (isAirport) {
      YahooWoeType.AIRPORT
    } else {
      featureCode.map(_ match {
        case "ADM1" => YahooWoeType.ADMIN1
        case "ADM2" => YahooWoeType.ADMIN2
        case "ADM3" => YahooWoeType.ADMIN3
        case "ADM4" | "ADMD" => YahooWoeType.ADMIN3
        case _ => YahooWoeType.UNKNOWN
      }).getOrElse(YahooWoeType.UNKNOWN)
    }
  }

  def adminLevel: AdminLevel.Value = {
    if (isCountry) {
      COUNTRY
    } else {
      featureCode.map(_ match {
        case "ADM1" => ADM1
        case "ADM2" => ADM2
        case "ADM3" => ADM3
        case "ADM4" => ADM4
        case _ => OTHER
      }).getOrElse(OTHER)
    }
  }
}

trait InputFeature {
  def isValid: Boolean = true
  def shouldIndex: Boolean = true

  def featureClass: FeatureClass

  def adminCode: Option[String] = None

  def getAdminCode(level: AdminLevel.Value): Option[String] = None

  def adminId: Option[String] = None

  def parents: List[String] = Nil

  def population: Option[Int] = None
  def latitude: Double
  def longitude: Double
  def countryCode: String

  def name: String
  def asciiname: Option[String] = None

  def extraColumns: Map[String,String] = Map.empty

  def featureId: StoredFeatureId
}

class BasicFeatureClass(
  val woeType: YahooWoeType
) extends FeatureClass

class BasicFeature(
  val latitude: Double,
  val longitude: Double,
  val countryCode: String,
  val name: String,
  val featureId: StoredFeatureId,
  woeType: YahooWoeType
) extends InputFeature {
  override val featureClass = new BasicFeatureClass(woeType)
}

class GeonamesFeature(values: Map[GeonamesFeatureColumns.Value, String]) extends InputFeature {
  override def isValid = {
    values.contains(NAME) &&
    values.contains(LATITUDE) &&
    values.contains(LONGITUDE) &&
    TryO { values(LATITUDE).toDouble }.isDefined &&
    TryO { values(LONGITUDE).toDouble }.isDefined
  }

  override def shouldIndex = {
    !featureClass.isStupid &&
    !(featureClass.woeType =? YahooWoeType.ADMIN3 &&
      featureClass.adminLevel =? AdminLevel.OTHER &&
      name.startsWith("City of") &&
      countryCode =? "US"
    ) &&
    !(name.contains(", Stadt") && countryCode =? "DE")
  }

  override val featureClass = new GeonamesFeatureClass(values.get(FEATURE_CLASS), values.get(FEATURE_CODE))

  override def adminCode = getAdminCode(this.featureClass.adminLevel)

  override def getAdminCode(level: AdminLevel.Value): Option[String] = {
    level match {
      case COUNTRY => values.get(COUNTRY_CODE)
      case ADM1 => values.get(ADMIN1_CODE)
      case ADM2 => values.get(ADMIN2_CODE)
      case ADM3 => values.get(ADMIN3_CODE)
      case ADM4 => values.get(ADMIN4_CODE)
      case _ => None
    }
  }

  def makeAdminId(level: AdminLevel.Value): Option[String] = {
    if (getAdminCode(level).exists(_.nonEmpty)) {
      Some(
        AdminLevel.values.toList.filter(_ <= level).flatMap(l => getAdminCode(l)).mkString(".")
      )
    } else {
      None
    }
  }

  override def adminId: Option[String] = {
    if (featureClass.isAdmin) {
      makeAdminId(featureClass.adminLevel)
    } else {
        None
    }
  }

  override def parents: List[String] = {
    AdminLevel.values.filter(_ < featureClass.adminLevel).flatMap(l =>
      makeAdminId(l)
    ).toList.filterNot(_.endsWith(".00"))
  }

  override def population: Option[Int] = flatTryO {values.get(POPULATION).map(_.toInt)}
  override def latitude: Double = values.get(LATITUDE).map(_.toDouble).get
  override def longitude: Double = values.get(LONGITUDE).map(_.toDouble).get
  override def countryCode: String = if (featureClass.isIsraeliSettlement) {
    "IL"
  } else {
    values.get(COUNTRY_CODE).getOrElse("XX")
  }
  override def name: String = values.getOrElse(NAME, "no name")
  override def asciiname: Option[String] = values.get(ASCIINAME)
  def place: String = values.getOrElse(PLACE_NAME, "no name")

  override val extraColumns: Map[String,String] = values.getOrElse(EXTRA, "").split("\t").flatMap(p => {
    if (p.nonEmpty) {
      val parts = p.split(";")
      if (parts.size == 2) {
        Some(parts(0) -> parts(1))
      } else {
        None
      }
    } else {
      None
    }
  }).toMap

  def geonameid: Option[String] = values.get(GEONAMEID)

  def alternateNames: List[String] =
    values.get(ALTERNATENAMES).toList.flatMap(_.split(",").toList)

  def allNames: List[String] = {
    var names = List(name)
    if (featureClass.isCountry) {
      names ::= countryCode
    }

    names ++= alternateNames
    names
  }

  override val featureId = geonameid.flatMap(id => {
    StoredFeatureId.fromHumanReadableString(id, defaultNamespace = Some(GeonamesNamespace))
  }).get
}
