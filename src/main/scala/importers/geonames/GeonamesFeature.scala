// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder.importers.geonames

import com.foursquare.geocoder.Helpers._
import com.foursquare.geocoder.{Helpers, LogHelper, YahooWoeTypes}

object GeonamesFeatureColumns extends Enumeration {
   type GeonamesFeatureColumns = Value
   val GEONAMEID, PLACE_NAME, NAME, ASCIINAME, ALTERNATENAMES, LATITUDE, LONGITUDE,
      FEATURE_CLASS, FEATURE_CODE, COUNTRY_CODE, CC2, ADMIN1_CODE, ADMIN2_CODE, ADMIN3_CODE,
      ADMIN4_CODE, ADMIN1_NAME, ADMIN2_NAME, ADMIN3_NAME, POPULATION, ELEVATION, GTOPO30, TIMEZONE,
      MODIFICATION_DATE, ACCURACY = Value
}

import GeonamesFeatureColumns._

object GeonamesFeature extends LogHelper {
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

    parseLine(index, newLine, postalCodeColumns)
  }

  def parseFromAdminLine(index: Int, line: String): Option[GeonamesFeature] = {
    parseLine(index, line, adminColumns)
  }

  def parseLine(index: Int, line: String, columns: List[GeonamesFeatureColumns.Value]): Option[GeonamesFeature] = {
    val parts = line.split("\t")
    if (parts.size != columns.size) {
      logger.error("line %d has the wrong number of columns. Has %d, needs %d (%s)".format(
        index, parts.size, columns.size, parts.mkString(",")))
      None
    } else {
      val colMap = columns.zip(parts).toMap
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

object AdminLevel extends Enumeration {
  type AdminLevel = Value
  val COUNTRY, ADM1, ADM2, ADM3, ADM4, OTHER = Value
}

import AdminLevel._

// http://www.geonames.org/export/codes.html
// I added Z for zipcodes. It's missing from the geonames hierarchy.
class GeonamesFeatureClass(featureClass: Option[String], featureCode: Option[String]) {
  def isBuilding = featureClass.exists(_ == "S")
  def isPostalCode = featureClass.exists(_ == "Z")
  def isCity = featureCode.exists(_.contains("PPL"))
  def isCountry = featureCode.exists(_.contains("PCL"))
  def isAdmin = adminLevel != OTHER
  def isAirport = featureCode.exists(_ == "AIRP")

  def woeType: Int = {
    if (isCountry) {
      YahooWoeTypes.COUNTRY
    } else if (isPostalCode) {
      YahooWoeTypes.POSTAL_CODE
    } else if (isCity) {
      YahooWoeTypes.TOWN
    } else if (isAirport) {
      YahooWoeTypes.AIRPORT
    } else {
      featureCode.map(_ match {
        case "ADM1" => YahooWoeTypes.ADMIN1
        case "ADM2" => YahooWoeTypes.ADMIN2
        case "ADM3" => YahooWoeTypes.ADMIN3
        case _ => 0
      }).getOrElse(0)
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

class GeonamesFeature(values: Map[GeonamesFeatureColumns.Value, String]) {
  def isValid = {
    values.contains(NAME) &&
    values.contains(LATITUDE) && 
    values.contains(LONGITUDE) &&
    tryo { values(LATITUDE).toDouble }.isDefined &&
    tryo { values(LONGITUDE).toDouble }.isDefined
  }
  
  val featureClass = new GeonamesFeatureClass(values.get(FEATURE_CLASS), values.get(FEATURE_CODE))

  def adminCode(level: AdminLevel.Value): Option[String] = {
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
    if (adminCode(level).exists(_.nonEmpty)) {
      Some(
        AdminLevel.values.filter(_ <= level).flatMap(l => adminCode(l)).mkString("-")
      )
    } else {
      None
    }
  }

  def adminId: Option[String] = {
    if (featureClass.isAdmin) {
      makeAdminId(featureClass.adminLevel)
    } else {
        None
    }
  }

  def parents: List[String] = {
    AdminLevel.values.filter(_ < featureClass.adminLevel).flatMap(l =>
      makeAdminId(l)
    ).toList
  }

  def population: Option[Int] = flattryo {values.get(POPULATION).map(_.toInt)}
  def latitude: Double = values.get(LATITUDE).map(_.toDouble).get
  def longitude: Double = values.get(LONGITUDE).map(_.toDouble).get
  def countryCode: String = values.get(COUNTRY_CODE).getOrElse("XX")
  def name: String = values.getOrElse(NAME, "no name")
  def place: String = values.getOrElse(PLACE_NAME, "no name")

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
}