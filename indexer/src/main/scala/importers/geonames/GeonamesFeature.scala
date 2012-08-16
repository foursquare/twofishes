// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes.Helpers._
import com.foursquare.twofishes.{Helpers, LogHelper, YahooWoeType}
import org.bson.types.ObjectId

object GeonamesFeatureColumns extends Enumeration {
   type GeonamesFeatureColumns = Value
   val GEONAMEID, PLACE_NAME, NAME, ASCIINAME, ALTERNATENAMES, LATITUDE, LONGITUDE,
      FEATURE_CLASS, FEATURE_CODE, COUNTRY_CODE, CC2, ADMIN1_CODE, ADMIN2_CODE, ADMIN3_CODE,
      ADMIN4_CODE, ADMIN1_NAME, ADMIN2_NAME, ADMIN3_NAME, POPULATION, ELEVATION, GTOPO30, TIMEZONE,
      MODIFICATION_DATE, ACCURACY, EXTRA = Value
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

    def cb(in: Map[GeonamesFeatureColumns.Value, String]) = {
      in ++ List(
        (GeonamesFeatureColumns.FEATURE_CLASS -> "Z"),
        (GeonamesFeatureColumns.GEONAMEID -> "geonamezip:%s".format(new ObjectId()))
      )
    }

    parseLine(index, newLine, postalCodeColumns, cb)
  }

  def parseFromAdminLine(index: Int, line: String): Option[GeonamesFeature] = {
    def cb(in: Map[GeonamesFeatureColumns.Value, String]) = in
    parseLine(index, line, adminColumns, cb)
  }

  def parseLine(index: Int, line: String,
      columns: List[GeonamesFeatureColumns.Value],
      modifyCallback: Map[GeonamesFeatureColumns.Value, String] => Map[GeonamesFeatureColumns.Value, String]
      ): Option[GeonamesFeature] = {
    val parts = line.split("\t")
    if (parts.size < columns.size) {
      logger.error("line %d has too few columns. Has %d, needs %d (%s)".format(
        index, parts.size, columns.size, parts.mkString(",")))
      None
    } else {
      var colMap = modifyCallback(columns.zip(parts).toMap)

      if (parts.size > columns.size) {
        colMap += (EXTRA -> columns.drop(parts.size).mkString("\t"))
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
  def isStupid = featureCode.exists(_ == "RGNE")

  def woeType: YahooWoeType = {
    if (isCountry) {
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

  def extraColumns: Map[String,String] = values.getOrElse(EXTRA, "").split("\t").flatMap(p => {
    if (p.nonEmpty) {
      val parts = p.split(";")
      Some(parts(0) -> parts(1))
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
}