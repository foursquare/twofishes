// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.twofishes.util

import com.foursquare.twofishes.util.Lists.Implicits._
import com.ibm.icu.util.ULocale
import scala.io.Source

case class CountryInfo(
  iso2: String,
  iso3: String,
  fips: String,
  englishName: String,
  languages: List[String],
  geonameid: Int
)

object CountryInfo {
  import CountryInfoFields._

  val englishNameOverrides = Map(
    "NL" -> "The Netherlands",
    "PN" -> "Pitcairn Islands"
  )

  val countryInfos = Source.fromFile("./data/downloaded/countryInfo.txt").getLines.filterNot(l => l.startsWith("#") || l.isEmpty).toList.flatMap(l => {
    val parts = l.split("\t")
    try {
      Some(CountryInfo(
        iso2=parts(ISO2.id),
        iso3=parts(ISO3.id),
        fips=parts(FIPS.id),
        englishName=englishNameOverrides.get(parts(ISO2.id)).getOrElse(parts(ENGLISH_NAME.id)),
        languages=parts(LANGUAGES.id).split(",").toList,
        geonameid=parts(GEONAMEID.id).toInt
      ))
    } catch {
      case e: Exception => None
    }
  })
}

object CountryCodes {
  val countryMap = CountryInfo.countryInfos.map(i => (i.iso2, i.englishName)).toMap ++ Map(
    "TA" -> "Tristan da Cunha",
    "AC" -> "Ascension Island",
    "DG" -> "Diego Garcia",
    "IC" -> "Canary Islands",
    "CP" -> "Clipperton Island",
    "EA" -> "Ceuta and Melilla"
  )

  val nameToCountryMap = countryMap.map(_.swap)

  val countryLangMap = CountryInfo.countryInfos.map(i => (i.iso2, i.languages)).toMap

  val codeConversions = Map(
    "FX" -> "FR", // "metropolitan france"
    "KO-" -> "XK", // Kosovo
    "CYN" -> "CY", // Northern Cyprus
    "AP" -> "XX",   // Officially reserved (Asia/Pacific or African Regional Industrial Property Organization)
    "NQ" -> "AQ", // Merged into Antarctica (AQ, ATA, 010)
    "JT" -> "UM", // Merged into United States Minor Outlying Islands (UM, UMI, 581)
    "MI" -> "UM", // Merged into United States Minor Outlying Islands (UM, UMI, 581)
    "WK" -> "UM", // Merged into United States Minor Outlying Islands (UM, UMI, 581)
    "PU" -> "UM", // Merged into United States Minor Outlying Islands (UM, UMI, 581)
    "CT" -> "KI", //Merged into Kiribati (KI, KIR, 296)
    "PZ" -> "PA" //  Merged into Panama (PA, PAN, 591)
  )

  def getNameByCode(code: String): Option[String] = {
    val preCode = codeConversions.get(code).getOrElse(code)
    countryMap.get(preCode)
  }

  def getISO3ByCode(code: String): Option[String] = {
    val preCode = codeConversions.get(code).getOrElse(code)
    new ULocale("en", preCode).getISO3Country match {
      case "" => None
      case str => Some(str)
    }
  }

  def getISO2ByCode(code: String): Option[String] = {
    val preCode = codeConversions.get(code).getOrElse(code)
    new ULocale("en", preCode).getCountry match {
      case "" => None
      case str => Some(str)
    }
  }

  def getLangs(code: String): List[String] = {
    countryLangMap.getOrElse(getISO2ByCode(code).getOrElse("XX"), Nil).map(l =>
      l.split("-")(0))
  }
  // This only stores lang part, drops locale part
  val bestLang = countryLangMap.keys.flatMap(cc => getLangs(cc).headOption.map(lang => (cc, lang))).toMap

  def isLocalLanguageForCountry(cc: String, lang: String): Boolean = {
    getLangs(cc).has(lang)
  }
}
