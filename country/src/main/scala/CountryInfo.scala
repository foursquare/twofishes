// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.twofishes.country

import scala.io.Source
import scala.io.BufferedSource

object CountryInfoFields extends Enumeration {
  val ISO2,
      ISO3,
      ISO_NUMERIC,
      FIPS,
      ENGLISH_NAME,
      CAPITAL,
      AREA_SQ_KM,
      POPULATION,
      CONTINENT,
      TLD,
      CURRENCY_CODE,
      CURRENCY_NAME,
      PHONE,
      POSTAL_CODE_FORMAT,
      POSTAL_CODE_REGEX,
      LANGUAGES,
      GEONAMEID,
      NEIGHBOURS,
      EQUIVALENT_FIPS_CODE = Value
}

object CountryNames {
  val englishNameOverrides = Map(
    "TA" -> "Tristan da Cunha",
    "AC" -> "Ascension Island",
    "DG" -> "Diego Garcia",
    "IC" -> "Canary Islands",
    "CP" -> "Clipperton Island",
    "EA" -> "Ceuta and Melilla"
  )
}

case class CountryInfo(
  iso2: String,
  iso3: String,
  fips: String,
  englishName: String,
  languages: List[String],
  geonameid: Int
) {
  def getEnglishName: String = 
    CountryNames.englishNameOverrides.get(iso2).getOrElse(englishName)

  def bestLang: Option[String] = getLangs.headOption
  def getLangs: List[String] = languages.map(_.split("-")(0))

  // assume 2 letter incoming, allow variant matching
  def isLocalLanguage(lang: String) = getLangs.exists(_.startsWith(lang))
}

object CountryInfo {
  import CountryInfoFields._

  val englishNameOverrides = Map(
    "NL" -> "The Netherlands",
    "PN" -> "Pitcairn Islands"
  )

  val countryLines = new BufferedSource(getClass.getResourceAsStream("/countryInfo.txt")).getLines
  val countryInfos = countryLines.filterNot(l => l.startsWith("#") || l.isEmpty).toList.flatMap(l => {
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

  private val countryInfoMap = countryInfos.map(ci => (ci.iso2, ci)).toMap
  private val countryInfoMapByISO3 = countryInfos.map(ci => (ci.iso3, ci)).toMap

  private val codeConversions = Map(
    "FX" -> "FR", // "metropolitan france"
    "KO-" -> "XK", // Kosovo
    "CYN" -> "CY", // Northern Cyprus
    "AP" -> "XX",  // Officially reserved (Asia/Pacific or African Regional Industrial Property Organization)
    "NQ" -> "AQ", // Merged into Antarctica (AQ, ATA, 010)
    "JT" -> "UM", // Merged into United States Minor Outlying Islands (UM, UMI, 581)
    "MI" -> "UM", // Merged into United States Minor Outlying Islands (UM, UMI, 581)
    "WK" -> "UM", // Merged into United States Minor Outlying Islands (UM, UMI, 581)
    "PU" -> "UM", // Merged into United States Minor Outlying Islands (UM, UMI, 581)
    "CT" -> "KI", // Merged into Kiribati (KI, KIR, 296)
    "PZ" -> "PA" //  Merged into Panama (PA, PAN, 591)
  )

  def getCanonicalISO2(cc: String): String = codeConversions.getOrElse(cc, cc)

  def getCountryInfoByISO2(cc: String): Option[CountryInfo] =
    countryInfoMap.get(codeConversions.getOrElse(cc, cc))

  def getCountryInfoByISO3(cc: String): Option[CountryInfo] =
    countryInfoMapByISO3.get(cc)

  def getCountryInfo(cc: String): Option[CountryInfo] =
    getCountryInfoByISO2(cc).orElse(getCountryInfoByISO3(cc))
}