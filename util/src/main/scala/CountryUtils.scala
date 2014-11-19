// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import com.foursquare.twofishes.util.Lists.Implicits._
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

object CountryUtils {
  import CountryInfoFields._

  private val countryCodeToGeoIdMap: Map[String, Long] =
    new BufferedSource(getClass.getResourceAsStream("/countryInfo.txt"))
      .getLines.filterNot(_.startsWith("#"))
      .map(line => {
          val parts = line.split("\t")
          try { 
            (parts(ISO2.id) -> parts(GEONAMEID.id).toLong)
          } catch {
            case e: Exception => {
              (parts(ISO2.id) -> -1.toLong)
            }
          }
        })
      .toMap

  private val countryCodeToLocalLangMap: Map[String, Set[String]] =
    new BufferedSource(getClass.getResourceAsStream("/countryInfo.txt"))
      .getLines.filterNot(_.startsWith("#"))
      .map(line => {
      val parts = line.split("\t")
      val langs = parts(LANGUAGES.id).split(",").map(l => l.split("-")(0)).toSet
      try {
        (parts(ISO2.id) -> langs)
      } catch {
        case e: Exception => {
          (parts(ISO2.id) -> Set.empty[String])
        }
      }
    })
      .toMap
  def isLocalLanguageForCountry(cc: String, lang: String): Boolean = {
    countryCodeToLocalLangMap.getOrElse(cc, Set.empty[String]).contains(lang)
  }

  private val dependentCountryRelationships =
    new BufferedSource(getClass.getResourceAsStream("/dependent_countries.txt"))
      .getLines.filterNot(_.startsWith("#"))
      .map(line => {
          var parts = line.split("\\|")
          (parts(0) -> parts(1).split(",").toList)
        })
      .toMap
	def getDependentCountryCodesForCountry(cc: String): List[String] = 
    dependentCountryRelationships.getOrElse(cc, Nil)

	private val inverseDependentCountryRelationships = (for {
  	  cc <- dependentCountryRelationships.keys
      dcc <- getDependentCountryCodesForCountry(cc)
    } yield {
      (dcc -> cc)
    }).toMap
  // dependent country -> parent country relationships are meant to be one to one
  // this means we cannot support queries of the form "X, Y" where X is in Antarctica
  // and Y is a country with territories in Antarctica, which is probably all right
  def getCountryIdOnWhichCountryIsDependent(cc: String): Option[Long] = 
    countryCodeToGeoIdMap.get(inverseDependentCountryRelationships.getOrElse(cc, ""))

  def isCountryDependentOnCountry(dcc: String, cc: String) =
    getDependentCountryCodesForCountry(cc).has(dcc)

}
