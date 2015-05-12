package com.foursquare.geo.country


object CountryUtils {
  val englishNameOverrides = Map(
    "TA" -> "Tristan da Cunha",
    "AC" -> "Ascension Island",
    "DG" -> "Diego Garcia",
    "IC" -> "Canary Islands",
    "CP" -> "Clipperton Island",
    "EA" -> "Ceuta and Melilla"
  )

  def getNameByCode(code: String): Option[String] = {
    englishNameOverrides.get(CountryInfo.getCanonicalISO2(code))
      .orElse(CountryInfo.getCountryInfo(code).map(_.englishName))
  }

  def bestLang(code: String): Option[String] =
    getLangs(code).headOption

  def getLangs(code: String): List[String] =
    getLangsWithVariants(code).map(_.split("-")(0))

  def getLangsWithVariants(code: String): List[String] =
    CountryInfo.getCountryInfo(code).toList.flatMap(_.languages)
}