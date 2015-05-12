// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.geo.country

import scala.io.BufferedSource

object DependentCountryInfo {
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
  def getCountryIdOnWhichCountryIsDependent(cc: String): Option[Int] =
    CountryInfo.getCountryInfo(inverseDependentCountryRelationships.getOrElse(cc, "")).flatMap(_.geonameid)

  def isCountryDependentOnCountry(dcc: String, cc: String) =
    getDependentCountryCodesForCountry(cc).exists(_ == dcc)
}
