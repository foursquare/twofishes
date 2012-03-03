// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder

// comments from http://developer.yahoo.com/geo/geoplanet/guide/concepts.html

object YahooWoeTypes {
  val COUNTRY = 12
  val POSTAL_CODE = 11

  // One of the major populated places within a country.
  // This category includes incorporated cities and towns, major unincorporated towns and villages.
  val TOWN = 7

  // One of the subdivisions within a town. This category includes suburbs, neighborhoods, wards.
  val SUBURB = 22

  // One of the primary administrative areas within a country.
  // Place type names associated with this place type include:
  // State, Province, Prefecture, Country, Region, Federal District.
  val ADMIN1 = 8 

  // One of the secondary administrative areas within a country.
  // Place type names associated with this place type include:
  // County, Province, Parish, Department, District.
  val ADMIN2 = 9

  // One of the tertiary administrative areas within a country.
  // Place type names associated with this place type include:
  // Commune, Municipality, District, Ward.
  val ADMIN3 = 10

  val AIRPORT = 14

  val order = List(POSTAL_CODE, AIRPORT,  TOWN, SUBURB, ADMIN3, ADMIN2, ADMIN1, COUNTRY)

  def getOrdering(woetype: Option[Int]): Int = {
    woetype.map(t => order.indexOf(t)).getOrElse(-10)
  }
}