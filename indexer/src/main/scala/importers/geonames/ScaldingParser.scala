package com.foursquare.twofishes.importers.geonames

import com.twitter.scalding._
import TDsl._


case class GeonamesAdminFeature(
  geonameid: Long,
  name: String,
  asciiname: String,
  alternateNames: String,
  latitude: Double,
  longitude: Double,
  featureClass: Char,
  featureCode: String,
  countryCode: String,
  cc2: String,
  admin1Code: String,
  admin2Code: String,
  admin3Code: String,
  admin4Code: String,
  population: Long,
  elevation: Long,
  gtopo30: String,
  timezone: String,
  modificationDate: String
)

class WordCountJob(args : Args) extends Job(args) {
  Tsv( args("input") )
    .flatMap('line -> 'word) { line : String => line.split("""\s+""") }
    .groupBy('word) { _.size }
    .write( Tsv( args("output") ) )
}
