// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofish.importers.geonames

object GeonamesImporterConfig {
  // Buildings are places like the eiffel tower, but also schools and federal offices.
  val shouldParseBuildings = false

  // If set to true, the code expects data/downloaded/allCountries.txt and data/downloaded/zip/allCountries.txt
  // If set to false, the code expects data/downloaded/<parseCountry>.txt and data/downloaded/zip/<parseCountry>.txt
  val parseWorld = false
  val parseCountry = "US"

  // Expects data/downloaded/alternateNames.txt
  // This is an important file because it contains translated names, abbreviated names, and preferred names
  // for each feature. Without it, we can't generate pretty strings for display

  val importAlternateNames = false
  val importPostalCodes = true

  // Geonames doesn't have bounding boxes, only points. This is a precomputed mapping of geonameids to yahoo
  // woeids to flickr bounding boxes. Precomputed because I could't get the geojson java libraries to work.
  val importBoundingBoxes = false
  val boundingBoxFilename = "./data/computed/flickr_bbox.tsv"
}
