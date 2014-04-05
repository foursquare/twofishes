// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

case class GeonamesImporterConfig(
  // Buildings are places like the eiffel tower, but also schools and federal offices.
  shouldParseBuildings: Boolean = false,

  // If set to true, the code expects data/downloaded/allCountries.txt and data/downloaded/zip/allCountries.txt
  // If set to false, the code expects data/downloaded/<parseCountry>.txt and data/downloaded/zip/<parseCountry>.txt
  parseWorld: Boolean = false,
  parseCountry: String = "US",

  // Expects data/downloaded/alternateNames.txt
  // This is an important file because it contains translated names, abbreviated names, and preferred names
  // for each feature. Without it, we can't generate pretty strings for display
  importAlternateNames: Boolean = true,
  importPostalCodes: Boolean = true,

  // Geonames doesn't have bounding boxes, only points. This is a precomputed mapping of geonameids to yahoo
  // woeids to flickr bounding boxes. Precomputed because I could't get the geojson java libraries to work.
  importBoundingBoxes: Boolean = true,
  boundingBoxDirectory: String = "./data/computed/bboxes/",

  buildMissingSlugs: Boolean = false,
  hfileBasePath: String = null,
  outputPrefixIndex: Boolean = true,
  outputRevgeo: Boolean = false,
  reloadData: Boolean = true,
  redoPolygonMatches: Boolean = false
)

object GeonamesImporterConfigParser {
  def parse(args: Array[String]): GeonamesImporterConfig = {
    val parser =
      new scopt.OptionParser[GeonamesImporterConfig]("twofishes") {
        opt[Boolean]("parse_world")
          .text("parse the whole world, or one country")
          .action{ (v, c) => c.copy(parseWorld = v) }
        opt[String]("parse_country")
          .text("country to parse, two letter iso code")
          .action{ (v, c) => c.copy(parseCountry = v) }
        opt[Boolean]("parse_postal_codes")
          .text("parse postal codes")
          .action{ (v, c) => c.copy(importPostalCodes = v) }
        opt[String]("hfile_basepath")
          .text("directory to output hfiles to")
          .action{ (v, c) => c.copy(hfileBasePath = v )}
          .required()
        opt[Boolean]("parse_alternate_names")
          .text("parse alternate names")
          .action{ (v, c) => c.copy(importAlternateNames = v ) }
        opt[Boolean]("output_prefix_index")
          .text("wheter or not to output autocomplete acceleration index")
          .action{ (v, c) => c.copy(outputPrefixIndex = v) }
        opt[Boolean]("build_missing_slugs")
          .text("build pretty hopefully stable slugs per feature")
          .action{ (v, c) => c.copy(buildMissingSlugs = v ) }
        opt[Boolean]("output_revgeo_index")
          .text("whether or not to output s2 revgeo index")
          .action{ (v, c) => c.copy(outputRevgeo = v) }
        opt[Boolean]("reload_data")
          .text("reload data into mongo")
          .action{ (v, c) => c.copy(reloadData = v) }
        opt[Boolean]("redo_polygon_matches")
          .text("redo polygon matches for files which have a mapping.json")
          .action{ (v, c) => c.copy(redoPolygonMatches = v) }
      }

    // parser.parse returns Option[C]
    parser.parse(args, GeonamesImporterConfig()) getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
      GeonamesImporterConfig()
    }
  }
}
