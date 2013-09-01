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
  providerMapping: Map[String, Int] = Map("geonameid" -> 1)
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
        opt[(String, Int)]("provider_mapping")
          .text("mapping from provider namespace to integer")
          .action { case ((key, value), c) =>
            println(key)
            println(value)
            if (value == 1) {
              throw new Exception("1 is reserved for geonameid")
            }
            c.copy(providerMapping = (c.providerMapping ++ Map(key -> value)))
=======
  var hfileBasePath: String = null

  var outputPrefixIndex: Boolean = true
  var outputRevgeo: Boolean = false

  var reloadData: Boolean = true

  private val config = this

  val providerMapping = new HashMap[String, Int]
  providerMapping("geonameid") = 1

  val parser =
    new scopt.OptionParser("twofishes") {
      booleanOpt("parse_world", "parse the whole world, or one country",
        { v: Boolean => config.parseWorld = v } )
      opt("parse_country", "country to parse, two letter iso code",
        { v: String => config.parseCountry = v } )
      booleanOpt("parse_postal_codes", "parse postal codes",
        { v: Boolean => config.importPostalCodes = v } )
      opt("hfile_basepath", "directory to output hfiles to",
        { v: String => config.hfileBasePath = v} )
      booleanOpt("parse_alternate_names", "parse alternate names",
        { v: Boolean => config.importAlternateNames = v } )
      booleanOpt("output_prefix_index", "wheter or not to output autocomplete acceleration index",
        { v: Boolean => config.outputPrefixIndex = v} )
      booleanOpt("build_missing_slugs", "build pretty hopefully stable slugs per feature",
        { v: Boolean => config.buildMissingSlugs = v } )
      booleanOpt("output_revgeo_index", "whether or not to output s2 revgeo index",
        { v: Boolean => config.outputRevgeo = v} )
      booleanOpt("reload_data", "reload data into mongo",
        { v: Boolean => config.reloadData = v} )
      keyValueOpt("provider_mapping", "mapping from provider namespace to integer",
        {(key: String, value: String) => {
          println(key)
          println(value)
          if (value == "1") {
            throw new Exception("1 is reserved for geonameid")
          }
          .keyValueName("providerid", "intValue")
      }

    // parser.parse returns Option[C]
    parser.parse(args, GeonamesImporterConfig()) getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
      GeonamesImporterConfig()
    }
  }
}
