// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.indexer

case class IndexRunnerConfig(
  hfileBasePath: String = null,
  outputPrefixIndex: Boolean = true,
  outputRevgeo: Boolean = false,
  outputS2Covering: Boolean = false,
  reloadData: Boolean = true,
  redoPolygonMatching: Boolean = false,
  skipPolygonMatching: Boolean = false,
  revgeoIndexPoints: Boolean = false,
  createUnmatchedFeatures: Boolean = false
)

object IndexRunnerConfigParser {
  def parse(args: Array[String]): IndexRunnerConfig = {
    val parser =
      new scopt.OptionParser[IndexRunnerConfig]("twofishes") {
        opt[String]("hfile_basepath")
          .text("directory to output hfiles to")
          .action{ (v, c) => c.copy(hfileBasePath = v )}
          .required()
        opt[Boolean]("output_prefix_index")
          .text("wheter or not to output autocomplete acceleration index")
          .action{ (v, c) => c.copy(outputPrefixIndex = v) }
        opt[Boolean]("output_revgeo_index")
          .text("whether or not to output s2 revgeo index")
          .action{ (v, c) => c.copy(outputRevgeo = v) }
        opt[Boolean]("output_s2_covering_index")
          .text("whether or not to output s2 covering index")
          .action{ (v, c) => c.copy(outputS2Covering = v) }
        opt[Boolean]("reload_data")
          .text("reload data into mongo")
          .action{ (v, c) => c.copy(reloadData = v) }
        opt[Boolean]("redo_polygon_matching")
          .text("redo polygon matches for files which have a mapping.json")
          .action{ (v, c) => c.copy(redoPolygonMatching = v) }
        opt[Boolean]("skip_polygon_matching")
          .text("don't try to match polygons to geonames features for which we don't have a mapping")
          .action{ (v, c) => c.copy(skipPolygonMatching = v) }
        opt[Boolean]("revgeo_index_points")
          .text("index point features for radius queries")
          .action{ (v, c) => c.copy(revgeoIndexPoints = v) }
        opt[Boolean]("create_unmatched_features")
          .text("create features for unmatched polygons")
          .action{ (v, c) => c.copy(createUnmatchedFeatures = v) }
      }

    // parser.parse returns Option[C]
    parser.parse(args, IndexRunnerConfig()) getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
      IndexRunnerConfig()
    }
  }
}