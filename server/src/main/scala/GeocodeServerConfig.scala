 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

case class GeocodeServerConfig(
  runHttpServer: Boolean = true,
  thriftServerPort: Int = 8080,
  host: String = "0.0.0.0",
  hfileBasePath: String = "",
  shouldPreload: Boolean = true,
  shouldWarmup: Boolean = false,
  maxTokens: Int = 10,
  reload: Boolean = false,
  hotfixBasePath: String = "",
  enablePrivateEndpoints: Boolean = false,
  removeLowRankingParses: Boolean = true,
  minPopulationForLowRankingParse: Int = 50000
)

object GeocodeServerConfigSingleton {
  var config: GeocodeServerConfig = null

  def init(args: Array[String]) = {
    config = GeocodeServerConfigParser.parse(args)
    config
  }
}

object GeocodeServerConfigParser {
  def parse(args: Array[String]): GeocodeServerConfig = {
    val parser =
      new scopt.OptionParser[GeocodeServerConfig]("twofishes") {
        opt[String]("host")
          .text("bind to specified host (default 0.0.0.0)")
          .action { (x, c) => c.copy(host = x) }
        opt[Int]('p', "port")
          .action { (x, c) => c.copy(thriftServerPort = x) }
          .text("port to run thrift server on")
        opt[Boolean]('h', "run_http_server")
          .action { (x, c) => c.copy(runHttpServer = x) }
          .text("whether or not to run http/json server on port+1")
        opt[String]("hfile_basepath")
          .text("directory containing output hfile for serving")
          .action { (x, c) => c.copy(hfileBasePath = x) }
          .required
        opt[Boolean]("preload")
          .text("scan the hfiles at startup to prevent a cold start, turn off when testing")
          .action { (x, c) => c.copy(shouldPreload = x) }
        opt[Boolean]("warmup")
          .text("warmup the server at startup to prevent a cold start, turn off when testing")
          .action { (x, c) => c.copy(shouldWarmup = x) }
        opt[Int]("max_tokens")
          .action { (x, c) => c.copy(maxTokens = x) }
          .text("maximum number of tokens to allow geocoding")
        opt[String]("hotfix_basepath")
          .text("directory containing hotfix files")
          .action { (x, c) => c.copy(hotfixBasePath = x) }
        opt[Boolean]("enable_private_endpoints")
          .text("enable private endpoints on server")
          .action { (x, c) => c.copy(enablePrivateEndpoints = x)}
        opt[Boolean]("remove_low_ranking_parses")
          .text("whether to remove low ranking parses")
          .action { (x, c) => c.copy(removeLowRankingParses = x)}
        opt[Int]("min_population_for_low_ranking_parse")
          .text("the minimum population to be considered a low ranking parse")
          .action { (x, c) => c.copy(minPopulationForLowRankingParse = x)}
        }

    // parser.parse returns Option[C]
    parser.parse(args, GeocodeServerConfig()) getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
      GeocodeServerConfig()
    }
  }
}
