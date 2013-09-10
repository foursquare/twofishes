 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

case class GeocodeServerConfig(
  runHttpServer: Boolean = true,
  thriftServerPort: Int = 8080,
  hfileBasePath: String = "",
  shouldPreload: Boolean = true,
  shouldWarmup: Boolean = false
)

object GeocodeServerConfigParser {
  def parse(args: Array[String]): GeocodeServerConfig = {
    val parser =
      new scopt.OptionParser[GeocodeServerConfig]("twofishes") {
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
        }

    // parser.parse returns Option[C]
    parser.parse(args, GeocodeServerConfig()) getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
      GeocodeServerConfig()
    }
  }
}
