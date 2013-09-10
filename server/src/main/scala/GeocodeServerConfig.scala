 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

class GeocodeServerConfig(args: Array[String]) {
  var runHttpServer: Boolean = true
  var thriftServerPort: Int = 8080
  var hfileBasePath: String = null
  var shouldPreload: Boolean = true
  var shouldWarmup: Boolean = false

  private val config = this

  val parser =
    new scopt.OptionParser("twofishes", "0.12") {
      intOpt("p", "port", "port to run thrift server on",
        { v: Int => config.thriftServerPort = v } )
      booleanOpt("h", "run_http_server", "whether or not to run http/json server on port+1",
        { v: Boolean => config.runHttpServer = v } )
      opt("hfile_basepath", "directory containing output hfile for serving",
        { v: String => config.hfileBasePath = v} )
      booleanOpt("preload", "scan the hfiles at startup to prevent a cold start, turn off when testing",
        { v: Boolean => config.shouldPreload = v} )
      booleanOpt("warmup", "run N queries against the server at start, before being healthy",
        { v: Boolean => config.shouldWarmup = v} )
      }

  if (!parser.parse(args)) {
    // arguments are bad, usage message will have been displayed
    System.exit(1)
  }

  if (hfileBasePath == null) {
    println("must specify --hfile_basepath")
    System.exit(1)
  }
}
