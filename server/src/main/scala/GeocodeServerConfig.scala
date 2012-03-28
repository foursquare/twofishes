 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofish


class GeocodeServerConfig(args: Array[String]) {
  var runHttpServer: Boolean = false
  var thriftServerPort: Int = 8080

  private val config = this

  val parser = 
    new scopt.OptionParser("twofish", "0.12") {
      intOpt("p", "port", "port to run thrift server on",
        { v: Int => config.thriftServerPort = v } )
      booleanOpt("h", "run_http_server", "whether or not to run http/json server on port+1",
        { v: Boolean => config.runHttpServer = v } )
    }

  if (!parser.parse(args)) {
    // arguments are bad, usage message will have been displayed
    System.exit(1)
  }
}
