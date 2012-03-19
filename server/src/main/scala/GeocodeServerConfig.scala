 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.geocoder

object GeocodeServerConfig {
  // Whether or not to run the http server that lets you issue interactive GET requests
  // to the thrift server/
  val runHttpServer = true

  // This port will be used for finagle or vanilla-thrift, depending on which you run
  val thriftServerPort = 8080
}