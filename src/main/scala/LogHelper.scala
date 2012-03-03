// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder

trait LogHelper {
//  val logger = LoggerFactory.getLogger(classOf[this])
  class FakeLogger {
    def debug(s: String) { println(s) }
    def error(s: String) { println(s) }
    def info(s: String)  { println(s) }
  }

  val logger = new FakeLogger()
}