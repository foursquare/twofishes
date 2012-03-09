// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder

import com.twitter.logging.Logger
import com.twitter.logging.config._
import com.twitter.logging.config.ConsoleHandlerConfig

object LogHelper {
  def init() {
    val config = new LoggerConfig {
      node = ""
      level = Level.INFO
      handlers = new ConsoleHandlerConfig {}
      // handlers = new FileHandlerConfig {
      //   filename = "/var/log/example/example.log"
      //   roll = Policy.SigHup
      // }
    }
    config()
  }
}

trait LogHelper {
  val logger = Logger.get(getClass)
}