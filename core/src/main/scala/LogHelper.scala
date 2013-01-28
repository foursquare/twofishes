// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.twitter.logging.config.{ConsoleHandlerConfig, _}

object LogHelper {
  def init() {
    val config = new LoggerConfig {
      node = ""
      level = Level.ERROR
      handlers = new ConsoleHandlerConfig {}
    }
    config()
  }
}

object NullLogger {
  def ifTrace(msg: => String) {
    println(msg)
  }
  def error(s: String) {}
  def info(s: String) {}
}


 trait LogHelper {
  val logger = NullLogger
 }
