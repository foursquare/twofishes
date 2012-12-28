// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

trait TwofishesLogger {
  def ifDebug(s: => String, level: Int = 0)
}