// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

trait TwofishesLogger {
  def ifDebug(formatSpecifier: String, va: Any*)
}
