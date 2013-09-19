//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.foursquare.twofishes.util.TwofishesLogger
import com.twitter.ostrich.stats.Stats
import com.twitter.util.Duration
import java.util.Date
import scala.collection.mutable.ListBuffer
import scalaj.collection.Implicits._

class MemoryLogger(req: CommonGeocodeRequestParams) extends TwofishesLogger {
  def this(req: GeocodeRequest) {
    this(GeocodeRequestUtils.geocodeRequestToCommonRequestParams(req))
  }

  val startTime = new Date()

  def timeSinceStart = {
    new Date().getTime() - startTime.getTime()
  }

  val lines: ListBuffer[String] = new ListBuffer()

  def ifDebug(formatSpecifier: String, va: Any*) {
    if (va.isEmpty) {
      ifLevelDebug(1, "%s", formatSpecifier)
    } else {
      ifLevelDebug(1, formatSpecifier, va:_*)
    }
  }

  def ifLevelDebug(level: Int, formatSpecifier: String, va: Any*) {
    if (level >= 0 && req.debug >= level) {
      val finalString = if (va.isEmpty) {
        formatSpecifier
      } else {
        formatSpecifier.format(va:_*)
      }
      lines.append("%d: %s".format(timeSinceStart, finalString))
    }
  }

  def getLines: List[String] = lines.toList

  def toOutput(): String = lines.mkString("<br>\n");

  def logDuration[T](ostrichKey: String, what: String)(f: => T): T = {
    val (rv, duration) = Duration.inNanoseconds(f)
    Stats.addMetric(ostrichKey + "_usec", duration.inMicroseconds.toInt)
    ifDebug("%s in %s µs / %s ms", what, duration.inMicroseconds, duration.inMilliseconds)
    rv
  }
}
