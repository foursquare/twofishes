package com.foursquare.twofishes.util

import com.twitter.ostrich.stats.Stats
import com.twitter.util.Duration
import com.weiglewilczek.slf4s.Logging

trait DurationUtils extends Logging {
  def logDuration[T](ostrichKey: String, extraInfo: String = "")(f: => T): T = {
    val (rv, duration) = Duration.inNanoseconds(f)
    if (duration.inMilliseconds > 200) {
      logger.debug(ostrichKey + ": " + extraInfo + " in %s µs / %s ms".format(duration.inMicroseconds, duration.inMilliseconds))
    }
    Stats.addMetric(ostrichKey + "_msec", duration.inMilliseconds.toInt)
    rv
  }

  def logPhase[T](what: String)(f: => T): T = {
    logger.info("starting: " + what)
    val (rv, duration) = Duration.inNanoseconds(f)
    logger.info("finished: %s in %s secs / %s mins".format(
    	what, duration.inSeconds, duration.inMinutes
    ))
    rv
  }
 }
