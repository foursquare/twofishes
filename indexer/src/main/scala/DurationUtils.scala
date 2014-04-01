// should move to util
package com.foursquare.twofishes

import com.twitter.util.Duration
import com.weiglewilczek.slf4s.Logging

trait DurationUtils extends Logging {
  def logDuration[T](what: String)(f: => T): T = {
    val (rv, duration) = Duration.inNanoseconds(f)
    if (duration.inMilliseconds > 200) {
      logger.debug(what + " in %s µs / %s ms".format(duration.inMicroseconds, duration.inMilliseconds))
    }
    rv
  }
}