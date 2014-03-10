// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import com.twitter.util.Duration
import com.weiglewilczek.slf4s.Logging

object Helpers extends Logging {
  def duration[T](name: String)(f: => T): T = {
    val (rv, duration) = Duration.inMilliseconds(f)
    logger.info("%s took %s seconds".format(name, duration.inSeconds))
    rv
  }

  def TryO[T](f: => T): Option[T] = {
    try {
      Some(f)
    } catch {
      case _: Exception => None
    }
  }

  def flatTryO[T](f: => Option[T]): Option[T] = {
    try {
      f
    } catch {
      case _: Exception => None
    }
  }
}
