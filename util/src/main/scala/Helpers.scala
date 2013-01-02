// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import com.twitter.util.Duration

object Helpers {
  def duration[T](name: String)(f: => T): T = {
    val (rv, duration) = Duration.inMilliseconds(f)
    println("%s took %s seconds".format(name, duration.inSeconds))
    rv
  }

  def TryO[T](f: => T): Option[T] = {
    try {
      Some(f)
    } catch {
      case _ => None
    }
  }

  def flatTryO[T](f: => Option[T]): Option[T] = {
    try {
      f
    } catch {
      case _ => None
    }
  }
}