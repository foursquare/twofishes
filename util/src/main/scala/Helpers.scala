// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

object Helpers {
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