// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder

trait Helpers {
  def tryo[T](f: => T): Option[T] = {
    try {
      Some(f)
    } catch {
      case _ => None
    }
  }

  def flattryo[T](f: => Option[T]): Option[T] = {
    try {
      f
    } catch {
      case _ => None
    }
  }
}