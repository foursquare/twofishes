// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import scala.collection.mutable.HashMap

object SlugEntryMap {
  type SlugEntryMap = HashMap[String, SlugEntry]
}

case class SlugEntry(
  id: String,
  score: Int,
  var deprecated: Boolean = false,
  permanent: Boolean = false
) {
  override def toString(): String = {
    "%s\t%s\t%s".format(id, score, deprecated)
  }
}
