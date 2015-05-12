// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.mongo

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

case class PolygonIndex(
  @Key("_id") _id: ObjectId,
  polygon: Array[Byte],
  source: String
)

object PolygonIndexDAO extends SalatDAO[PolygonIndex, String](
  collection = MongoConnection()("geocoder")("polygon_index")) {
  def makeIndexes() {}
}