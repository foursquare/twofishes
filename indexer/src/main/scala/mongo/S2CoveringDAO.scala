// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.mongo

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

case class S2CoveringIndex(
  @Key("_id") _id: ObjectId,
  cellIds: List[Long]
)

object S2CoveringIndexDAO extends SalatDAO[S2CoveringIndex, String](
  collection = MongoConnection()("geocoder")("s2_covering_index")) {
  def makeIndexes() {}
}