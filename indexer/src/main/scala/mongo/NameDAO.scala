// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.mongo

import com.foursquare.twofishes.util.StoredFeatureId
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

case class NameIndex(
  name: String,
  fid: Long,
  cc: String,
  pop: Int,
  woeType: Int,
  flags: Int,
  lang: String,
  noPrefix: Boolean,
  @Key("_id") _id: ObjectId
) {

  def fidAsFeatureId = StoredFeatureId.fromLong(fid).getOrElse(
    throw new RuntimeException("can't convert %d to a feature id".format(fid)))
}

object NameIndexDAO extends SalatDAO[NameIndex, String](
  collection = MongoConnection()("geocoder")("name_index")) {
  def makeIndexes() {
    collection.ensureIndex(DBObject("name" -> -1, "pop" -> -1, "fid" -> 1, "noPrefix" -> 1))
  }
}