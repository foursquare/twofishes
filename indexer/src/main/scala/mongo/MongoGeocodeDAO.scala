// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.mongo

import com.foursquare.twofishes.GeocodeRecord
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

object MongoGeocodeDAO extends SalatDAO[GeocodeRecord, ObjectId](
  collection = MongoConnection()("geocoder")("features")) {
  def makeIndexes() {
    collection.ensureIndex(DBObject("hasPoly" -> -1))
    collection.ensureIndex(DBObject("loc" -> "2dsphere", "_woeType" -> -1))
    collection.ensureIndex(DBObject("polyId" -> 1))
  }
}