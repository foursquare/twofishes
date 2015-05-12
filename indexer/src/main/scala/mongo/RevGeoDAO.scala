// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.mongo

import com.foursquare.twofishes.GeocodePoint
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

case class RevGeoIndex(
  cellid: Long,
  polyId: ObjectId,
  full: Boolean,
  geom: Option[Array[Byte]],
  point: Option[(Double, Double)] = None
) {
  def getGeocodePoint: Option[GeocodePoint] = {
    point.map({case (lat, lng) => {
      GeocodePoint.newBuilder.lat(lat).lng(lng).result
	}})
  }
}

object RevGeoIndexDAO extends SalatDAO[RevGeoIndex, String](
  collection = MongoConnection()("geocoder")("revgeo_index")) {
  def makeIndexes() {
    collection.ensureIndex(DBObject("cellid" -> -1))
  }
}