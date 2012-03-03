// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder

import com.novus.salat._
import com.novus.salat.global._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection

case class DisplayName(
  @Key("l") langugage: String,
  @Key("n") name: String,
  @Key("p") preferred: Boolean
)

case class FeatureId(
  @Key("n") namespace: String,
  id: String) {
  override def toString = "%s:%s".format(namespace, id)
}

object Implicits {
  implicit def fidToString(fid: FeatureId): String = fid.toString
  implicit def fidListToString(fids: List[FeatureId]): List[String] = fids.map(_.toString)
}

case class GeocodeRecord(
  ids: List[String],
  names: List[String],
  cc: String,
  woeType: Option[Int],
  lat: Double,
  lng: Double,
  @Key("dns") displayNames: List[DisplayName],
  @Key("p") parents: List[String],
  population: Option[Int],
  boost: Option[Int] = None
)

trait GeocodeStorageService {
  def getByName(name: String): Iterator[GeocodeRecord]
  def insert(record: GeocodeRecord): Unit
}

object MongoGeocodeDAO extends SalatDAO[GeocodeRecord, ObjectId](
  collection = MongoConnection()("geocoder")("features"))

class MongoGeocodeStorageService extends GeocodeStorageService {
  override def getByName(name: String): Iterator[GeocodeRecord] = {
    MongoGeocodeDAO.find(MongoDBObject("names" -> name))
  }

  def insert(record: GeocodeRecord) {
    MongoGeocodeDAO.insert(record)
  }
}