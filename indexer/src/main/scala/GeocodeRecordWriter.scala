// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import scala.collection.JavaConversions._

case class NameIndex(
  name: String,
  fid: String,
  pop: Int,
  woeType: Int,
  flags: Int,
  lang: String,
  @Key("_id") _id: ObjectId
)

case class IdPop(
  fid: String,
  pop: Int
)

case class PolygonIndex(
  @Key("_id") _id: String,
  wkbGeometry: Array[Byte]
)

trait GeocodeStorageWriteService {
  def insert(record: GeocodeRecord): Unit
  def setRecordNames(id: StoredFeatureId, names: List[DisplayName])
  def addNameToRecord(name: DisplayName, id: StoredFeatureId)
  def addNameIndex(name: NameIndex)
  def savePolygon(id: StoredFeatureId, wkbGeometry: Array[Byte])
  def addPolygonToRecord(id: StoredFeatureId, wkbGeometry: Array[Byte])
  def addSlugToRecord(id: StoredFeatureId, slug: String)
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord]
}

object MongoGeocodeDAO extends SalatDAO[GeocodeRecord, ObjectId](
  collection = MongoConnection()("geocoder")("features"))

object NameIndexDAO extends SalatDAO[NameIndex, String](
  collection = MongoConnection()("geocoder")("name_index"))

object PolygonIndexDAO extends SalatDAO[PolygonIndex, String](
  collection = MongoConnection()("geocoder")("polygons"))

class MongoGeocodeStorageService extends GeocodeStorageWriteService {
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord] = {
    MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))))
  }

  def insert(record: GeocodeRecord) {
    MongoGeocodeDAO.insert(record)
  }

  def addNameToRecord(name: DisplayName, id: StoredFeatureId) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$addToSet" -> MongoDBObject("displayNames" -> grater[DisplayName].asDBObject(name))),
      false, false)
  }

  def addNameIndex(name: NameIndex) {
    NameIndexDAO.insert(name)
  }


  def savePolygon(id: StoredFeatureId, wkbGeometry: Array[Byte]) {
    PolygonIndexDAO.insert(
      PolygonIndex(id.toString, wkbGeometry)
    )
  }

  def addPolygonToRecord(id: StoredFeatureId, wkbGeometry: Array[Byte]) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$set" ->
        MongoDBObject(
          "wkbGeometry" -> wkbGeometry,
          "hasPoly" -> true
        )
      ),
      false, false)
  }


  def addSlugToRecord(id: StoredFeatureId, slug: String) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$set" -> MongoDBObject("slug" -> slug)),
      false, false)
  }

  def setRecordNames(id: StoredFeatureId, names: List[DisplayName]) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$set" -> MongoDBObject(
        "displayNames" -> names.map(n => grater[DisplayName].asDBObject(n)))),
      false, false)
  }
}