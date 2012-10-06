// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.novus.salat._
import com.novus.salat.global._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import scala.collection.JavaConversions._
import com.twitter.util.{Future, FuturePool}

case class NameIndex(
  name: String,
  fid: String,
  pop: Int,
  woeType: Int,
  alias: Boolean,
  @Key("_id") _id: ObjectId
)

case class FidIndex(
  @Key("_id") fid: String,
  oid: ObjectId
)

case class IdPop(
  fid: String,
  pop: Int
)

trait GeocodeStorageWriteService {
  def insert(record: GeocodeRecord): Unit
  def setRecordNames(id: StoredFeatureId, names: List[DisplayName])
  def addNameToRecord(name: DisplayName, id: StoredFeatureId)
  def addNameIndex(name: NameIndex)
  def addBoundingBoxToRecord(id: StoredFeatureId, bbox: BoundingBox)
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord]
}

object MongoGeocodeDAO extends SalatDAO[GeocodeRecord, ObjectId](
  collection = MongoConnection()("geocoder")("features"))

object NameIndexDAO extends SalatDAO[NameIndex, String](
  collection = MongoConnection()("geocoder")("name_index"))

object FidIndexDAO extends SalatDAO[FidIndex, String](
  collection = MongoConnection()("geocoder")("fid_index"))

class MongoGeocodeStorageService extends GeocodeStorageWriteService {
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord] = {
    MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))))
  }

  def insert(record: GeocodeRecord) {
    MongoGeocodeDAO.insert(record)

    record.ids.foreach(fid => {
      FidIndexDAO.insert(FidIndex(fid, record._id))
    })
  }

  def addNameToRecord(name: DisplayName, id: StoredFeatureId) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$addToSet" -> MongoDBObject("displayNames" -> grater[DisplayName].asDBObject(name))),
      false, false)
  }

  def addNameIndex(name: NameIndex) {
    NameIndexDAO.insert(name)
  }

  def addBoundingBoxToRecord(id: StoredFeatureId, bbox: BoundingBox) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$set" -> MongoDBObject("boundingbox" -> grater[BoundingBox].asDBObject(bbox))),
      false, false)
  }

  def setRecordNames(id: StoredFeatureId, names: List[DisplayName]) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$set" -> MongoDBObject(
        "displayNames" -> names.map(n => grater[DisplayName].asDBObject(n)))),
      false, false)
  }
}

