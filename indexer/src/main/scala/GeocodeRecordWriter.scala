// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofish

import com.novus.salat._
import com.novus.salat.global._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import scala.collection.JavaConversions._
import com.twitter.util.{Future, FuturePool}

case class NameIndex(
  @Key("_id") name: String,
  fids: List[String]
)

case class FidIndex(
  @Key("_id") fid: String,
  oid: ObjectId
)

trait GeocodeStorageWriteService {
  def insert(record: GeocodeRecord): Unit
  def setRecordNames(id: StoredFeatureId, names: List[DisplayName])
  def addNameToRecord(name: DisplayName, id: StoredFeatureId)
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

    record.names.foreach(name => {
      record.ids.foreach(fid => {
        NameIndexDAO.update(MongoDBObject("_id" -> name),
          MongoDBObject("$addToSet" -> MongoDBObject("fids" -> fid)),
          true, false)
      })
    })
  }

  def addNameToRecord(name: DisplayName, id: StoredFeatureId) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$addToSet" -> MongoDBObject("dns" -> grater[DisplayName].asDBObject(name))),
      false, false)
  }

  def addBoundingBoxToRecord(id: StoredFeatureId, bbox: BoundingBox) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$set" -> MongoDBObject("bb" -> grater[BoundingBox].asDBObject(bbox))),
      false, false)
  }

  def setRecordNames(id: StoredFeatureId, names: List[DisplayName]) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$set" -> MongoDBObject(
        "dns" -> names.map(n => grater[DisplayName].asDBObject(n)))),
      false, false)
  }
}