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
  flags: Int,
  lang: String,
  @Key("_id") _id: ObjectId
  // var nameBytes: Array[Byte] = Array.empty
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
  def addSlugToRecord(id: StoredFeatureId, slug: String)
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord]
}

object MongoGeocodeDAO extends SalatDAO[GeocodeRecord, ObjectId](
  collection = MongoConnection()("geocoder")("features"))

object NameIndexDAO extends SalatDAO[NameIndex, String](
  collection = MongoConnection()("geocoder")("name_index"))

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
    // val nameBytes = name.name.getBytes("UTF-8")
    // val paddingSize = 200
    // if (nameBytes.size > paddingSize) {
    //   throw new Exception("nameBytes.size > %d".format(paddingSize))
    // }
    // val byteArray = new Array[Byte](paddingSize)
    // System.arraycopy(nameBytes, 0, byteArray, byteArray.size - nameBytes.size, nameBytes.size)
    // name.nameBytes = byteArray
    NameIndexDAO.insert(name)
  }

  def addBoundingBoxToRecord(id: StoredFeatureId, bbox: BoundingBox) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))),
      MongoDBObject("$set" -> MongoDBObject("boundingbox" -> grater[BoundingBox].asDBObject(bbox))),
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