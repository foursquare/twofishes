// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.util.StoredFeatureId
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.mongodb.Bytes
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

trait GeocodeStorageWriteService {
  def insert(record: GeocodeRecord): Unit
  def insert(record: List[GeocodeRecord]): Unit
  def setRecordNames(id: StoredFeatureId, names: List[DisplayName])
  def addBoundingBoxToRecord(bbox: BoundingBox, id: StoredFeatureId)
  def addNameToRecord(name: DisplayName, id: StoredFeatureId)
  def addNameIndex(name: NameIndex)
  def addNameIndexes(names: List[NameIndex])
  def addPolygonToRecord(id: StoredFeatureId, polyId: ObjectId)
  def addSlugToRecord(id: StoredFeatureId, slug: String)
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord]
}

object MongoGeocodeDAO extends SalatDAO[GeocodeRecord, ObjectId](
  collection = MongoConnection()("geocoder")("features")) {
  def makeIndexes() {
    collection.ensureIndex(DBObject("ids" -> -1))
    collection.ensureIndex(DBObject("hasPoly" -> -1))
    collection.ensureIndex(DBObject("loc" -> "2dsphere", "_woeType" -> -1))

  }
}

case class NameIndex(
  name: String,
  fid: Long,
  pop: Int,
  woeType: Int,
  flags: Int,
  lang: String,
  @Key("_id") _id: ObjectId
) {

  def fidAsFeatureId = StoredFeatureId.fromLong(fid).getOrElse(
    throw new RuntimeException("can't convert %d to a feature id".format(fid)))
}

object NameIndexDAO extends SalatDAO[NameIndex, String](
  collection = MongoConnection()("geocoder")("name_index")) {
  def makeIndexes() {
    collection.ensureIndex(DBObject("name" -> -1, "pop" -> -1))
  }
}

case class PolygonIndex(
  @Key("_id") _id: ObjectId,
  polygon: Array[Byte]
)

object PolygonIndexDAO extends SalatDAO[PolygonIndex, String](
  collection = MongoConnection()("geocoder")("polygon_index")) {
  def makeIndexes() {}
}

case class RevGeoIndex(
  cellid: Long,
  polyId: ObjectId,
  full: Boolean,
  geom: Option[Array[Byte]]
)

object RevGeoIndexDAO extends SalatDAO[RevGeoIndex, String](
  collection = MongoConnection()("geocoder")("revgeo_index")) {
  def makeIndexes() {
    collection.ensureIndex(DBObject("cellid" -> -1))
  }
}

class MongoGeocodeStorageService extends GeocodeStorageWriteService {
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord] = {
    val geocodeCursor = MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.longId))))
    geocodeCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
    geocodeCursor
  }

  def insert(record: GeocodeRecord) {
    MongoGeocodeDAO.insert(record)
  }

  def insert(records: List[GeocodeRecord]) {
    MongoGeocodeDAO.insert(records)
  }

  def addBoundingBoxToRecord(bbox: BoundingBox, id: StoredFeatureId) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.longId))),
      MongoDBObject("$set" -> MongoDBObject("boundingbox" -> grater[BoundingBox].asDBObject(bbox))),
      false, false)
  }

  def addNameToRecord(name: DisplayName, id: StoredFeatureId) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.longId))),
      MongoDBObject("$addToSet" -> MongoDBObject("displayNames" -> grater[DisplayName].asDBObject(name))),
      false, false)
  }

  def addNameIndex(name: NameIndex) {
    NameIndexDAO.insert(name)
  }

  def addNameIndexes(names: List[NameIndex]) {
    NameIndexDAO.insert(names)
  }

  def addPolygonToRecord(id: StoredFeatureId, polyId: ObjectId) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.longId))),
      MongoDBObject("$set" ->
        MongoDBObject(
          "hasPoly" -> true,
          "polyId" -> polyId
        )
      ),
      false, false)
  }

  def addSlugToRecord(id: StoredFeatureId, slug: String) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.longId))),
      MongoDBObject("$set" -> MongoDBObject("slug" -> slug)),
      false, false)
  }

  def setRecordNames(id: StoredFeatureId, names: List[DisplayName]) {
    MongoGeocodeDAO.update(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.longId))),
      MongoDBObject("$set" -> MongoDBObject(
        "displayNames" -> names.map(n => grater[DisplayName].asDBObject(n)))),
      false, false)
  }
}
