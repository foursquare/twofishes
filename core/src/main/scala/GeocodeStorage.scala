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

object Implicits {
  implicit def fidToString(fid: StoredFeatureId): String = fid.toString
  implicit def fidListToString(fids: List[StoredFeatureId]): List[String] = fids.map(_.toString)
}

case class DisplayName(
  @Key("l") lang: String,
  @Key("n") name: String,
  @Key("p") preferred: Boolean
)

case class StoredFeatureId(
  @Key("n") namespace: String,
  id: String) {
  override def toString = "%s:%s".format(namespace, id)
}

case class Point(lat: Double, lng: Double)

case class BoundingBox(
  ne: Point,
  sw: Point
)

case class GeocodeRecord(
  _id: ObjectId = new ObjectId,
  ids: List[String],
  names: List[String],
  cc: String,
  @Key("wt") _woeType: Int,
  lat: Double,
  lng: Double,
  @Key("dns") displayNames: List[DisplayName],
  @Key("p") parents: List[String],
  population: Option[Int],
  boost: Option[Int] = None,
  @Key("bb") boundingbox: Option[BoundingBox] = None
) extends Ordered[GeocodeRecord] {
  def featureIds = ids.map(id => {
    val parts = id.split(":")
    StoredFeatureId(parts(0), parts(1))
  })

  lazy val woeType = YahooWoeType.findByValue(_woeType)
  
  def compare(that: GeocodeRecord): Int = {
    YahooWoeTypes.getOrdering(this.woeType) - YahooWoeTypes.getOrdering(that.woeType)
  }

  def toGeocodeServingFeature(): GeocodeServingFeature = {
    // geom
    val geometry = new FeatureGeometry(
      new GeocodePoint(lat, lng))

    boundingbox.foreach(bounds =>
      geometry.setBounds(new GeocodeBoundingBox(
        new GeocodePoint(bounds.ne.lat, bounds.ne.lng),
        new GeocodePoint(bounds.sw.lat, bounds.sw.lng)
      ))  
    )
    
    val feature = new GeocodeFeature(
      cc, geometry
    )

    feature.setWoeType(this.woeType)

    feature.setIds(featureIds.map(i => {
      new FeatureId(i.namespace, i.id)
    }))

    val filteredNames = displayNames.filterNot(n => List("post", "link").contains(n.lang))

    feature.setNames(filteredNames.map(name => {
      var flags: List[FeatureNameFlags] = Nil
      if (name.lang == "abbr") {
        flags ::= FeatureNameFlags.ABBREVIATION
      }
      if (name.preferred) {
        flags ::= FeatureNameFlags.PREFERRED
      }

      val fname = new FeatureName(name.name, name.lang)
      if (flags.nonEmpty) {
        fname.setFlags(flags)
      }
      fname
    }))

    val scoring = new ScoringFeatures()
    boost.foreach(b => scoring.setBoost(b))
    population.foreach(p => scoring.setPopulation(p))
    scoring.setParents(parents)
    
    val servingFeature = new GeocodeServingFeature()
    servingFeature.setId(_id.toString)
    servingFeature.setScoringFeatures(scoring)
    servingFeature.setFeature(feature)

    servingFeature
  }

  def isCountry = woeType == YahooWoeType.COUNTRY
  def isPostalCode = woeType == YahooWoeType.POSTAL_CODE
}

class GeocodeStorageFutureReadService(underlying: GeocodeStorageReadService, future: FuturePool) {
  def getByName(name: String): Future[Iterator[GeocodeServingFeature]] = future {
    underlying.getByName(name)
  }

  def getByObjectIds(ids: Seq[ObjectId]): Future[Map[ObjectId, GeocodeServingFeature]] = future {
    underlying.getByObjectIds(ids)
  }
}

trait GeocodeStorageReadService {
  def getByName(name: String): Iterator[GeocodeServingFeature]
  def getByObjectIds(ids: Seq[ObjectId]): Map[ObjectId, GeocodeServingFeature]
}

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