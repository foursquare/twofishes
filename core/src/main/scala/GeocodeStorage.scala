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

class DisplayNameOrdering(lang: Option[String], preferAbbrev: Boolean) extends Ordering[DisplayName] {
  def compare(a: DisplayName, b: DisplayName) = {
    scoreName(b) - scoreName(a)
  }

  def scoreName(name: DisplayName): Int = {
    var score = 0
    if (name.preferred) {
      score += 1
    }
    if (lang.exists(_ == name.lang)) {
      score += 2
    }
    if (name.lang == "abbr" && preferAbbrev) {
      score += 4
    }
    score
  }
}

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

  def bestName(lang: Option[String], preferAbbrev: Boolean): Option[DisplayName] = {
    displayNames.sorted(new DisplayNameOrdering(lang, preferAbbrev)).headOption
  }

  def toGeocodeFeature(parentMap: Map[String, GeocodeRecord],
      full: Boolean = false,
      lang: Option[String] = None): GeocodeFeature = {
    val myBestName = bestName(lang, false)

    val parentFeatures = parents.flatMap(pid => parentMap.get(pid)).filterNot(_.isCountry).sorted

    val displayName = (List(myBestName) ++ parentFeatures.map(_.bestName(lang, true)))
      .flatMap(_.map(_.name)).mkString(", ")

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

    feature.setName(myBestName.map(_.name).getOrElse(""))
    feature.setDisplayName(displayName)
    feature.setWoeType(this.woeType)

    feature.setId(_id.toString)

    feature.setIds(featureIds.map(i => {
      new FeatureId(i.namespace, i.id)
    }))

    if (full) {
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
    }

    val scoring = new ScoringFeatures()
    boost.foreach(b => scoring.setBoost(b))
    population.foreach(p => scoring.setPopulation(p))
    scoring.setParents(parents)
    feature.setScoringFeatures(scoring)

    feature
  }

  def isCountry = woeType == YahooWoeType.COUNTRY
  def isPostalCode = woeType == YahooWoeType.POSTAL_CODE
}

class GeocodeStorageFutureReadService(underlying: GeocodeStorageReadService, future: FuturePool) {
  def getByName(name: String): Future[Iterator[GeocodeFeature]] = future {
    underlying.getByName(name)
  }

  def getByObjectIds(ids: Seq[ObjectId]): Future[Map[ObjectId, GeocodeFeature]] = future {
    underlying.getByObjectIds(ids)
  }
}

trait GeocodeStorageReadService {
  def getByName(name: String): Iterator[GeocodeFeature]
  def getByObjectIds(ids: Seq[ObjectId]): Map[ObjectId, GeocodeFeature]
}

// fix parse ordering/hydration
// fix insert
// fix in-memory
// build index
// test


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

import com.mongodb.MongoOptions
import com.mongodb.ServerAddress

object MongoOpts {
  val mo = new MongoOptions()
  mo.connectionsPerHost = 200000
  mo.threadsAllowedToBlockForConnectionMultiplier = 100
}

object MongoGeocodeDAO extends SalatDAO[GeocodeRecord, ObjectId](
  collection = MongoConnection(new ServerAddress("localhost"), MongoOpts.mo)("geocoder")("features"))

object NameIndexDAO extends SalatDAO[NameIndex, String](
  collection = MongoConnection()("geocoder")("name_index"))

object FidIndexDAO extends SalatDAO[FidIndex, String](
  collection = MongoConnection()("geocoder")("fid_index"))

class MongoGeocodeStorageService extends GeocodeStorageWriteService {
  def getByName(name: String): Iterator[GeocodeRecord] = {
    val fids: List[String] = NameIndexDAO.find(MongoDBObject("_id" -> name)).flatMap(_.fids).toList
    val oids: List[ObjectId] = FidIndexDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> fids))).map(_.oid).toList
    MongoGeocodeDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> oids.toList)))
  }

  def getById(id: StoredFeatureId): Iterator[GeocodeRecord] = {
    MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> List(id.toString))))
  }

  def getByObjectIds(ids: Seq[ObjectId]): Iterator[GeocodeRecord] = {
    MongoGeocodeDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> ids.toList)))
  }

  def getByIds(ids: Seq[String]): Iterator[GeocodeRecord] = {
    MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> ids.toList)))
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