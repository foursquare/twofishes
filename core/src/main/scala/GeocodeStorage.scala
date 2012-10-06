// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.twitter.util.{Future, FuturePool}
import org.bson.types.ObjectId
import scala.collection.JavaConversions._

object Implicits {
  implicit def fidToString(fid: StoredFeatureId): String = fid.toString
  implicit def fidListToString(fids: List[StoredFeatureId]): List[String] = fids.map(_.toString)
}

case class DisplayName(
  lang: String,
  name: String,
  flags: Int,
  _id: ObjectId = new ObjectId()
)

case class StoredFeatureId(
  namespace: String,
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
  _woeType: Int,
  lat: Double,
  lng: Double,
  displayNames: List[DisplayName],
  parents: List[String],
  population: Option[Int],
  boost: Option[Int] = None,
  boundingbox: Option[BoundingBox] = None
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

    val filteredNames: List[DisplayName] = displayNames.filterNot(n => List("post", "link").contains(n.lang))
    var hackedNames: List[DisplayName] = Nil

    // HACK(blackmad)
    if (this.woeType == YahooWoeType.ADMIN1 && cc == "JP") {
      hackedNames ++= 
        filteredNames.filter(n => n.lang == "en" || n.lang == "" || n.lang == "alias")
          .map(n => DisplayName(n.lang, n.name + " Prefecture", FeatureNameFlags.ALIAS.getValue))
    }

    if (this.woeType == YahooWoeType.TOWN && cc == "TW") {
      hackedNames ++= 
        filteredNames.filter(n => n.lang == "en" || n.lang == "" || n.lang == "alias")
          .map(n => DisplayName(n.lang, n.name + " County", FeatureNameFlags.ALIAS.getValue))
    }

    val allNames = filteredNames ++ hackedNames

    feature.setNames(allNames.map(name => {
      var flags: List[FeatureNameFlags] = Nil
      if (name.lang == "abbr") {
        flags ::= FeatureNameFlags.ABBREVIATION
      }

      FeatureNameFlags.values.foreach(v => {
        if ((v.getValue() & name.flags) > 0) {
          flags ::= v
        }
      })

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

trait GeocodeStorageFutureReadService {
  def getIdsByNamePrefix(name: String): Future[Seq[ObjectId]]
  def getIdsByName(name: String): Future[Seq[ObjectId]]
  def getByName(name: String): Future[Seq[GeocodeServingFeature]]
  def getByObjectIds(ids: Seq[ObjectId]): Future[Map[ObjectId, GeocodeServingFeature]]
}

class WrappedGeocodeStorageFutureReadService(underlying: GeocodeStorageReadService, future: FuturePool) extends GeocodeStorageFutureReadService {
  def getIdsByNamePrefix(name: String): Future[Seq[ObjectId]] = future {
    underlying.getIdsByNamePrefix(name)
  }

  def getIdsByName(name: String): Future[Seq[ObjectId]] = future {
    underlying.getIdsByName(name)
  }

  def getByName(name: String): Future[Seq[GeocodeServingFeature]] = future {
    underlying.getByName(name)
  }

  def getByObjectIds(ids: Seq[ObjectId]): Future[Map[ObjectId, GeocodeServingFeature]] = future {
    underlying.getByObjectIds(ids)
  }
}

trait GeocodeStorageReadService {
  def getIdsByName(name: String): Seq[ObjectId]
  def getIdsByNamePrefix(name: String): Seq[ObjectId]
  def getByName(name: String): Seq[GeocodeServingFeature]
  def getByObjectIds(ids: Seq[ObjectId]): Map[ObjectId, GeocodeServingFeature]
}