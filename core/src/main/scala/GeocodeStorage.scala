// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.util.StoredFeatureId
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import com.vividsolutions.jts.io.WKBReader
import java.nio.ByteBuffer
import org.apache.thrift.{TBaseHelper, TDeserializer, TSerializer}
import org.apache.thrift.protocol.TCompactProtocol
import org.bson.types.ObjectId
import scala.collection.mutable.HashMap
import scalaj.collection.Implicits._

class SlugEntryMap extends HashMap[String, SlugEntry]

case class SlugEntry(
  id: String,
  score: Int,
  var deprecated: Boolean = false,
  permanent: Boolean = false
) {
  override def toString(): String = {
    "%s\t%s\t%s".format(id, score, deprecated)
  }
}

case class DisplayName(
  lang: String,
  name: String,
  flags: Int,
  _id: ObjectId = new ObjectId()
)

case class Point(lat: Double, lng: Double)

case class BoundingBox(
  ne: Point,
  sw: Point
)

object GeoJsonPoint {
  def apply(lat: Double, lng: Double): GeoJsonPoint =
    GeoJsonPoint(coordinates = List(lng, lat))
}
case class GeoJsonPoint(
  `type`: String = "Point",
  coordinates: List[Double]
)
object NilPoint extends GeoJsonPoint("Point", List(0, 0))

case class GeocodeRecord(
  _id: Long,
  ids: List[Long],
  names: List[String],
  cc: String,
  _woeType: Int,
  lat: Double,
  lng: Double,
  displayNames: List[DisplayName] = Nil,
  parents: List[Long] = Nil,
  population: Option[Int] = None,
  boost: Option[Int] = None,
  boundingbox: Option[BoundingBox] = None,
  displayBounds: Option[BoundingBox] = None,
  canGeocode: Boolean = true,
  slug: Option[String] = None,
  polygon: Option[Array[Byte]] = None,
  hasPoly: Boolean = false,
  var attributes: Option[Array[Byte]] = None,
  extraRelations: List[Long] = Nil,
  var loc: GeoJsonPoint = NilPoint
) extends Ordered[GeocodeRecord] {
  // gross that we overwrite this
  loc = GeoJsonPoint(lat, lng)

  val factory = new TCompactProtocol.Factory()
  val serializer = new TSerializer(factory)
  val deserializer = new TDeserializer(factory)

  def setAttributes(attr: Option[GeocodeFeatureAttributes]) {
    attributes = attr.map(a => serializer.serialize(a))
  }

  def getAttributes(): Option[GeocodeFeatureAttributes] = {
    attributes.map(bytes => {
      val attr = new RawGeocodeFeatureAttributes()
      deserializer.deserialize(attr, bytes)
      attr
    })
  }

  def featureId: StoredFeatureId = StoredFeatureId.fromLong(_id).getOrElse(
    throw new RuntimeException("can't convert %s to a StoredFeatureId".format(_id)))

  def featureIds: List[StoredFeatureId] = ids.flatMap(StoredFeatureId.fromLong _)

  def parentFeatureIds: List[StoredFeatureId] = parents.flatMap(StoredFeatureId.fromLong _)

  lazy val woeType = YahooWoeType.findByIdOrNull(_woeType)

  def center = {
    val geomFactory = new GeometryFactory()
    geomFactory.createPoint(new Coordinate(lng, lat))
  }

  def compare(that: GeocodeRecord): Int = {
    YahooWoeTypes.getOrdering(this.woeType) - YahooWoeTypes.getOrdering(that.woeType)
  }

  def fixRomanianName(s: String) = {
    val romanianTranslationTable = List(
      // cedilla -> comma
      "Ţ" -> "Ț",
      "Ş" -> "Ș",
      // tilde and caron to breve
      "Ã" -> "Ă",
      "Ǎ" -> "Ă"
    ).flatMap({case (from, to) => {
      List(
        from.toLowerCase -> to.toLowerCase,
        from.toUpperCase -> to.toUpperCase
      )
    }})

    var newS = s
    romanianTranslationTable.foreach({case (from, to) => {
      newS = newS.replace(from, to)
    }})
    newS
  }

  def toGeocodeServingFeature(): GeocodeServingFeature = {
    // geom
    val geometryBuilder = FeatureGeometry.newBuilder
      .center(GeocodePoint(lat, lng))

    if (polygon.isEmpty) {
      boundingbox.foreach(bounds => {
        val currentBounds = (bounds.ne.lat, bounds.ne.lng, bounds.sw.lat, bounds.sw.lng)

        // This breaks at 180, I get that, to fix.
        val finalBounds = (
          List(bounds.ne.lat, bounds.sw.lat).max,
          List(bounds.ne.lng, bounds.sw.lng).max,
          List(bounds.ne.lat, bounds.sw.lat).min,
          List(bounds.ne.lng, bounds.sw.lng).min
        )

        if (finalBounds != currentBounds) {
          println("incorrect bounds %s -> %s".format(currentBounds, finalBounds))
        }

        geometryBuilder.bounds(GeocodeBoundingBox(
          GeocodePoint(finalBounds._1, finalBounds._2),
          GeocodePoint(finalBounds._3, finalBounds._4)
        ))
      })
    }

    displayBounds.foreach(bounds => {
      val currentBounds = (bounds.ne.lat, bounds.ne.lng, bounds.sw.lat, bounds.sw.lng)

      // This breaks at 180, I get that, to fix.
      val finalBounds = (
        List(bounds.ne.lat, bounds.sw.lat).max,
        List(bounds.ne.lng, bounds.sw.lng).max,
        List(bounds.ne.lat, bounds.sw.lat).min,
        List(bounds.ne.lng, bounds.sw.lng).min
      )

      if (finalBounds != currentBounds) {
        println("incorrect bounds %s -> %s".format(currentBounds, finalBounds))
      }

      geometryBuilder.displayBounds(GeocodeBoundingBox(
        GeocodePoint(finalBounds._1, finalBounds._2),
        GeocodePoint(finalBounds._3, finalBounds._4)
      ))
    })

    polygon.foreach(poly => {
      geometryBuilder.wkbGeometry(ByteBuffer.wrap(poly))

      val wkbReader = new WKBReader()
      val g = wkbReader.read(poly)

      val envelope = g.getEnvelopeInternal()

      geometryBuilder.bounds(GeocodeBoundingBox(
        GeocodePoint(envelope.getMaxY(), envelope.getMaxX()),
        GeocodePoint(envelope.getMinY(), envelope.getMinX())
      ))
    })

    val allNames: List[DisplayName] = displayNames.filterNot(n => List("post", "link").contains(n.lang))

    val nameCandidates = allNames.map(name => {
      var flags: List[FeatureNameFlags] = Nil
      if (name.lang == "abbr") {
        flags ::= FeatureNameFlags.ABBREVIATION
      }

      FeatureNameFlags.values.foreach(v => {
        if ((v.getValue() & name.flags) > 0) {
          flags ::= v
        }
      })

      FeatureName.newBuilder.name(name.name).lang(name.lang)
        .applyIf(flags.nonEmpty, _.flags(flags))
        .result
    })

    var finalNames = nameCandidates
      .groupBy(n => "%s%s".format(n.lang, n.name))
      .flatMap({case (k,values) => {
        var allFlags = values.flatMap(_.flags)

        // If we collapsed multiple names, and not all of them had ALIAS,
        // then we should strip off that flag because some other entry told
        // us it didn't deserve to be ranked down
        if (values.size > 1 && values.exists(n => !n.flags.has(FeatureNameFlags.ALIAS))) {
          allFlags = allFlags.filterNot(_ =? FeatureNameFlags.ALIAS)
        }

        values.headOption.map(_.copy(flags = allFlags.distinct))
      }})
      .map(n => {
        if (n.lang == "ro") {
          n.copy(
            name = fixRomanianName(n.name)
          )
        } else {
          n
        }
      })

    // Lately geonames has these stupid JP aliases, like "Katsuura Gun" for "Katsuura-gun"
    if (cc =? "JP" || cc =? "TH") {
      def isPossiblyBad(s: String): Boolean = {
        s.contains(" ") && s.split(" ").forall(_.headOption.exists(Character.isUpperCase))
      }

      def makeBetterName(s: String): String = {
        val parts = s.split(" ")
        val head = parts.headOption
        val rest = parts.drop(1)
        (head.toList ++ rest.map(_.toLowerCase)).mkString("-")
      }

      val enNames = finalNames.filter(_.lang == "en")
      enNames.foreach(n => {
        if (isPossiblyBad(n.name)) {
          finalNames = finalNames.filterNot(_.name =? makeBetterName(n.name))
        }
      })
    }

    val feature = GeocodeFeature.newBuilder
      .cc(cc)
      .geometry(geometryBuilder.result)
      .woeType(this.woeType)
      .ids(featureIds.map(_.thriftFeatureId))
      .id(featureIds.headOption.map(_.humanReadableString))
      .longId(featureIds.headOption.map(_.longId))
      .slug(slug)
      .names(finalNames.toSeq)
      .attributes(getAttributes())
      .result

    val scoringBuilder = ScoringFeatures.newBuilder
      .boost(boost)
      .population(population)
      .parentIds(parents)
      .applyIf(extraRelations.nonEmpty, _.extraRelationIds(extraRelations))
      .applyIf(hasPoly, _.hasPoly(hasPoly))

    if (!canGeocode) {
      scoringBuilder.canGeocode(false)
    }

    val scoring = scoringBuilder.result

    val servingFeature = GeocodeServingFeature.newBuilder
      .longId(featureId.longId)
      .scoringFeatures(scoring)
      .feature(feature)
      .result

    servingFeature
  }

  def isCountry = woeType == YahooWoeType.COUNTRY
  def isPostalCode = woeType == YahooWoeType.POSTAL_CODE
}

trait GeocodeStorageReadService {
  def getIdsByName(name: String): Seq[StoredFeatureId]
  def getIdsByNamePrefix(name: String): Seq[StoredFeatureId]
  def getByName(name: String): Seq[GeocodeServingFeature]
  def getByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, GeocodeServingFeature]

  def getBySlugOrFeatureIds(ids: Seq[String]): Map[String, GeocodeServingFeature]

  def getMinS2Level: Int
  def getMaxS2Level: Int
  def getLevelMod: Int
  def getByS2CellId(id: Long): Seq[CellGeometry]
  def getPolygonByFeatureId(id: StoredFeatureId): Option[Geometry]
  def getPolygonByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Geometry]

  def hotfixesDeletes: Seq[StoredFeatureId] = Nil
  def hotfixesBoosts: Map[StoredFeatureId, Int] = Map.empty
}
