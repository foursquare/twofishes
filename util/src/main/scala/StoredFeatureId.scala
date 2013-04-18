package com.foursquare.twofishes.util

import com.foursquare.twofishes._
import org.bson.types.ObjectId

sealed abstract class FeatureNamespace(val name: String, val id: Byte)
case object MaponicsNamespace extends FeatureNamespace("maponics", 0.toByte)
case object GeonamesNamespace extends FeatureNamespace("geonameid", 1.toByte)
case object GeonamesZipNamespace extends FeatureNamespace("geonamezip", 2.toByte)

object FeatureNamespace {
  val values = List(GeonamesNamespace, MaponicsNamespace, GeonamesZipNamespace)

  def fromId(id: Byte): FeatureNamespace = fromIdOpt(id).getOrElse(
    throw new RuntimeException("unrecognized feature namespace id '%d'".format(id))
  )

  def fromIdOpt(id: Byte): Option[FeatureNamespace] = values.find(_.id == id)

  def fromName(n: String): FeatureNamespace = fromNameOpt(n).getOrElse(
    throw new RuntimeException("unrecognized feature namespace name '%s'".format(n))
  )

  def fromNameOpt(n: String): Option[FeatureNamespace] = values.find(_.name == n)
}

sealed abstract class StoredFeatureId(val namespace: FeatureNamespace) {
  def namespaceSpecificId: Long
  def id: Long = {
    val lower56 = ((namespaceSpecificId << 8) >> 8)
    assert(lower56 == namespaceSpecificId)
    (namespace.id.toLong << 56) | lower56
  }

  def humanReadableString: String = "%s:%s".format(namespace.name, namespaceSpecificId)
  override def toString: String = humanReadableString

  def legacyObjectId: ObjectId = {
    val n = (namespace.id.toLong << 32) + namespaceSpecificId
    val bytes = BigInt(n).toByteArray
    val arr = bytes.reverse.padTo(12, 0: Byte).reverse
    new ObjectId(arr)
  }

  def thriftFeatureId: FeatureId = new FeatureId(namespace.name, namespaceSpecificId.toString)
}

case class GeonamesId(override val namespaceSpecificId: Long) extends StoredFeatureId(GeonamesNamespace)
case class MaponicsId(override val namespaceSpecificId: Long) extends StoredFeatureId(MaponicsNamespace)
// TODO(nsanch, blackmad): write the mapping from postalcode to int
case class GeonamesZip(countryAndPostalCode: String) extends StoredFeatureId(GeonamesZipNamespace) {
  def this(country: String, postalcode: String) = this("%s-%s".format(country, postalcode))

  override def namespaceSpecificId = throw new RuntimeException("no Long id's supported for GeonamesZip")
  override def legacyObjectId = new ObjectId
  override def thriftFeatureId: FeatureId = new FeatureId(namespace.name, countryAndPostalCode)
  override def humanReadableString = "%s:%s".format(namespace.name, countryAndPostalCode)
  override def id = throw new RuntimeException("unable to generate Long id for GeonamesZip feature")
}

object StoredFeatureId {
  def apply(ns: FeatureNamespace, id: String): StoredFeatureId = ns match {
    case GeonamesNamespace => GeonamesId(id.toLong)
    case GeonamesZipNamespace => GeonamesZip(id)
    case MaponicsNamespace => MaponicsId(id.toLong)
  }

  def fromNamespaceAndId(n: String, id: String): Option[StoredFeatureId] = {
    FeatureNamespace.fromNameOpt(n).map(ns => StoredFeatureId(ns, id))
  }

  def fromArbitraryString(s: String): Option[StoredFeatureId] = {
    if (ObjectId.isValid(s)) {
      fromLegacyObjectId(new ObjectId(s))
    } else {
      Helpers.TryO(s.toLong).flatMap(fromLong _)
        .orElse(fromHumanReadableString(s))
    }
  }

  def fromHumanReadableString(s: String, defaultNamespace: Option[FeatureNamespace] = None): Option[StoredFeatureId] = {
    (s.split(":").toList, defaultNamespace) match {
      case (key :: value :: Nil, _) => FeatureNamespace.fromNameOpt(key).map(ns => StoredFeatureId(ns, value))
      case (value :: Nil, Some(ns)) => Some(StoredFeatureId(ns, value))
      case _ => None
    }
  }

  def fromLegacyObjectId(oid: ObjectId): Option[StoredFeatureId] = {
    val encodedAsLegacyLong = BigInt(oid.toByteArray).toLong
    val idOfNamespace = (encodedAsLegacyLong >> 32)
    val idFromNamespace = (encodedAsLegacyLong << 32) >> 32
    FeatureNamespace.fromIdOpt(idOfNamespace.toByte).map(ns => StoredFeatureId(ns, idFromNamespace.toString))
  }

  def fromLong(fullId: Long): Option[StoredFeatureId] = {
    // top 8 bits
    val idOfNamespace = fullId >> 56
    // lower 56 bits
    val idFromNamespace = (fullId << 8) >> 8
    FeatureNamespace.fromIdOpt(idOfNamespace.toByte).map(ns => StoredFeatureId(ns, idFromNamespace.toString))
  }

  def fromThriftFeatureId(t: FeatureId) = fromNamespaceAndId(t.source, t.id)
}
