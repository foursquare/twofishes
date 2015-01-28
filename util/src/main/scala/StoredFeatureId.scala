package com.foursquare.twofishes.util

import com.foursquare.twofishes._
import org.bson.types.ObjectId

sealed abstract class FeatureNamespace(val name: String, val id: Byte)
case object MaponicsNamespace extends FeatureNamespace("maponics", 0.toByte)
case object GeonamesNamespace extends FeatureNamespace("geonameid", 
  Option(System.getProperty("geonameidNamespace")).map(_.toInt).getOrElse(1).toByte)
case object GeonamesZipNamespace extends FeatureNamespace("geonamezip", 2.toByte)
case object AdHocNamespace extends FeatureNamespace("adhoc", 3.toByte)
case object WoeIdNamespace extends FeatureNamespace("woeid", 4.toByte)
case object OsmNamespace extends FeatureNamespace("osm", 5.toByte)
case object GettyNamespace extends FeatureNamespace("tgn", 6.toByte)

object FeatureNamespace {
  // higher is better
  val NamespaceOrdering = List(OsmNamespace, WoeIdNamespace, AdHocNamespace, GeonamesNamespace, MaponicsNamespace, GettyNamespace)

  val values = List(WoeIdNamespace, AdHocNamespace, GeonamesNamespace, MaponicsNamespace, GeonamesZipNamespace, OsmNamespace, GettyNamespace)

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
  // higher is better
  def getOrdering(): Int = FeatureNamespace.NamespaceOrdering.indexOf(namespace)

  def namespaceSpecificId: Long
  def longId: Long = {
    val lower56 = ((namespaceSpecificId << 8) >> 8)
    assert(lower56 == namespaceSpecificId)
    (namespace.id.toLong << 56) | lower56
  }

  def humanReadableString: String = namespace.name + ":" + namespaceSpecificId
  override def toString: String = humanReadableString

  def legacyObjectId: ObjectId = {
    val n = (namespace.id.toLong << 32) + namespaceSpecificId
    val bytes = BigInt(n).toByteArray
    val arr = bytes.reverse.padTo(12, 0: Byte).reverse
    new ObjectId(arr)
  }

  def thriftFeatureId: FeatureId = FeatureId(namespace.name, namespaceSpecificId.toString)
}

case class WoeId(override val namespaceSpecificId: Long) extends StoredFeatureId(WoeIdNamespace)
case class AdHocId(override val namespaceSpecificId: Long) extends StoredFeatureId(AdHocNamespace)
case class GeonamesId(override val namespaceSpecificId: Long) extends StoredFeatureId(GeonamesNamespace)
case class MaponicsId(override val namespaceSpecificId: Long) extends StoredFeatureId(MaponicsNamespace)
case class OsmId(override val namespaceSpecificId: Long) extends StoredFeatureId(OsmNamespace)
case class GettyId(override val namespaceSpecificId: Long) extends StoredFeatureId(GettyNamespace)

case class GeonamesZip(override val namespaceSpecificId: Long) extends StoredFeatureId(GeonamesZipNamespace) {
  def this(country: String, postalcode: String) = this(GeonamesZip.convertToLong(country, postalcode))
  def this(countryAndPostalCode: String) = this(GeonamesZip.convertToLong(countryAndPostalCode))

  val (country, postalCode) = GeonamesZip.convertFromLong(namespaceSpecificId)
  val countryAndPostalCode = "%s-%s".format(country, postalCode)

  // Override the default implementation of legacyObjectId because we actually
  // need the full 56 bits for the postal code id here.
  override def legacyObjectId: ObjectId = {
    val n = (namespace.id.toLong << 56) + namespaceSpecificId
    val bytes = BigInt(n).toByteArray
    val arr = bytes.reverse.padTo(12, 0: Byte).reverse
    new ObjectId(arr)
  }

  override def thriftFeatureId: FeatureId = FeatureId(namespace.name, countryAndPostalCode)
  override def humanReadableString = "%s:%s".format(namespace.name, countryAndPostalCode)
}

// The hackiest, easiest, laziest encoding of postal codes to 56 bits. It
// assumes that the country codes are all in this list, that all postal codes
// are at most 9 characters long, and that all characters in all postal codes
// are in the Base38 string.
object GeonamesZip {
  // This is the list of CC's that are represented in
  // data/downloaded/zip/allCountries.txt. If you ever update this, which may
  // be necessary when we get new geonames data, please ADD THINGS AT THE END
  // OF THE ARRAY so we leave the existing array indices alone.
  val supportedCountries: Array[String] = Array(
    "AD", "AR", "AS", "AT", "AU", "BD", "BE", "BG", "BR", "CA", "CH", "CZ",
    "DE", "DK", "DO", "ES", "FI", "FO", "FR", "GB", "GF", "GG", "GL", "GP",
    "GT", "GU", "GY", "HR", "HU", "IM", "IN", "IS", "IT", "JE", "JP", "LI",
    "LK", "LT", "LU", "MC", "MD", "MH", "MK", "MP", "MQ", "MX", "MY", "NL",
    "NO", "NZ", "PH", "PK", "PL", "PM", "PR", "PT", "RE", "RU", "SE", "SI",
    "SJ", "SK", "SM", "TH", "TR", "US", "VA", "VI", "YT", "ZA"
    // NEW COUNTRIES GO AT THE END
  )


  assert(supportedCountries.size < 256)

  val Base38 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ- "

  def convertToLong(countryAndPostalCode: String): Long = {
    val parts = countryAndPostalCode.split("-", 2)
    convertToLong(parts(0), parts(1))
  }

  def convertToLong(country: String, postalCode: String): Long = {
    val topByte = supportedCountries.indexOf(country).toByte

    if (topByte < 0) {
      throw new RuntimeException("Cannot encode postal code for unsupported country %s".format(country))
    }

    // Ridiculous hacks for french postal codes that are too long.
    val hackedPostalCode = {
      if (country == "FR") {
        if (postalCode.contains("CEDEX")) {
          postalCode.replaceAll(" CEDEX ?", "CE")
        } else if (postalCode.contains(" SP ")) {
          postalCode.replaceAll(" SP ", "SP")
        } else if (postalCode.contains("CITYSSIMO")) {
          postalCode.replace(" CITYSSIMO", "CI")
        } else {
          postalCode
        }
      } else {
        postalCode
      }
    }

    // 38^9 fits into the 48 bits we give ourselves.
    if (hackedPostalCode.size > 9) {
      throw new RuntimeException("Can only encode postal codes of size <= 9. '%s' is too long".format(postalCode))
    }

    if (hackedPostalCode.exists(ch => Base38.indexOf(ch) < 0)) {
      throw new RuntimeException("can only encode postal codes with A-Z, 0-9, ' ' and '-': '%s'".format(postalCode))
    }

    (topByte.toLong << 48) +
      hackedPostalCode.foldLeft(0L)((num: Long, char: Char) => (num * 38L) + Base38.indexOf(char))
  }

  def convertFromLong(longId: Long): (String, String) = {
    val cc = supportedCountries((longId >> 48).toByte)
    var postalCode = ""
    var lng = (longId << 16) >> 16
    while (lng != 0) {
      postalCode = Base38((lng % 38).toInt) + postalCode
      lng /= 38
    }
    // de-hack the stuff from convertToLong
    if (cc == "FR") {
      if (postalCode.endsWith("CE")) {
        (cc, postalCode.replace("CE", " CEDEX"))
      } else if (postalCode.contains("CE")) {
        (cc, postalCode.replace("CE", " CEDEX "))
      } else if (postalCode.contains("SP")) {
        (cc, postalCode.replace("SP", " SP "))
      } else if (postalCode.contains("CI")) {
        (cc, postalCode.replace("CI", " CITYSSIMO"))
      } else {
        (cc, postalCode)
      }
    } else {
      (cc, postalCode)
    }
  }
}

object StoredFeatureId {
  def apply(ns: FeatureNamespace, id: String): StoredFeatureId = ns match {
    // TODO(nsanch): this is terrible, please fix it
    case GeonamesZipNamespace => new GeonamesZip(id)
    case _ => apply(ns, id.toLong)
  }

  def apply(ns: FeatureNamespace, id: Long): StoredFeatureId = ns match {
    case GeonamesNamespace => GeonamesId(id)
    case GeonamesZipNamespace => new GeonamesZip(id)
    case MaponicsNamespace => MaponicsId(id)
    case AdHocNamespace => AdHocId(id)
    case WoeIdNamespace => WoeId(id)
    case OsmNamespace => OsmId(id)
    case GettyNamespace => GettyId(id)
  }

  private def fromNamespaceAndId(n: String, id: String): Option[StoredFeatureId] = {
    FeatureNamespace.fromNameOpt(n).map(ns => StoredFeatureId(ns, id))
  }

  def fromUserInputString(s: String): Option[StoredFeatureId] = {
    if (ObjectId.isValid(s)) {
      fromLegacyObjectId(new ObjectId(s))
    } else {
      Helpers.TryO(s.toLong).flatMap(fromLong _)
        .orElse(fromHumanReadableString(s))
    }
  }

  def fromHumanReadableString(s: String, defaultNamespace: Option[FeatureNamespace] = None): Option[StoredFeatureId] = {
    (s.split(":", 2).toList, defaultNamespace) match {
      case (key :: value :: Nil, _) => StoredFeatureId.fromNamespaceAndId(key, value)
      case (value :: Nil, Some(ns)) => Some(StoredFeatureId(ns, value))
      case _ => None
    }
  }

  def fromLegacyObjectId(oid: ObjectId): Option[StoredFeatureId] = {
    val encodedAsLegacyLong = BigInt(oid.toByteArray).toLong
    val maybeIdOfNamespace = (encodedAsLegacyLong >> 56).toByte
    if (maybeIdOfNamespace == GeonamesZipNamespace.id) {
      val idFromNamespace = (encodedAsLegacyLong << 8) >> 8
      Some(new GeonamesZip(idFromNamespace))
    } else {
      val idOfNamespace = (encodedAsLegacyLong >> 32)
      val idFromNamespace = (encodedAsLegacyLong << 32) >> 32
      FeatureNamespace.fromIdOpt(idOfNamespace.toByte).map(ns => StoredFeatureId(ns, idFromNamespace))
    }
  }

  def fromLong(fullId: Long): Option[StoredFeatureId] = {
    // top 8 bits
    val idOfNamespace = fullId >> 56
    // lower 56 bits
    val idFromNamespace = (fullId << 8) >> 8
    FeatureNamespace.fromIdOpt(idOfNamespace.toByte).map(ns => StoredFeatureId(ns, idFromNamespace))
  }

  def fromThriftFeatureId(t: FeatureId) = fromNamespaceAndId(t.source, t.id)
}
