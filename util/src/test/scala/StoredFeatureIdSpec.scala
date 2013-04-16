package com.foursquare.twofishes.util

import org.bson.types.ObjectId
import org.specs2.mutable.Specification

class StoredFeatureIdSpec extends Specification {
  "maponics conversion" in {
    val fidOpt = StoredFeatureId.fromHumanReadableString("maponics:1234")
    val fid = fidOpt.get
    fid.id must_== 1234
    fid.legacyObjectId must_== new ObjectId("0000000000000000000004d2")
    fid.humanReadableString must_== "maponics:1234"
    fid.namespace must_== MaponicsNamespace
    fid.namespaceSpecificId must_== 1234

    StoredFeatureId.fromLong(fid.id) must_== fidOpt
    StoredFeatureId.fromLegacyObjectId(fid.legacyObjectId) must_== fidOpt
    StoredFeatureId.fromNamespaceAndId(fid.namespace.name, fid.namespaceSpecificId.toString) must_== fidOpt
  }

  "geonames conversion" in {
    val fidOpt = StoredFeatureId.fromHumanReadableString("geonameid:1234")
    val fid = fidOpt.get
    fid.id must_== 1234L + (1L << 56)
    fid.legacyObjectId must_== new ObjectId("0000000000000001000004d2")
    fid.humanReadableString must_== "geonameid:1234"
    fid.namespace must_== GeonamesNamespace
    fid.namespaceSpecificId must_== 1234

    StoredFeatureId.fromLong(fid.id) must_== fidOpt
    StoredFeatureId.fromLegacyObjectId(fid.legacyObjectId) must_== fidOpt
    StoredFeatureId.fromNamespaceAndId(fid.namespace.name, fid.namespaceSpecificId.toString) must_== fidOpt
  }

  "from arbitrary string" in {
    StoredFeatureId.fromArbitraryString("geonameid:1234").get.humanReadableString must_== "geonameid:1234"
    StoredFeatureId.fromArbitraryString((1234L + (1L << 56)).toString).get.humanReadableString must_== "geonameid:1234"
    StoredFeatureId.fromArbitraryString("0000000000000001000004d2").get.humanReadableString must_== "geonameid:1234"
  }
}
