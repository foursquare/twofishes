package com.foursquare.twofishes.util

import org.bson.types.ObjectId
import org.specs2.mutable.Specification

class StoredFeatureIdSpec extends Specification {
  def testConversion(humStr: String, nsId: Long, longId: Long, oid: ObjectId, ns: FeatureNamespace) = {
    val fidOpt = StoredFeatureId.fromHumanReadableString(humStr)
    val fid = fidOpt.get

    fid.longId must_== longId
    fid.legacyObjectId must_== oid
    fid.humanReadableString must_== humStr
    fid.namespace must_== ns
    fid.namespaceSpecificId must_== nsId

    StoredFeatureId(fid.namespace, fid.namespaceSpecificId) must_== fid
    StoredFeatureId.fromLong(fid.longId) must_== fidOpt
    StoredFeatureId.fromLegacyObjectId(fid.legacyObjectId) must_== fidOpt
  }

  "featureids" should {
    "maponics conversion" in {
      testConversion("maponics:1234", 1234, 1234, new ObjectId("0000000000000000000004d2"), MaponicsNamespace)
    }

    "geonames conversion" in {
      testConversion("geonameid:1234", 1234, 1234 + (1L << 56), new ObjectId("0000000000000001000004d2"),
        GeonamesNamespace)
    }

    "geonameszip conversion" in {
      testConversion(
        "geonamezip:US-10003",
        18295873488277779L,
        18295873488277779L + (2L << 56),
        new ObjectId("0000000002410000001fd113"),
        GeonamesZipNamespace)

      testConversion(
        "geonamezip:JP-100-0001",
        9570263698809793L,
        9570263698809793L + (2L << 56),
        new ObjectId("000000000222001aa82ca3c1"),
        GeonamesZipNamespace)

      testConversion(
        "geonamezip:FR-68968 CEDEX 9",
        5067260887862605L,
        5067260887862605L + (2L << 56),
        new ObjectId("00000000021200a59d348d4d"),
        GeonamesZipNamespace)
    }

    "geonameszip long construction" in {
      (new GeonamesZip("US-10003")).longId must_==
        ((GeonamesZipNamespace.id.toLong << 56) +
         (GeonamesZip.supportedCountries.indexOf("US").toLong << 48) +
         (1 * math.pow(38, 4).toLong) +
         (0 * math.pow(38, 3).toLong) +
         (0 * math.pow(38, 2).toLong) +
         (0 * math.pow(38, 1).toLong) +
         (3 * math.pow(38, 0).toLong))
    }

    "geonameszip long conversion" in {
      def testLongConversion(cc: String, postalCode: String) = {
        val lng = GeonamesZip.convertToLong(cc, postalCode)
        lng must_== (lng << 8) >> 8
        GeonamesZip.convertFromLong(lng) must_== (cc, postalCode)
      }

      testLongConversion("US", "10003")
      testLongConversion("JP", "100-0001")
      testLongConversion("FR", "68968 CEDEX 9")
      testLongConversion("FR", "68968 CEDEX")
      testLongConversion("FR", "68968 CITYSSIMO")
      testLongConversion("FR", "68968 SP 7")
    }
  }
}
