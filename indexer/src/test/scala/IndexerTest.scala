// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.importers.geonames._
import com.twitter.util.Future
import collection.JavaConverters._
import org.specs2.mutable._
import org.bson.types.ObjectId
import scala.collection.mutable.HashMap

class MockGeocodeStorageWriteService extends GeocodeStorageWriteService {
  val nameMap = new HashMap[StoredFeatureId, List[DisplayName]]

  def insert(record: GeocodeRecord): Unit = {}
  def setRecordNames(id: StoredFeatureId, names: List[DisplayName]) {}
  def addNameToRecord(name: DisplayName, id: StoredFeatureId) {
    if (!nameMap.contains(id)) {
      nameMap(id) = Nil
    }

    nameMap(id) = name :: nameMap(id)
  }
  def addBoundingBoxToRecord(id: StoredFeatureId, bbox: BoundingBox) {}
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord] = Nil.iterator
  def addNameIndex(name: NameIndex) {}
}

class IndexerSpec extends Specification {
  var store = new MockGeocodeStorageWriteService()
  val parser = new GeonamesParser(store)

  "name deduping works" in {
    val record = GeocodeRecord(new ObjectId(),
      List("geonameid:1"),
      Nil, "", 0, 0.0, 0.0,
      List(
        DisplayName("en", "San Francisco County", 1),
        DisplayName("en", "San Francisco County", 1),
        DisplayName("en", "San Francisco County", 0),
        DisplayName("en", "San Francisco", 0)
      ),
      Nil, None)

    val feature = record.toGeocodeServingFeature.feature

    feature.names.size aka feature.names.toString mustEqual 3
  }

  "rewrites work" in {
    val (deaccentedNames, otherModifiedNames) = parser.rewriteNames(
      List("Mount Laurel", "North Bergen"))
    val names = deaccentedNames ++ otherModifiedNames
    names.size aka names.toString mustEqual 4
    names must contain("Mt Laurel")
    names must contain("Mtn Laurel")
    names must contain("Mountain Laurel")
    names must contain("N Bergen")
  }

 "long rewrites work" in {
    val (deaccentedNames, otherModifiedNames) =
      parser.rewriteNames(List("Griffiss Air Force Base"))
    val names = deaccentedNames ++ otherModifiedNames
    names.size aka names.toString mustEqual 1
    names must contain("Griffiss AFB")
  }
  
  "deletes work" in {
    val (deaccentedNames, otherModifiedNames) =
      parser.rewriteNames(List("Cook County", "Township of Brick"))
    val names = deaccentedNames ++ otherModifiedNames
    names.size aka names.toString mustEqual 5
    names must contain("Cook")
    names must contain("Brick")
    names must contain("of Brick")
    names must contain("Charter Township of Brick")
    names must contain("Twp of Brick")
  }

  "deletes and rewrites work" in {
    val (deaccentedNames, otherModifiedNames) =
      parser.rewriteNames(List("Saint Ferdinand Township"))
    val names = deaccentedNames ++ otherModifiedNames
    names must contain("St Ferdinand")
    names must contain("Saint Ferdinand")
  }


  "deletes work in practice" in {
    parser.parseAlternateNamesLine(
      "2727895\t5391997\ten\tSan Francisco County\t1", 0
    )

    val names = store.nameMap(StoredFeatureId("geonameid", "5391997"))
    names.size mustEqual 2
    names.exists(_.name == "San Francisco County") mustEqual true
    names.exists(_.name == "San Francisco") mustEqual true
  }

  // "deletes work in practice  -- county" in {
  //   val record = parser.parseFeature(
  //     new GeonamesFeature(Map(
  //       GeonamesFeatureColumns.LATITUDE -> "40.74",
  //       GeonamesFeatureColumns.LONGITUDE -> "-74",
  //       GeonamesFeatureColumns.NAME -> "San Francisco County",
  //       GeonamesFeatureColumns.GEONAMEID -> "1"
  //     ))
  //   )

  //   record.names mustEqual List(
  //     "san francisco county",
  //     "san francisco"
  //   )
  // }

  // "deaccents work" in {
  //   val record = parser.parseFeature(
  //     new GeonamesFeature(Map(
  //       GeonamesFeatureColumns.LATITUDE -> "40.74",
  //       GeonamesFeatureColumns.LONGITUDE -> "-74",
  //       GeonamesFeatureColumns.NAME -> "Ōsaka",
  //       GeonamesFeatureColumns.GEONAMEID -> "1"
  //     ))
  //   )

  //   record.names mustEqual List(
  //     "ōsaka",
  //     "osaka"
  //   )

  //   record.displayNames must contain(
  //       DisplayName("en", "Ōsaka", 0)
  //   )
  //   record.displayNames must contain(
  //       DisplayName("alias", "Osaka", 0)
  //   )
  // }

}
