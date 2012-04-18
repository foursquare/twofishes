// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.importers.geonames._
import com.twitter.util.Future
import collection.JavaConverters._
import org.specs2.mutable._
import org.bson.types.ObjectId
import scala.collection.mutable.HashMap

class MockGeocodeStorageWriteService extends GeocodeStorageWriteService {
  def insert(record: GeocodeRecord): Unit = {}
  def setRecordNames(id: StoredFeatureId, names: List[DisplayName]) {}
  def addNameToRecord(name: DisplayName, id: StoredFeatureId) {}
  def addBoundingBoxToRecord(id: StoredFeatureId, bbox: BoundingBox) {}
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord] = Nil.iterator
}

class IndexerSpec extends Specification {
  val store = new MockGeocodeStorageWriteService()
  val parser = new GeonamesParser(store)

  "rewrites work" in {
    val names = parser.doRewrites(List("mount laurel", "north bergen"))
    names.size aka names.toString mustEqual 6
    names must contain("mt laurel")
    names must contain("mtn laurel")
    names must contain("mount laurel")
    names must contain("mountain laurel")
    names must contain("north bergen")
    names must contain("n bergen")
  }

 "long rewrites work" in {
    val names = parser.doRewrites(List("griffiss air force base"))
    names.size aka names.toString mustEqual 2
    names must contain("griffiss air force base")
    names must contain("griffiss afb")
  }
  
  "deletes work" in {
    val names = parser.doDeletes(List("cook county", "township of brick"))
    names.size aka names.toString mustEqual 5
    names must contain("cook county")
    names must contain("cook")
    names must contain("township of brick")    
    names must contain("brick")
    names must contain("of brick")
  }

  "deletes work in practice" in {
    val record = parser.parseFeature(
      new GeonamesFeature(Map(
        GeonamesFeatureColumns.LATITUDE -> "40.74",
        GeonamesFeatureColumns.LONGITUDE -> "-74",
        GeonamesFeatureColumns.NAME -> "Brick Township",
        GeonamesFeatureColumns.ALTERNATENAMES -> "Township of Brick"
      ))
    )

    // Yes, this is totally awful. awful awful.
    record.names mustEqual List(
      "brick township",
      "brick charter",
      "of brick",
      "charter township of brick",
      "brick charter charter township",
      "brick twp",
      "brick",
      "township of brick",
      "charter brick",
      "brick charter twp",
      "twp of brick",
      "brick charter township",
      "brick charter charter",
      "charter of brick")
  }
}
