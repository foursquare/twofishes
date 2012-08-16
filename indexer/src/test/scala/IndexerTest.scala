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
    val names = parser.doRewrites(List("Mount Laurel", "North Bergen"))
    names.size aka names.toString mustEqual 6
    names must contain("Mount Laurel")
    names must contain("Mt Laurel")
    names must contain("Mtn Laurel")
    names must contain("Mountain Laurel")
    names must contain("North Bergen")
    names must contain("N Bergen")
  }

 "long rewrites work" in {
    val names = parser.doRewrites(List("Griffiss Air Force Base"))
    names.size aka names.toString mustEqual 2
    names must contain("Griffiss AFB")
    names must contain("Griffiss Air Force Base")
  }
  
  "deletes work" in {
    val names = parser.doDeletes(List("Cook County", "Township of Brick"))
    names.size aka names.toString mustEqual 5
    names must contain("Cook")
    names must contain("Cook County")
    names must contain("Brick")
    names must contain("of Brick")
    names must contain("Township of Brick")
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
    record.names.toList must haveTheSameElementsAs(List(
      "brick charter township",
      "township of brick",
      "charter township of brick",
      "brick township",
      "twp of brick",
      "brick twp",
      "of brick",
      "brick"
    ))
  }

  "deletes work in practice  -- county" in {
    val record = parser.parseFeature(
      new GeonamesFeature(Map(
        GeonamesFeatureColumns.LATITUDE -> "40.74",
        GeonamesFeatureColumns.LONGITUDE -> "-74",
        GeonamesFeatureColumns.NAME -> "San Francisco County"
      ))
    )

    record.names mustEqual List(
      "san francisco county",
      "san francisco"
    )
  }

}
