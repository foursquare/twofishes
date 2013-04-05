// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import collection.JavaConverters._
import com.foursquare.twofishes.importers.geonames._
import com.foursquare.twofishes.util.{GeometryUtils, StoredFeatureId}
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.bson.types.ObjectId
import org.specs2.mutable._
import scala.collection.mutable.{HashMap, ListBuffer}

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
  def addPolygonToRecord(id: StoredFeatureId, wkbGeometry: Array[Byte]) {}
  def addBoundingBoxToRecord(id: StoredFeatureId, bbox: BoundingBox) {}
  def addSlugToRecord(id: StoredFeatureId, slug: String) {}
  def getById(id: StoredFeatureId): Iterator[GeocodeRecord] = Nil.iterator
  def addNameIndex(name: NameIndex) {}
}

class IndexerSpec extends Specification {
  var store = new MockGeocodeStorageWriteService()
  val slugIndexer = new SlugIndexer()
  val parser = new GeonamesParser(store, slugIndexer, Map.empty)

  "processFeatureName" in {
    val names = parser.processFeatureName("US", "en", "New York", false, false)
    names.size mustEqual 1
  }

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

  "ARRONDISSEMENT rewrites work" in {
    val (deaccentedNames, otherModifiedNames) = parser.rewriteNames(
      List("9eme"))
    val names = deaccentedNames ++ otherModifiedNames
    names.size aka names.toString mustEqual 2
    names must contain("9E ARRONDISSEMENT")
    names must contain("9th ARRONDISSEMENT")
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

  "calculateCoverForRecord must not clip noho geometry or think it's full" in {
    val indexer = new RevGeoIndexer("unused", new FidMap(false))
    val out = new HashMap[Long, ListBuffer[CellGeometry]]
    val geomText =
      "POLYGON ((-73.99679999999995 40.72540600000008," +
      "-73.99633399999993 40.72594600000008, -73.99552899999992 40.72689200000008, " +
      "-73.9946559999999 40.72794200000004, -73.99337199999991 40.729468000000054, " +
      "-73.99242399999991 40.730579000000034, -73.9913049999999 40.730120000000056, " +
      "-73.9905589999999 40.72980400000006, -73.9901109999999 40.72962700000005, " +
      "-73.98989599999993 40.72955200000007, -73.99037799999991 40.72888100000006, " +
      "-73.99120599999992 40.72769800000009, -73.9912369999999 40.727654000000086, " +
      "-73.99140399999993 40.72738600000008, -73.99147199999993 40.72725600000007, " +
      "-73.99163599999991 40.726879000000054, -73.99193699999995 40.72599100000008, " +
      "-73.9922929999999 40.72500800000006, -73.99260699999991 40.72410800000006, " +
      "-73.99282299999993 40.72418300000004, -73.99396699999994 40.72458900000004, " +
      "-73.99412099999995 40.724646000000064, -73.99440699999991 40.72470800000008, " +
      "-73.99451299999993 40.724740000000054, -73.99529599999994 40.72502500000007, " +
      "-73.9953789999999 40.72505000000007, -73.99551999999994 40.72509400000007, " +
      "-73.99592399999995 40.725200000000086, -73.99665499999992 40.72536900000006, " +
      "-73.99679999999995 40.72540600000008))"
    val geomBytes = (new WKBWriter).write((new WKTReader).read(geomText))
    val record = GeocodeRecord(
      ids = List("nohoId"),
      names = List("noho"),
      cc = "US",
      _woeType = YahooWoeType.SUBURB.getValue,
      lat = 40.727343,
      lng = -73.993347,
      displayNames = Nil,
      parents = Nil,
      population = None,
      polygon = Some(geomBytes),
      hasPoly = Some(true))

    indexer.calculateCoverForRecord(record, out, new HashMap[Long, Geometry])

    val wktWriter = new WKTWriter
    val wkbReader = new WKBReader
    out.foreach({
      case (cellid, cells) => cells.foreach(cell => {
        cell.isFull must_== false
        wktWriter.write(wkbReader.read(cell.wkbGeometry.array())) must_== wktWriter.write(wkbReader.read(geomBytes))
      })
    })
  }


  // "deletes work in practice" in {
  //   parser.parseAlternateNamesLine(
  //     "2727895\t5391997\ten\tSan Francisco County\t1", 0
  //   )

  //   val names = store.nameMap(StoredFeatureId("geonameid", "5391997"))
  //   names.size mustEqual 2
  //   names.exists(_.name == "San Francisco County") mustEqual true
  //   names.exists(_.name == "San Francisco") mustEqual true
  // }

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
