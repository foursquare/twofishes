// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder

import collection.JavaConverters._
import com.twitter.util.FuturePool
import java.util.concurrent.Executors
import org.specs2.mutable._
import scala.collection.mutable.HashMap

class MockGeocodeStorageReadService extends GeocodeStorageReadService {
  val nameMap = new HashMap[String, List[GeocodeRecord]]
  val idMap = new HashMap[String, List[GeocodeRecord]]

  def addName(name: String, record: GeocodeRecord) {
    nameMap(name) = record :: nameMap.getOrElse(name, Nil)
  }

  def addId(id: String, record: GeocodeRecord) {
    idMap(id) = record:: idMap.getOrElse(id, Nil)
  }

  var id = 0
  def addGeocode(
    name: String,
    parents: List[GeocodeRecord],
    lat: Double,
    lng: Double,
    woeType: YahooWoeType,
    population: Option[Int] = None
  ): GeocodeRecord = {
    val idStr = "test:%d".format(id)
    val record = GeocodeRecord(
      ids = List(idStr),
      cc = "US",
      _woeType = woeType.getValue,
      lat = lat,
      lng = lng,
      displayNames = List(DisplayName("en", name, true)),
      parents = parents.flatMap(_.ids),
      names = List(name.toLowerCase),
      population = population
    )

    addName(name.toLowerCase, record)
    addId(idStr, record)

    id += 1
    record

  }

  // implementation methods
  def getByName(name: String): Iterator[GeocodeRecord] =
    nameMap.getOrElse(name, Nil).iterator

  def getByIds(ids: Seq[String]): Iterator[GeocodeRecord] =
    ids.flatMap(id => idMap.getOrElse(id.toString, Nil)).iterator

  def getById(id: StoredFeatureId): Iterator[GeocodeRecord] = 
    idMap.getOrElse(id.toString, Nil).iterator
}

class GeocoderSpec extends Specification {
  def addRegoPark(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val nyRecord = store.addGeocode("New York", List(usRecord), 3, 4, YahooWoeType.ADMIN1)
    val regoParkRecord = store.addGeocode("Rego Park", List(nyRecord, usRecord), 5, 6, YahooWoeType.TOWN)
    store
  }

  def buildRegoPark(): MockGeocodeStorageReadService = {
    addRegoPark(new MockGeocodeStorageReadService())
  }

  def addLosAngeles(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val caRecord = store.addGeocode("California", List(usRecord), 10, 11, YahooWoeType.ADMIN1)
    val losAngelesRecord = store.addGeocode("Los Angeles", List(caRecord, usRecord), 12, 13, YahooWoeType.TOWN)
    store
  }

  def buildLostAngeles(): MockGeocodeStorageReadService = {
    addLosAngeles(new MockGeocodeStorageReadService())
  }

  "one feature geocodes succeeds with matching data" in {
    val store = buildRegoPark()

    val mongoFuturePool = FuturePool(Executors.newFixedThreadPool(1))
    val r = new GeocoderImpl(mongoFuturePool, store).geocode(new GeocodeRequest("Rego Park")).apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== ""
    interp.feature.center.lat must_== 5
    interp.feature.center.lng must_== 6
    interp.feature.displayName must_== "Rego Park, New York"
    interp.feature.woeType must_== YahooWoeType.TOWN
    interp.where must_== "rego park"
  }

  "hierarchical feature geocodes succeeds with matching data" in {
    val store = buildRegoPark()

    val mongoFuturePool = FuturePool(Executors.newFixedThreadPool(1))
    val r = new GeocoderImpl(mongoFuturePool, store).geocode(new GeocodeRequest("Rego Park, New York")).apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== ""
    interp.feature.center.lat must_== 5
    interp.feature.center.lng must_== 6
    interp.where must_== "rego park new york"
  }

  "splitting geocodes succeeds with matching data" in {
    val store = buildRegoPark()

    val mongoFuturePool = FuturePool(Executors.newFixedThreadPool(1))
    val r = new GeocoderImpl(mongoFuturePool, store).geocode(new GeocodeRequest("Pizza Rego Park, New York")).apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== "pizza"
    interp.feature.center.lat must_== 5
    interp.feature.center.lng must_== 6
    interp.where must_== "rego park new york"
  }

  "geocodes fails without matching data" in {
    val store = buildRegoPark()

    val mongoFuturePool = FuturePool(Executors.newFixedThreadPool(1))
    val r = new GeocoderImpl(mongoFuturePool, store).geocode(new GeocodeRequest("Berlin, Germany")).apply()
    r.interpretations.size must_== 0
  }

  "geocode interpretations don't cross parent hierarchies" in {
    val store = buildRegoPark()
    addLosAngeles(store)

    val mongoFuturePool = FuturePool(Executors.newFixedThreadPool(1))
    val r = new GeocoderImpl(mongoFuturePool, store).geocode(new GeocodeRequest("Rego Park, California")).apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== "rego park"
    interp.feature.center.lat must_== 10
    interp.feature.center.lng must_== 11
    interp.where must_== "california"
  }

  // add a basic ranking test
  // add a preferred name test
}