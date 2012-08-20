// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import collection.JavaConverters._
import com.twitter.util.Future
import org.specs2.mutable._
import scala.collection.mutable.HashMap
import org.bson.types.ObjectId


class MockGeocodeStorageReadService extends GeocodeStorageFutureReadService {
  val nameMap = new HashMap[String, List[ObjectId]]
  val idMap = new HashMap[ObjectId, GeocodeServingFeature]

  def getByName(name: String): Future[Seq[GeocodeServingFeature]] = {
    getIdsByName(name).flatMap(getByObjectIds).map(_.map(_._2).toList)
  }

  def getIdsByNamePrefix(name: String): Future[Seq[ObjectId]] = {
    Future.value(
      nameMap.toList.filter(_._1.startsWith(name)).flatMap(_._2)
    )
  }

  def getIdsByName(name: String): Future[Seq[ObjectId]] = {
    Future.value(nameMap.getOrElse(name, Nil))
  }

  def getByObjectIds(ids: Seq[ObjectId]): Future[Map[ObjectId, GeocodeServingFeature]] = {
    Future.value(
      ids.map(id => {
        (id -> idMap(id))
      }).toMap
    )
  }

  def addGeocode(
    name: String,
    parents: List[GeocodeServingFeature],
    lat: Double,
    lng: Double,
    woeType: YahooWoeType,
    population: Option[Int] = None,
    cc: String = "US"
  ): GeocodeServingFeature = {
    var id = new ObjectId()

    val center = new GeocodePoint()
    center.setLat(lat)
    center.setLng(lng)

    val geometry = new FeatureGeometry()
    geometry.setCenter(center)

    val feature = new GeocodeFeature()
    feature.setGeometry(geometry)
    feature.setCc(cc)
    feature.setWoeType(woeType)

    val featureName = new FeatureName()
    featureName.setName(name)
    featureName.setLang("en")
    feature.setNames(List(featureName).asJava)

    val scoringFeatures = new ScoringFeatures()
    scoringFeatures.setParents(parents.map(_.id).asJava)
    population.foreach(p => scoringFeatures.setPopulation(p))

    val servingFeature = new GeocodeServingFeature()
    servingFeature.setScoringFeatures(scoringFeatures)
    servingFeature.setId(id.toString)
    servingFeature.setFeature(feature)
    nameMap(name.toLowerCase) = id :: nameMap.getOrElse(name.toLowerCase, Nil)
    idMap(id) = servingFeature

    servingFeature
  }
}

class GeocoderSpec extends Specification {
  def addParisFrance(store: MockGeocodeStorageReadService) = {
    val frRecord = store.addGeocode("FR", Nil, 1, 2, YahooWoeType.COUNTRY, cc="FR")
    val idfRecord = store.addGeocode("IDF", List(frRecord), 3, 4, YahooWoeType.ADMIN1, cc="FR")
    val parisRecord = store.addGeocode("Paris", List(idfRecord, frRecord), 50, 60, YahooWoeType.TOWN, population=Some(1000000), cc="FR")
    store
  }

  def addParisTX(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val txRecord = store.addGeocode("Texas", List(usRecord), 3, 4, YahooWoeType.ADMIN1)
    val parisRecord = store.addGeocode("Paris", List(txRecord, usRecord), 5, 6, YahooWoeType.TOWN, population=Some(20000))
    store
  }

  def addRegoPark(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val nyRecord = store.addGeocode("New York", List(usRecord), 3, 4, YahooWoeType.ADMIN1)
    val regoParkRecord = store.addGeocode("Rego Park", List(nyRecord, usRecord), 5, 6, YahooWoeType.TOWN)
    store
  }

  def addRego(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    store.addGeocode("Rego", List(usRecord), 5, 6, YahooWoeType.TOWN)
    store
  }

  def getStore = new MockGeocodeStorageReadService()

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

    val req = new GeocodeRequest("Rego Park")

    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== ""
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
    interp.feature.displayName must_== "Rego Park, New York, US"
    interp.feature.woeType must_== YahooWoeType.TOWN
    interp.where must_== "rego park"
    interp.parents mustEqual null
  }

  "full returns parents" in {
    val store = buildRegoPark()

    val req = new GeocodeRequest("Rego Park")
    req.setFull(true)

    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.parents.size mustEqual 2
  }

  "don't include matching country in displayName" in {
    val store = buildRegoPark()

    val req = new GeocodeRequest("Rego Park")
    req.setCc("US")

    val r2 = new GeocoderImpl(store, req).geocode().apply()
    r2.interpretations.size must_== 1
    val interp2 = r2.interpretations.asScala(0)
    interp2.feature.displayName must_== "Rego Park, New York"
  }

  "hierarchical feature geocodes succeeds with matching data" in {
    val store = buildRegoPark()

    val req = new GeocodeRequest("Rego Park, New York")
    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== ""
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
    interp.where must_== "rego park new york"
  }

  "splitting geocodes succeeds with matching data" in {
    val store = buildRegoPark()

    val req = new GeocodeRequest("Pizza Rego Park, New York")
    val r = new GeocoderImpl(store, req).geocode().apply()

    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== "pizza"
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
    interp.where must_== "rego park new york"
  }

  "geocodes fails without matching data" in {
    val store = buildRegoPark()

    val req = new GeocodeRequest("Berlin, Germany")
    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 0
  }

  "geocode interpretations don't cross parent hierarchies" in {
    val store = buildRegoPark()
    addLosAngeles(store)

    val req = new GeocodeRequest("Rego Park, California")
    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== "rego park"
    interp.feature.geometry.center.lat must_== 10
    interp.feature.geometry.center.lng must_== 11
    interp.where must_== "california"
  }

  "full request fills parents" in {
    val store = buildRegoPark()

    val req = new GeocodeRequest("Rego Park, New York")
    req.setFull(true)
    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== ""
    interp.where must_== "rego park new york"
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6

    val parents = interp.parents.asScala
    parents.size must_== 2
    parents(0).name mustEqual "New York"
    parents(1).name mustEqual "US"
  }

 "full request fills parents" in {
    val store = getStore
    addParisTX(store)
    addParisFrance(store)

    println(store.nameMap("paris"))

    val req = new GeocodeRequest("Paris")
    req.setDebug(1)
    val r = new GeocoderImpl(store, req).geocode().apply()
    println(r.debugLines.asScala.mkString("\n"))
    r.interpretations.size must_== 2 
    val interp1 = r.interpretations.asScala(0)
    interp1.what must_== ""
    interp1.where must_== "paris"
    interp1.feature.cc must_== "FR"

    val interp2 = r.interpretations.asScala(1)
    interp2.what must_== ""
    interp2.where must_== "paris"
    interp2.feature.cc must_== "US"
  }

  "everything after connector geocodes" in {
    val store = buildRegoPark()

    val req = new GeocodeRequest("Pizza near Rego Park, New York")
    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.what must_== "pizza"
    interp.where must_== "rego park new york"
  }

  "not everything after connector geocodes" in {
    val store = buildRegoPark()

    val req = new GeocodeRequest("Pizza near lkjdsfjksl Rego Park, New York")
    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 0
  }

  "autocomplete" in {
    val store = buildRegoPark()
    addRego(store)

    val req = new GeocodeRequest("Rego")
    req.setAutocomplete(true)

    store.getIdsByNamePrefix("rego").get().size must_== 2

    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 2
    val interp = r.interpretations.asScala(0)
  } 

  "autocomplete 2" in {
    val store = buildRegoPark()
    addRego(store)

    val req = new GeocodeRequest("Rego P")
    req.setAutocomplete(true)

    store.getIdsByNamePrefix("rego p").get().size must_== 1

    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
  } 

  "woe restrict works" in {
    val store = getStore
    val parisTownRecord = store.addGeocode("Paris", Nil, 5, 6, YahooWoeType.TOWN, cc="FR")
    val parisAdmin1Record = store.addGeocode("Paris", Nil, 10, 11, YahooWoeType.ADMIN1, cc="FR")

    val req = new GeocodeRequest("paris")
    req.setWoeRestrict(List(YahooWoeType.ADMIN1).asJava)

    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 1
    val interp = r.interpretations.asScala(0)
    interp.feature.geometry.center.lat must_== 10
    interp.feature.geometry.center.lng must_== 11

    req.setWoeRestrict(List(YahooWoeType.ADMIN2).asJava)

    val r2 = new GeocoderImpl(store, req).geocode().apply()
    r2.interpretations.size must_== 0
  }

  "woe hint works" in {
    val store = getStore
    val parisTownRecord = store.addGeocode("Paris", Nil, 5, 6, YahooWoeType.TOWN, cc="FR")
    val parisAdmin1Record = store.addGeocode("Paris", Nil, 10, 11, YahooWoeType.ADMIN1, cc="FR")

    val req = new GeocodeRequest("paris")
    req.setWoeHint(List(YahooWoeType.ADMIN1).asJava)

    val r = new GeocoderImpl(store, req).geocode().apply()
    r.interpretations.size must_== 2
    val interp = r.interpretations.asScala(0)
    interp.feature.geometry.center.lat must_== 10
    interp.feature.geometry.center.lng must_== 11
  }


  // add a preferred name test
  // add a name filtering test
  // add a displayname test
}
