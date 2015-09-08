// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util.{GeometryUtils, GeonamesId, NameNormalizer, StoredFeatureId}
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import java.nio.ByteBuffer
import org.specs2.mutable._
import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

class MockGeocodeStorageReadService extends GeocodeStorageReadService {
  val nameMap = new HashMap[String, List[StoredFeatureId]]
  val idMap = new HashMap[StoredFeatureId, GeocodeServingFeature]

  def getByName(name: String): Seq[GeocodeServingFeature] = {
    getByFeatureIds(getIdsByName(name)).map(_._2).toSeq
  }

  def getIdsByNamePrefix(name: String): Seq[StoredFeatureId] = {
    nameMap.toList.filter(_._1.startsWith(name)).flatMap(_._2)
  }

  def getIdsByName(name: String): Seq[StoredFeatureId] = {
    nameMap.getOrElse(name, Nil)
  }

  def getByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, GeocodeServingFeature] = {
    ids.map(id => {
      (id -> idMap(id))
    }).toMap
  }

  def getBySlugOrFeatureIds(ids: Seq[String]): Map[String, GeocodeServingFeature] = {
    (for {
      id <- ids
      feature <- idMap.values.filter(servingFeature =>
        id == servingFeature.feature.slugOrNull ||
        servingFeature.feature.ids.exists(fid => "%s:%s".format(fid.source, fid.id) == id)
      )
    } yield {
      (id -> feature)
    }).toMap
  }

  val s2map = new HashMap[Long, ListBuffer[CellGeometry]]

  def addGeometry(geom: Geometry, woeType: YahooWoeType, id: StoredFeatureId) {
    val wkbWriter = new WKBWriter()
    val cells =
      GeometryUtils.s2PolygonCovering(
        geom,
        getMinS2Level,
        getMaxS2Level,
        levelMod = Some(getLevelMod),
        maxCellsHintWhichMightBeIgnored = Some(1000)
      )

    cells.foreach(cellid => {
      val s2shape = ShapefileS2Util.fullGeometryForCell(cellid)
      val bucket = s2map.getOrElseUpdate(cellid.id, new ListBuffer[CellGeometry]())
      val cellGeometryBuilder = CellGeometry.newBuilder
      val recordShape = geom.buffer(0)
      if (recordShape.contains(s2shape)) {
        cellGeometryBuilder.full(true)
      } else {
        cellGeometryBuilder.wkbGeometry(
          ByteBuffer.wrap(wkbWriter.write(s2shape.intersection(recordShape))))
      }
      cellGeometryBuilder.woeType(woeType)
      cellGeometryBuilder.longId(id.longId)
      bucket += cellGeometryBuilder.result
    })
  }

  def getByS2CellId(id: Long): Seq[CellGeometry] = {
    s2map.getOrElse(id, Seq())
  }

  def getPolygonByFeatureId(id: StoredFeatureId): Option[Geometry] = None
  def getPolygonByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Geometry] = Map.empty

  def getS2CoveringByFeatureId(id: StoredFeatureId): Option[Seq[Long]] = None
  def getS2CoveringByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Seq[Long]] = Map.empty

  def getS2InteriorByFeatureId(id: StoredFeatureId): Option[Seq[Long]] = None
  def getS2InteriorByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Seq[Long]] = Map.empty

  def getLevelMod: Int = 2
  def getMinS2Level: Int = 8
  def getMaxS2Level: Int = 12

  def refresh() { }

  var idCounter = 0
  def addGeocode(
    name: String,
    parents: List[GeocodeServingFeature],
    lat: Double,
    lng: Double,
    woeType: YahooWoeType,
    population: Option[Int] = None,
    cc: String = "US",
    slug: Option[String] = None
  ): MutableGeocodeServingFeature = {
    val id = idCounter
    idCounter += 1
    val fid = GeonamesId(id)

    val center = GeocodePoint(lat, lng)

    val geometry = FeatureGeometry.newBuilder.center(center).result

    val featureBuilder = GeocodeFeature.newBuilder
      .geometry(geometry)
      .cc(cc)
      .woeType(woeType)
      .longId(fid.longId)
      .slug(slug)

    val featureName = FeatureName.newBuilder
      .name(name)
      .lang("en")
      .flags(List(FeatureNameFlags.PREFERRED))
      .result
    featureBuilder.names(List(featureName))

    featureBuilder.ids(List(fid.thriftFeatureId))

    val scoringFeaturesBuilder = ScoringFeatures.newBuilder
    scoringFeaturesBuilder.parentIds(parents.map(_.longId))
    scoringFeaturesBuilder.population(population)

    val servingFeatureBuilder = GeocodeServingFeature.newBuilder
      .scoringFeatures(scoringFeaturesBuilder.result)
      .longId(fid.longId)
      .feature(featureBuilder.result)

    nameMap(NameNormalizer.normalize(name)) = fid :: nameMap.getOrElse(NameNormalizer.normalize(name), Nil)

    val servingFeature = servingFeatureBuilder.resultMutable
    idMap(fid) = servingFeature
    servingFeature
  }
}

class GeocoderSpec extends Specification {
  GeocodeServerConfigSingleton.init(Array("--hfile_basepath", ""))

  def addParisFrance(store: MockGeocodeStorageReadService) = {
    val frRecord = store.addGeocode("FR", Nil, 1, 2, YahooWoeType.COUNTRY, cc="FR")
    val idfRecord = store.addGeocode("IDF", List(frRecord), 3, 4, YahooWoeType.ADMIN1, cc="FR")
    val parisRecord = store.addGeocode("Paris", List(idfRecord, frRecord), 50, 60, YahooWoeType.TOWN, population=Some(1000000), cc="FR")
    store
  }

  def addSenayans(store: MockGeocodeStorageReadService) = {
    val idRecord = store.addGeocode("ID", Nil, 1, 2, YahooWoeType.COUNTRY, cc="ID")
    val adm1Record1 = store.addGeocode("Daerah Khusus Ibukota Jakarta", List(idRecord), 3, 4, YahooWoeType.ADMIN1, cc="ID")
    val senayanRecord1 = store.addGeocode("Senayan", List(adm1Record1, idRecord), 5, 6, YahooWoeType.TOWN, population=Some(20000), cc="ID")

    val adm1Record2 = store.addGeocode("Sumatera Utara", List(idRecord), 3, 4, YahooWoeType.ADMIN1, cc="ID")
    val senayanRecord2 = store.addGeocode("Senayan", List(adm1Record2, idRecord), 10, 20, YahooWoeType.TOWN, population=Some(20000), cc="ID")
    store
  }

  def addParisTX(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val txRecord = store.addGeocode("Texas", List(usRecord), 3, 4, YahooWoeType.ADMIN1)
    val parisRecord = store.addGeocode("Paris", List(txRecord, usRecord), 5, 6, YahooWoeType.TOWN, population=Some(20000))
    store
  }

  def addParisIL(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val ilRecord = store.addGeocode("Illinois", List(usRecord), 3, 4, YahooWoeType.ADMIN1)
    val parisRecord = store.addGeocode("Paris", List(ilRecord, usRecord), 2, 6, YahooWoeType.TOWN, population=Some(20000))
    store
  }

  def addKansas(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val kansasRecord = store.addGeocode("Kansas", List(usRecord), 3, 4, YahooWoeType.ADMIN1, population=Some(2000000))
    store
  }

  def addKansasCityMO(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val moRecord = store.addGeocode("Missouri", List(usRecord), 3, 4, YahooWoeType.ADMIN1)
    val kansasCityRecord = store.addGeocode("Kansas City", List(moRecord, usRecord), 2, 6, YahooWoeType.TOWN, population=Some(20000))
    store
  }

  def addSohos(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val nyRecord = store.addGeocode("New York", List(usRecord), 3, 4, YahooWoeType.ADMIN1)
    val soho1 = store.addGeocode("Soho", List(nyRecord, usRecord),
      40.723537, -74.005313, YahooWoeType.TOWN, population = Some(500012))
    val soho2 = store.addGeocode("Soho", List(nyRecord, usRecord),
     40.72241, -73.99961, YahooWoeType.TOWN, population = Some(500012))
    store
  }

  def addBrooklyns(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val brooklynTigerAdm2 = store.addGeocode("Brooklyn", List(usRecord),
      40.63439, -73.95027, YahooWoeType.ADMIN2, population = Some(2504700))
    val brooklynGeonamesTown = store.addGeocode("Brooklyn", List(usRecord),
     40.6501, -73.94958, YahooWoeType.TOWN, population = Some(2300664))
    store
  }

  def addRegoPark(store: MockGeocodeStorageReadService) = {
    val usRecord = store.addGeocode("US", Nil, 1, 2, YahooWoeType.COUNTRY)
    val nyRecord = store.addGeocode("New York", List(usRecord), 3, 4, YahooWoeType.ADMIN1)
    val regoParkRecord = store.addGeocode("Rego Park", List(nyRecord, usRecord), 5, 6, YahooWoeType.TOWN,
      population = Some(500012))
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
    val caRecord = store.addGeocode("California", List(usRecord), 10, 11, YahooWoeType.ADMIN1,
            population = Some(5000000))
    val losAngelesRecord = store.addGeocode("Los Angeles", List(caRecord, usRecord), 12, 13, YahooWoeType.TOWN)
    val laRecord = store.addGeocode("L.A.", List(caRecord, usRecord), 12, 13, YahooWoeType.TOWN)
    store
  }

  "one feature geocodes succeeds with matching data" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Rego Park").debug(4).result

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size aka r.toString must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull must_== ""
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
    interp.feature.displayNameOrNull must_== "Rego Park, New York, US"
    interp.feature.woeTypeOrNull must_== YahooWoeType.TOWN
    interp.whereOrNull must_== "rego park"
    interp.parents.isEmpty must_== true
  }

  "admin1, city fails" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("New York Rego Park").debug(4).result

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size aka r.toString must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull must_== "new york"
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
    interp.feature.displayNameOrNull must_== "Rego Park, New York, US"
    interp.feature.woeTypeOrNull must_== YahooWoeType.TOWN
    interp.whereOrNull must_== "rego park"
    interp.parents.isEmpty must_== true
  }

  "everything returns parents" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Rego Park")
      .responseIncludes(List(ResponseIncludes.EVERYTHING))
      .result

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.parents.size mustEqual 2
  }

  "don't include matching country in displayName" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Rego Park")
      .cc("US")
      .result

    val r2 = new GeocodeRequestDispatcher(store).geocode(req)
    r2.interpretations.size must_== 1
    val interp2 = r2.interpretations()(0)
    interp2.feature.displayNameOrNull must_== "Rego Park, New York"
  }

  "hierarchical feature geocodes succeeds with matching data" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Rego Park, New York").result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull aka r.toString must_== ""
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
    interp.whereOrNull must_== "rego park new york"
  }

  "split on commas without spaces" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Rego Park,New York").result
    val r = new GeocodeRequestDispatcher(store).geocode(req)

    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull must_== ""
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
    interp.whereOrNull must_== "rego park new york"
  }

  "splitting geocodes succeeds with matching data" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Pizza Rego Park, New York").result
    val r = new GeocodeRequestDispatcher(store).geocode(req)

    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull must_== "pizza"
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
    interp.whereOrNull must_== "rego park new york"
  }

  "geocodes fails without matching data" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Berlin, Germany").result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 0
  }

  "geocode interpretations don't cross parent hierarchies" in {
    val store = buildRegoPark()
    addLosAngeles(store)

    val req = GeocodeRequest.newBuilder.query("Rego Park, California")
      .debug(4)
      .result

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations must haveSize(1)
    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull must_== "rego park"
    interp.feature.geometry.center.lat must_== 10
    interp.feature.geometry.center.lng must_== 11
    interp.whereOrNull must_== "california"
  }

  "everything request fills parents" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Rego Park, New York")
      .responseIncludes(List(ResponseIncludes.EVERYTHING))
      .result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull must_== ""
    interp.whereOrNull must_== "rego park new york"
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6

    val parents = interp.parents
    parents.size must_== 2
    parents(0).nameOrNull mustEqual "New York"
    parents(1).nameOrNull mustEqual "US"
  }

 "full request fills parents" in {
    val store = getStore
    addParisTX(store)
    addParisFrance(store)

    val req = GeocodeRequest.newBuilder.query("Paris")
      .maxInterpretations(2)
      .debug(1)
      .result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 2
    val interp1 = r.interpretations()(0)
    interp1.whatOrNull must_== ""
    interp1.whereOrNull must_== "paris"
    interp1.feature.cc must_== "FR"

    val interp2 = r.interpretations()(1)
    interp2.whatOrNull must_== ""
    interp2.whereOrNull must_== "paris"
    interp2.feature.cc must_== "US"
  }

 "ambiguous names" in {

    val store = getStore
    addParisTX(store)
    addParisIL(store)

    val req = GeocodeRequest.newBuilder.query("Paris US")
      .maxInterpretations(2)
      .debug(1)
      .result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size aka r.toString must_== 2
    val interp1 = r.interpretations()(0)
    interp1.feature.displayNameOrNull must_== "Paris, Texas, US"
    // TODO(blackmad): fix this test
    interp1.feature.highlightedNameOrNull must be_==("<b>Paris</b>, Texas, <b>US</b>").orPending

    val interp2 = r.interpretations()(1)
    interp2.feature.displayNameOrNull must_== "Paris, Illinois, US"
    interp2.feature.highlightedNameOrNull must_== "<b>Paris</b>, Illinois, <b>US</b>"
  }

  "common words in parsed phrases not deleted" in {
    val store = getStore
    addKansas(store)
    addKansasCityMO(store)

    val req = GeocodeRequest.newBuilder.query("Pizza in Kansas City").result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull must_== "pizza"
    interp.whereOrNull must_== "kansas city"
  }

  "everything after connector geocodes" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Pizza near Rego Park, New York").result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.whatOrNull must_== "pizza"
    interp.whereOrNull must_== "rego park new york"
  }

  "not everything after connector geocodes" in {
    val store = buildRegoPark()

    val req = GeocodeRequest.newBuilder.query("Pizza near lkjdsfjksl, New York").result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 0
  }

  "autocomplete" in {
    val store = buildRegoPark()
    addRego(store)

    val req = GeocodeRequest.newBuilder.query("Rego")
      .autocomplete(true)
      .debug(2)
      .result

    store.getIdsByNamePrefix("rego") must haveSize(2)

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations must haveSize(2)

    var interp = r.interpretations()(0)
    interp.whatOrNull must_== ""
    interp.whereOrNull must_== "rego"
    interp.feature.highlightedNameOrNull must_== "<b>Rego</b> Park, New York, US"

    interp = r.interpretations()(1)
    interp.whatOrNull must_== ""
    interp.whereOrNull must_== "rego"
    interp.feature.highlightedNameOrNull must_== "<b>Rego</b>, US"
  }

  "autocomplete 2" in {
    val store = buildRegoPark()
    addRego(store)

    val req = GeocodeRequest.newBuilder.query("Rego P").autocomplete(true).result

    // validating our test harness
    store.getIdsByNamePrefix("rego p").size must_== 1

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 1

    val interp = r.interpretations()(0)
    interp.whatOrNull must_== ""
    interp.whereOrNull must_== "rego p"
    interp.feature.highlightedNameOrNull must_== "<b>Rego P</b>ark, New York, US"
  }

  "autocomplete 3" in {
    val store = buildRegoPark()
    addRego(store)

    val req = GeocodeRequest.newBuilder.query("Rego Park New").autocomplete(true).debug(1).result

    // validating our test harness
    store.getIdsByNamePrefix("rego park").size must_== 1
    store.getIdsByNamePrefix("new").size must_== 1

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size aka r.debugLines.mkString("\n") must_== 1

    // val interp = r.interpretations()(0)
    // interp.whatOrNull must_== ""
    // interp.whereOrNull must_== "rego park new"
    // interp.feature.highlightedName must_== "<b>Rego Park, New</b> York, US"
  }

  "woe restrict works" in {
    val store = getStore
    val parisTownRecord = store.addGeocode("Paris", Nil, 5, 6, YahooWoeType.TOWN, cc="FR")
    val parisAdmin1Record = store.addGeocode("Paris", Nil, 10, 11, YahooWoeType.ADMIN1, cc="FR")

    val req = GeocodeRequest.newBuilder.query("paris").woeRestrict(List(YahooWoeType.ADMIN1)).result

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.feature.geometry.center.lat must_== 10
    interp.feature.geometry.center.lng must_== 11

    val req2 = req.mutableCopy
    req2.woeRestrict_=(List(YahooWoeType.ADMIN2))

    val r2 = new GeocodeRequestDispatcher(store).geocode(req2)
    r2.interpretations.size must_== 0
  }

  "woe hint works" in {
    val store = getStore
    val parisTownRecord = store.addGeocode("Paris", Nil, 5, 6, YahooWoeType.TOWN, cc="FR")
    val parisAdmin1Record = store.addGeocode("Paris", Nil, 10, 11, YahooWoeType.ADMIN1, cc="FR")

    val req = GeocodeRequest.newBuilder.query("paris")
      .maxInterpretations(2)
      .woeHint(List(YahooWoeType.ADMIN1))
      .result

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 2
    val interp = r.interpretations()(0)
    interp.feature.geometry.center.lat must_== 10
    interp.feature.geometry.center.lng must_== 11
  }

  "slug lookup works" in {
    val store = getStore
    val parisTownRecord = store.addGeocode("Paris", Nil, 5, 6, YahooWoeType.TOWN,
      cc="FR", slug=Some("paris-fr"))

    val req = GeocodeRequest.newBuilder.slug("paris-fr").result

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 1
    val interp = r.interpretations()(0)
    interp.feature.geometry.center.lat must_== 5
    interp.feature.geometry.center.lng must_== 6
  }

  "bad slug lookup fails" in {
    val store = getStore
    val parisTownRecord = store.addGeocode("Paris", Nil, 5, 6, YahooWoeType.TOWN,
      cc="FR", slug = Some("paris-fr"))

    val req = GeocodeRequest.newBuilder.slug("paris").result

    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size must_== 0
  }

  def getId(f: GeocodeServingFeature): StoredFeatureId = {
    StoredFeatureId.fromThriftFeatureId(f.feature.ids()(0)).get
  }

  "reverse geocode" in {
    val store = getStore
    val parisTownRecord = store.addGeocode("Paris", Nil, 5, 6, YahooWoeType.TOWN, cc="FR")
    val parisId = getId(parisTownRecord)
    store.addGeometry(
      new WKTReader().read("POLYGON ((1.878662109375 48.8719414772291,2.164306640625 49.14578361775004,2.779541015625 49.14578361775004,3.153076171875 48.72720881940671,2.581787109375 48.50932644976633,1.878662109375 48.8719414772291))"),
      YahooWoeType.TOWN,
      parisId
    )

    val nyTownRecord = store.addGeocode("New York", Nil, 5, 6, YahooWoeType.TOWN, cc="US")
    val nyId = getId(nyTownRecord)
    store.addGeometry(
      new WKTReader().read("POLYGON ((-74.0427017211914 40.7641613153526,-73.93146514892578 40.7641613153526,-73.93146514892578 40.681679458715635,-74.0427017211914 40.681679458715635,-74.0427017211914 40.7641613153526))"),
      YahooWoeType.TOWN,
      nyId
    )

    var req = GeocodeRequest.newBuilder.ll(GeocodePoint(48.7996273507997, 2.43896484375)).result
    var r = new ReverseGeocoderImpl(store, req).doGeocode()
    r.interpretations.size must_== 1
    r.interpretations()(0).feature.nameOrNull must_== "Paris"

    // look up in kansas
    var req2 = GeocodeRequest.newBuilder.ll(GeocodePoint(-97.822265625, 38.06539235133249)).result
    r = new ReverseGeocoderImpl(store, req2).doGeocode()
    r.interpretations.size must_== 0

    var req3 = GeocodeRequest.newBuilder.ll(GeocodePoint(40.74, -74)).result
    r = new ReverseGeocoderImpl(store, req3).doGeocode()
    r.interpretations.size must_== 1
    r.interpretations()(0).feature.nameOrNull must_== "New York"
  }

 "ambiguous names outside US" in {
    val store = getStore
    addSenayans(store)

    val req = GeocodeRequest.newBuilder.query("Senayan")
      .maxInterpretations(2)
      .debug(1)
      .result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size aka r.toString must_== 2
    val interp1 = r.interpretations()(0)
    interp1.feature.displayNameOrNull must_== "Senayan, Daerah Khusus Ibukota Jakarta, ID"
    interp1.feature.highlightedNameOrNull must_== "<b>Senayan</b>, Daerah Khusus Ibukota Jakarta, ID"

    val interp2 = r.interpretations()(1)
    interp2.feature.displayNameOrNull must_== "Senayan, Sumatera Utara, ID"
    interp2.feature.highlightedNameOrNull must_== "<b>Senayan</b>, Sumatera Utara, ID"
  }

 "duplicate features" in {
    val store = getStore
    addSohos(store)

    val req = GeocodeRequest.newBuilder.query("Soho")
      .maxInterpretations(2)
      .debug(1)
      .result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size aka r.toString must_== 1
    val interp1 = r.interpretations()(0)
    interp1.feature.displayNameOrNull must_== "Soho, New York, US"
    interp1.feature.highlightedNameOrNull must_== "<b>Soho</b>, New York, US"
  }

  "duplicate features, different woetype" in {
    val store = getStore
    addBrooklyns(store)

    val req = GeocodeRequest.newBuilder.query("Brooklyn")
      .maxInterpretations(2)
      .debug(1)
      .result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size aka r.toString must_== 1
    val interp1 = r.interpretations()(0)
    interp1.feature.woeTypeOrNull must_== YahooWoeType.TOWN
  }

  "name highlighting handles length-altering normalization" in {
    val store = getStore
    addLosAngeles(store)
    val req = GeocodeRequest.newBuilder.query("LA")
      .maxInterpretations(2)
      .debug(1)
      .result
    val r = new GeocodeRequestDispatcher(store).geocode(req)
    r.interpretations.size aka r.toString must_== 1
    val interp1 = r.interpretations()(0)
    interp1.feature.displayNameOrNull must_== "L.A., California, US"
    interp1.feature.highlightedNameOrNull must_== "<b>L.A.</b>, California, US"
  }

  // add a preferred name test
  // add a name filtering test
  // add a displayname test
}
