//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.Implicits._
import com.foursquare.twofishes.util.{GeoTools, GeometryUtils}
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.ostrich.stats.Stats
import com.twitter.util.Duration
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import com.vividsolutions.jts.io.{WKBReader, WKTWriter}
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.bson.types.ObjectId
import scala.collection.JavaConversions._
import scalaj.collection.Implicits._

class ReverseGeocodeParseOrdering extends Ordering[Parse[Sorted]] {
  def compare(a: Parse[Sorted], b: Parse[Sorted]): Int = {
    val comparisonOpt = for {
      aFeatureMatch <- a.headOption
      bFeatureMatch <- b.headOption
    } yield {
      val aServingFeature = aFeatureMatch.fmatch
      val bServingFeature = bFeatureMatch.fmatch
      val aWoeTypeOrder = YahooWoeTypes.getOrdering(aServingFeature.feature.woeType)
      val bWoeTypeOrder = YahooWoeTypes.getOrdering(bServingFeature.feature.woeType)
      if (aWoeTypeOrder != bWoeTypeOrder) {
         aWoeTypeOrder - bWoeTypeOrder
      } else {
        bServingFeature.scoringFeatures.boost - 
          aServingFeature.scoringFeatures.boost
      }
    }

    comparisonOpt.getOrElse(0)
  }
}

class ReverseGeocoderImpl(
  store: GeocodeStorageReadService,
  req: GeocodeRequest
) extends GeocoderImplTypes {
  val logger = new MemoryLogger(req)

  val responseProcessor = new ResponseProcessor(
    GeocodeRequestUtils.geocodeRequestToCommonRequestParams(req),
    store,
    logger)

  def featureGeometryIntersections(wkbGeometry: Array[Byte], otherGeom: Geometry) = {
    val wkbReader = new WKBReader()
    val geom = wkbReader.read(wkbGeometry)
    (geom, geom.intersects(otherGeom))
  }

  def computeCoverage(
    featureGeometry: Geometry,
    requestGeometry: Geometry
  ): Double = {
    val intersection = featureGeometry.intersection(requestGeometry)
    math.min(1, intersection.getArea() / requestGeometry.getArea())
  }

  def responseIncludes(include: ResponseIncludes): Boolean = GeocodeRequestUtils.responseIncludes(
    GeocodeRequestUtils.geocodeRequestToCommonRequestParams(req), include)

  def doReverseGeocode(cellids: Seq[Long], otherGeom: Geometry): GeocodeResponse = {
    val cellGeometries: Seq[CellGeometry] = cellids.map(store.getByS2CellId).flatten

    val featureOids: Seq[ObjectId] = {
      if (req.debug > 0) {
        logger.ifDebug("had %d candidates", cellGeometries.size)
        logger.ifDebug("s2 cells: %s", cellids)
      }
      (for {
        cellGeometry <- cellGeometries
        if (req.woeRestrict.isEmpty || req.woeRestrict.asScala.has(cellGeometry.woeType))
      } yield {
        val oid = new ObjectId(cellGeometry.getOid())
        if (cellGeometry.isFull) {
          logger.ifDebug("was full: %s", oid)
          Some(oid)
        } else if (cellGeometry.wkbGeometry != null) {
          val (geom, intersects) = logger.logDuration("intersectionCheck", "intersecting %s".format(oid)) {
            featureGeometryIntersections(cellGeometry.getWkbGeometry(), otherGeom)
          }
          if (intersects) {
            Some(oid)
          } else {
            None
          }
        } else {
          logger.ifDebug("not full and no geometry for: %s", oid)
          None
        }
      }).flatten
    }

    val servingFeaturesMap: Map[ObjectId, GeocodeServingFeature] =
      store.getByObjectIds(featureOids.toSet.toList)

    // need to get polygons if we need to calculate coverage
    val polygonMap: Map[ObjectId, Array[Byte]] =
      if (GeocodeRequestUtils.shouldFetchPolygon(GeocodeRequestUtils.geocodeRequestToCommonRequestParams(req))) {
        store.getPolygonByObjectIds(featureOids)
      } else { Map.empty }

    val wkbReader = new WKBReader()
    // for each, check if we're really in it
    val parses: SortedParseSeq = servingFeaturesMap.map({ case (oid, f) => {
      val parse = Parse[Sorted](List(FeatureMatch(0, 0, "", f)))
      if (responseIncludes(ResponseIncludes.REVGEO_COVERAGE) &&
          otherGeom.getNumPoints > 2) {
        polygonMap.get(oid).foreach(wkb => {
          val geom = wkbReader.read(wkb)
          if (geom.getNumPoints > 2) {
            parse.scoringFeatures.setPercentOfRequestCovered(computeCoverage(geom, otherGeom))
            parse.scoringFeatures.setPercentOfFeatureCovered(computeCoverage(otherGeom, geom))
          }
        })
      }
      parse
    }}).toSeq

    val parseParams = ParseParams()

    val maxInterpretations = if (req.maxInterpretations <= 0) {
      parses.size  
    } else {
      req.maxInterpretations
    }

    val sortedParses = parses.sorted(new ReverseGeocodeParseOrdering).take(maxInterpretations)
    val response = responseProcessor.hydrateParses(sortedParses, parseParams, polygonMap,
      fixAmbiguousNames = false)
    if (req.debug > 0) {
      val wktWriter = new WKTWriter
      response.setRequestWktGeometry(wktWriter.write(otherGeom))
    }
    response
  }

  def getAllLevels(): Seq[Int] = {
    for {
      level <- store.getMinS2Level.to(store.getMaxS2Level)
      if ((level - store.getMinS2Level) % store.getLevelMod) == 0
    } yield { level }
  }

  def reverseGeocodePoint(ll: GeocodePoint): GeocodeResponse = {
    val levels = getAllLevels()
    logger.ifDebug("doing point revgeo on %s at levels %s", ll, levels)

    val cellids: Seq[Long] = 
      levels.map(level =>
        GeometryUtils.getS2CellIdForLevel(ll.lat, ll.lng, level).id()
      )
    logger.ifDebug("looking up: ", cellids)

    val geomFactory = new GeometryFactory()
    val point = geomFactory.createPoint(
      new Coordinate(ll.lng, ll.lat)
    )

    doReverseGeocode(cellids, point)
  }

  def doGeometryReverseGeocode(geom: Geometry) = {
    val cellids = logger.logDuration("s2_cover_time", "s2_cover_time") {
      GeometryUtils.coverAtAllLevels(
        geom,
        store.getMinS2Level,
        store.getMaxS2Level,
        Some(store.getLevelMod)
      ).map(_.id())
    }
    Stats.addMetric("num_geom_cells", cellids.size)
    doReverseGeocode(cellids, geom)
  }

  def timeResponse(ostrichKey: String)(f: GeocodeResponse) = {
    val (rv, duration) = Duration.inNanoseconds(f)
    Stats.addMetric(ostrichKey + "_usec", duration.inMicroseconds.toInt)
    Stats.addMetric(ostrichKey + "_msec", duration.inMilliseconds.toInt)
    if (rv.interpretations.size > 0) {
      Stats.addMetric(ostrichKey + "_with_results_usec", duration.inMicroseconds.toInt)
      Stats.addMetric(ostrichKey + "_with_results_msec", duration.inMilliseconds.toInt)
    }
    rv
  }

  def reverseGeocode(): GeocodeResponse = {
    Stats.incr("revgeo-requests", 1)
    if (req.ll != null) {
      if (req.isSetRadius && req.radius > 0) {
        if (req.radius > 50000) {
          println("too large revgeo: " + req)
          //throw new Exception("radius too big (%d > %d)".format(req.radius, maxRadius))
          new GeocodeResponse()
        } else {
          val sizeDegrees = req.radius / 111319.9
          val gsf = new GeometricShapeFactory()
          gsf.setSize(sizeDegrees)
          gsf.setNumPoints(100)
          gsf.setCentre(new Coordinate(req.ll.lng, req.ll.lat))
          val geom = gsf.createCircle()
          timeResponse("revgeo-geom") {
            doGeometryReverseGeocode(geom)
          }
        }
      } else {
        timeResponse("revgeo-point") {
          reverseGeocodePoint(req.ll)
        }
      }
    } else if (req.bounds != null) {
      val s2rect = GeoTools.boundingBoxToS2Rect(req.bounds)
      val geomFactory = new GeometryFactory()
      val geom = geomFactory.createLinearRing(Array(
        new Coordinate(s2rect.lng.lo, s2rect.lat.lo),
        new Coordinate(s2rect.lng.hi, s2rect.lat.lo),
        new Coordinate(s2rect.lng.hi, s2rect.lat.hi),
        new Coordinate(s2rect.lng.hi, s2rect.lat.lo),
        new Coordinate(s2rect.lng.lo, s2rect.lat.lo)
      ))
      Stats.time("revgeo-geom") {
        doGeometryReverseGeocode(geom)
      }
    } else {
      throw new Exception("no bounds or ll")
    }
  }
}
