//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.foursquare.twofishes.util.StoredFeatureId
import com.twitter.ostrich.stats.Stats
import com.vividsolutions.jts.geom.Geometry
import org.bson.types.ObjectId
import scalaj.collection.Implicits._

class SlugGeocoderImpl(
  store: GeocodeStorageReadService,
  req: GeocodeRequest,
  logger: MemoryLogger
) extends GeocoderImplTypes {
  val commonParams = GeocodeRequestUtils.geocodeRequestToCommonRequestParams(req)
  val responseProcessor = new ResponseProcessor(
    commonParams,
    store,
    logger)

  def doSlugGeocode(slug: String): GeocodeResponse = {
    Stats.incr("slug-lookup-requests", 1)
    val parseParams = ParseParams()

    val featureMap: Map[String, GeocodeServingFeature] = store.getBySlugOrFeatureIds(List(slug))

    // a parse is still a seq of featurematch, bleh
    val parse = Parse[Sorted](featureMap.get(slug).map(servingFeature => {
      FeatureMatch(0, 0, "", servingFeature)
    }).toList)

    responseProcessor.buildFinalParses(List(parse), parseParams, 0)
  }
}

class BulkSlugLookupImpl(
  store: GeocodeStorageReadService,
  req: BulkSlugLookupRequest
) extends GeocoderImplTypes with BulkImplHelpers {
  val params = req.paramsOption.getOrElse(CommonGeocodeRequestParams.newBuilder.result)
  val logger = new MemoryLogger(params)

  val responseProcessor = new ResponseProcessor(params, store, logger)

  def slugLookup(): BulkSlugLookupResponse = {
    Stats.incr("bulk-slug-lookup-requests", 1)
    val parseParams = ParseParams()

    val featureMap: Map[String, GeocodeServingFeature] = store.getBySlugOrFeatureIds(req.slugs)
    //val inputToIdxes: Map[String, Seq[Int]] = req.slugs.zipWithIndex.groupBy(_._1).mapValues(_.map(_._2))

    val polygonMap: Map[StoredFeatureId, Geometry] = if (GeocodeRequestUtils.shouldFetchPolygon(params)) {
      store.getPolygonByFeatureIds(featureMap.values.flatMap(f => StoredFeatureId.fromLong(f.longId)).toSeq)
    } else {
      Map.empty
    }

    val parses = featureMap.values.map(servingFeature =>
      Parse[Sorted](Seq(FeatureMatch(0, 0, "", servingFeature)))).toSeq

    val interps: Seq[GeocodeInterpretation] = responseProcessor.hydrateParses(parses,
      parseParams, polygonMap, fixAmbiguousNames = true, dedupByMatchedName = false)

    val (interpIdxs, retInterps, parents) = makeBulkReply(
      req.slugs,
      featureMap.mapValues(servingFeature => StoredFeatureId.fromLong(servingFeature.feature.longId).toList),
      interps) 

    val respBuilder = BulkSlugLookupResponse.newBuilder
      .interpretations(interps)
      // map from input idx to interpretation indexes
      .interpretationIndexes(interpIdxs)
      .parentFeatures(parents)

    if (params.debug > 0) {
      respBuilder.debugLines(logger.getLines)
    }
    respBuilder.result
  }
}
