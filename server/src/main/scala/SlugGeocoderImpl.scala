//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import org.bson.types.ObjectId

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
    val parseParams = ParseParams()

    val featureMap: Map[String, GeocodeServingFeature]  = if (ObjectId.isValid(slug)) {
      val features = store.getByObjectIds(List(new ObjectId(slug)))
      features.map({case (key, value) => (key.toString, value)}).toMap
    } else {
      store.getBySlugOrFeatureIds(List(slug))
    }

    // a parse is still a seq of featurematch, bleh
    val parse = Parse[Sorted](featureMap.get(slug).map(servingFeature => {
      FeatureMatch(0, 0, "", servingFeature)
    }).toList)

    responseProcessor.buildFinalParses(List(parse), parseParams, 0)
  }
}