// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._

object FeatureMergers {

  def preEditFeaturesMerger(features: Seq[GeocodeServingFeature]): GeocodeServingFeature = {
    // if there aren't 8 features, throw
    if (features.size != 8) {
      throw new Exception("Incorrect number of features to merge")
    } else {
      val bboxes = features(0)
      val displayBboxes = features(1)
      val extraRelations = features(2)
      val boosts = features(3)
      val parents = features(4)
      val concordances = features(5)
      val slugs = features(6)
      val altNames = features(7)

      bboxes.copy(
        scoringFeatures = bboxes.scoringFeatures.copy(
          boost = boosts.scoringFeatures.boostOrNull,
          parentIds = parents.scoringFeatures.parentIdsOrNull,
          // hasPoly
          extraRelationIds = extraRelations.scoringFeatures.extraRelationIdsOrNull),
        feature = bboxes.feature.copy(
          geometry = bboxes.feature.geometry.copy(
            displayBounds = displayBboxes.feature.geometry.displayBoundsOrNull),// wkbGeometry, source
          ids = concordances.feature.idsOrNull,
          names = altNames.feature.namesOrNull,
          slug = slugs.feature.slugOrNull),
        slugs = slugs.slugsOrNull
      )
    }
  }

  def postEditFeaturesMerger(features: Seq[GeocodeServingFeature]): GeocodeServingFeature = {
    // if there aren't 3 features, throw
    if (features.size != 3) {
      throw new Exception("Incorrect number of features to merge")
    } else {
      val ignores = features(0)
      val moves = features(1)
      val names = features(2)

      ignores.copy(
        feature = ignores.feature.copy(
          geometry = moves.feature.geometry,
          names = names.feature.namesOrNull)
      )
    }
  }

  def preIndexBuildFeaturesMerger(features: Seq[GeocodeServingFeature]): GeocodeServingFeature = {
    // if there aren't 2 features, throw
    if (features.size != 2) {
      throw new Exception("Incorrect number of features to merge")
    } else {
      val polygons = features(0)
      val attributes = features(1)

      polygons.copy(
        feature = polygons.feature.copy(
          attributes = attributes.feature.attributesOrNull)
      )
    }
  }
}