// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding._

object FeatureJoiners {

  def boundingBoxJoiner(
    features: Grouped[LongWritable, GeocodeServingFeature],
    bboxes: Grouped[LongWritable, GeocodeBoundingBox]
  ): TypedPipe[(LongWritable, GeocodeServingFeature)] = {
    features.leftJoin(bboxes)
      .map({case (k: LongWritable, (f: GeocodeServingFeature, bboxOpt: Option[GeocodeBoundingBox])) => {
      bboxOpt match {
        case Some(bbox) =>
          (k -> f.copy(feature = f.feature.copy(geometry = f.feature.geometry.copy(bounds = bbox))))
        case None =>
          (k -> f)
      }
    }})
  }

  def displayBoundingBoxJoiner(
    features: Grouped[LongWritable, GeocodeServingFeature],
    displayBboxes: Grouped[LongWritable, GeocodeBoundingBox]
  ): TypedPipe[(LongWritable, GeocodeServingFeature)] = {
    features.leftJoin(displayBboxes)
      .map({case (k: LongWritable, (f: GeocodeServingFeature, displayBboxOpt: Option[GeocodeBoundingBox])) => {
      displayBboxOpt match {
        case Some(displayBbox) =>
          (k -> f.copy(feature = f.feature.copy(geometry = f.feature.geometry.copy(displayBounds = displayBbox))))
        case None =>
          (k -> f)
      }
    }})
  }

  def extraRelationsJoiner(
    features: Grouped[LongWritable, GeocodeServingFeature],
    extraRelations: Grouped[LongWritable, IntermediateDataContainer]
  ): TypedPipe[(LongWritable, GeocodeServingFeature)] = {
    features.leftJoin(extraRelations)
      .map({case (k: LongWritable, (f: GeocodeServingFeature, containerOpt: Option[IntermediateDataContainer])) => {
      containerOpt match {
        case Some(container) =>
          (k -> f.copy(scoringFeatures = f.scoringFeatures.copy(extraRelationIds = container.longList)))
        case None =>
          (k -> f)
      }
    }})
  }

  def boostsJoiner(
    features: Grouped[LongWritable, GeocodeServingFeature],
    boosts: Grouped[LongWritable, IntermediateDataContainer]
  ): TypedPipe[(LongWritable, GeocodeServingFeature)] = {
    features.leftJoin(boosts)
      .map({case (k: LongWritable, (f: GeocodeServingFeature, containerOpt: Option[IntermediateDataContainer])) => {
      containerOpt match {
        case Some(container) =>
          (k -> f.copy(scoringFeatures = f.scoringFeatures.copy(boost = container.intValue)))
        case None =>
          (k -> f)
      }
    }})
  }
}
