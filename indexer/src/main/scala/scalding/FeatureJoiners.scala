// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding._
import com.foursquare.twofishes.util.StoredFeatureId

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

  def parentsJoiner(
    features: Grouped[LongWritable, GeocodeServingFeature],
    hierarchy: Grouped[LongWritable, IntermediateDataContainer]
  ): TypedPipe[(LongWritable, GeocodeServingFeature)] = {
    features.leftJoin(hierarchy)
      .map({case (k: LongWritable, (f: GeocodeServingFeature, containerOpt: Option[IntermediateDataContainer])) => {
      containerOpt match {
        case Some(container) => {
          val parentIds = (f.scoringFeatures.parentIds ++ container.longList).distinct
          (k -> f.copy(scoringFeatures = f.scoringFeatures.copy(parentIds = parentIds)))
        }
        case None =>
          (k -> f)
      }
    }})
  }

  def concordancesJoiner(
    features: Grouped[LongWritable, GeocodeServingFeature],
    concordances: Grouped[LongWritable, IntermediateDataContainer]
  ): TypedPipe[(LongWritable, GeocodeServingFeature)] = {
    features.leftJoin(concordances)
      .map({case (k: LongWritable, (f: GeocodeServingFeature, containerOpt: Option[IntermediateDataContainer])) => {
      containerOpt match {
        case Some(container) => {
          val ids = (for {
            longId <- container.longList
            fid <- StoredFeatureId.fromLong(longId)
          } yield {
            fid.thriftFeatureId
          }).toSeq
          (k -> f.copy(feature = f.feature.copy(ids = f.feature.ids ++ ids)))
        }
        case None =>
          (k -> f)
      }
    }})
  }

  def slugsJoiner(
    features: Grouped[LongWritable, GeocodeServingFeature],
    slugs: Grouped[LongWritable, IntermediateDataContainer]
  ): TypedPipe[(LongWritable, GeocodeServingFeature)] = {
    features.leftJoin(slugs)
      .map({case (k: LongWritable, (f: GeocodeServingFeature, containerOpt: Option[IntermediateDataContainer])) => {
      containerOpt match {
        case Some(container) => {
          val sortedSlugEntries = (for {
            (slug, score, deprecated) <- (container.stringList, container.intList, container.boolList).zipped
          } yield {
            SlugEntry(slug, score, deprecated, permanent = true)
          }).toSeq
          if (sortedSlugEntries.nonEmpty) {
            (k -> f.copy(slugs = sortedSlugEntries.map(_.id), feature = f.feature.copy(slug = sortedSlugEntries.head.id)))
          } else {
            (k -> f)
          }
        }
        case None =>
          (k -> f)
      }
    }})
  }
}
