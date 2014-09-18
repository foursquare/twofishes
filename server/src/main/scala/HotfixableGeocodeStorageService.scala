// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.util.Lists.Implicits._
import com.foursquare.twofishes.util.StoredFeatureId
import com.vividsolutions.jts.geom.Geometry
import com.weiglewilczek.slf4s.Logging

class HotfixableGeocodeStorageService(
  underlying: GeocodeStorageReadService,
  hotfix: HotfixStorageService
) extends GeocodeStorageReadService with Logging {

  def getIdsByName(name: String): Seq[StoredFeatureId] = {
    (underlying.getIdsByName(name) ++ hotfix.getIdsToAddByName(name))
      .filterNot(hotfix.getIdsToRemoveByName(name).has)
      .distinct
  }

  def getIdsByNamePrefix(name: String): Seq[StoredFeatureId] = {
    (underlying.getIdsByNamePrefix(name) ++ hotfix.getIdsToAddByNamePrefix(name))
      .filterNot(hotfix.getIdsToRemoveByNamePrefix(name).has)
      .distinct
  }

  def getByName(name: String): Seq[GeocodeServingFeature] = {
    getByFeatureIds(getIdsByName(name)).map(_._2).toSeq
  }

  def getByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, GeocodeServingFeature] = {
    // filter results rather than incoming list of ids in order to correctly handle concordance
    (underlying.getByFeatureIds(ids)
      .filterNot({case (id, feature) => hotfix.getAddedOrModifiedFeatureLongIds.has(feature.longId)}) ++
      (for {
        id <- ids
        feature <- hotfix.getByFeatureId(id).toList
      } yield {
        (id -> feature)
      }).toMap
    ).filterNot({case (id, feature) => hotfix.getDeletedFeatureLongIds.has(feature.longId)})
  }

  def getBySlugOrFeatureIds(ids: Seq[String]): Map[String, GeocodeServingFeature] = {
    // the slug case is a little complicated since slugs are auto-generated and not meant to change
    // we do not want to have to deal with assigning slugs to newly created features, so we will only
    // support looking them up by id
    // the simplest way to do this is by converting the longIds that come back from the underlying
    // slug lookup into StoredFeatureIds, adding on any featureIds that are newly added, and then
    // calling getByFeatureIds
    val existingIdFidMap = underlying.getBySlugOrFeatureIds(ids)
      .map({case (idString, feature) => (idString -> StoredFeatureId.fromLong(feature.longId))})
      .toMap
    val newIdFidMap = (for {
      idString <- ids
      id <- StoredFeatureId.fromUserInputString(idString)
      // filter out ids that already exist
      if !(existingIdFidMap.contains(idString))
    } yield {
      (idString -> Some(id))
    }).toMap
    (for {
      (idString, featureIdOpt) <- existingIdFidMap ++ newIdFidMap
      featureId <- featureIdOpt
      (resultFeatureId, feature) <- getByFeatureIds(Seq(featureId)).headOption
    } yield {
      (idString -> feature)
    }).toMap
  }

  def getMinS2Level: Int = underlying.getMinS2Level
  def getMaxS2Level: Int = underlying.getMaxS2Level
  def getLevelMod: Int = underlying.getLevelMod

  def getByS2CellId(id: Long): Seq[CellGeometry] = {
    (underlying.getByS2CellId(id)
      .filterNot(cellGeometry => hotfix.getAddedOrModifiedPolygonFeatureLongIds.has(cellGeometry.longId)) ++
      hotfix.getCellGeometriesByS2CellId(id)
    ).filterNot(cellGeometry => hotfix.getDeletedPolygonFeatureLongIds.has(cellGeometry.longId))
  }

  def getPolygonByFeatureId(id: StoredFeatureId): Option[Geometry] = {
    if (hotfix.getDeletedPolygonFeatureLongIds.has(id.longId)) {
      None
    } else if (hotfix.getAddedOrModifiedPolygonFeatureLongIds.has(id.longId)) {
      hotfix.getPolygonByFeatureId(id)
    } else {
      underlying.getPolygonByFeatureId(id)
    }
  }

  def getPolygonByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Geometry] = {
    (for {
      id <- ids
      polygon <- getPolygonByFeatureId(id)
    } yield {
      (id -> polygon)
    }).toMap
  }

  def getS2CoveringByFeatureId(id: StoredFeatureId): Option[Seq[Long]] = {
    if (hotfix.getDeletedPolygonFeatureLongIds.has(id.longId)) {
      None
    } else if (hotfix.getAddedOrModifiedPolygonFeatureLongIds.has(id.longId)) {
      hotfix.getS2CoveringByFeatureId(id)
    } else {
      underlying.getS2CoveringByFeatureId(id)
    }
  }

  def getS2CoveringByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Seq[Long]] = {
    (for {
      id <- ids
      covering <- getS2CoveringByFeatureId(id)
    } yield {
      (id -> covering)
    }).toMap
  }
}
