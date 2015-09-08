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
    // first resolve newly created/modified slugs, if any
    val changedSlugFidMap = (for {
      idString <- ids
      longId <- hotfix.resolveNewSlugToLongId(idString)
      fid <- StoredFeatureId.fromLong(longId)
    } yield {
      (idString -> Some(fid))
    }).toMap

    // next, call underlying lookup to resolve any unchanged slugs/ids
    val existingIdFidMap = underlying.getBySlugOrFeatureIds(ids.filterNot(changedSlugFidMap.contains))
      .map({case (idString, feature) => (idString -> StoredFeatureId.fromLong(feature.longId))})
      .toMap

    // any valid id that is in neither of the above could only be newly created and without a slug
    val newIdFidMap = (for {
      idString <- ids
      id <- StoredFeatureId.fromUserInputString(idString)
      // filter out ids that already exist
      if !(existingIdFidMap.contains(idString) || changedSlugFidMap.contains(idString))
    } yield {
      (idString -> Some(id))
    }).toMap

    // put them all together
    (for {
      (idString, featureIdOpt) <- changedSlugFidMap ++ existingIdFidMap ++ newIdFidMap
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

  def getS2InteriorByFeatureId(id: StoredFeatureId): Option[Seq[Long]] = {
    if (hotfix.getDeletedPolygonFeatureLongIds.has(id.longId)) {
      None
    } else if (hotfix.getAddedOrModifiedPolygonFeatureLongIds.has(id.longId)) {
      hotfix.getS2InteriorByFeatureId(id)
    } else {
      underlying.getS2InteriorByFeatureId(id)
    }
  }

  def getS2InteriorByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Seq[Long]] = {
    (for {
      id <- ids
      covering <- getS2InteriorByFeatureId(id)
    } yield {
      (id -> covering)
    }).toMap
  }

  def refresh() {
    // refresh underlying first so hotfixes are applied on top of latest data
    underlying.refresh()
    hotfix.refresh()
  }
}
