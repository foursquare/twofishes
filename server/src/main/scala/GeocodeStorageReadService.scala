// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.GeocodeServingFeature
import com.foursquare.twofishes.util.StoredFeatureId
import com.vividsolutions.jts.geom.Geometry

trait GeocodeStorageReadService {
  def getIdsByName(name: String): Seq[StoredFeatureId]
  def getIdsByNamePrefix(name: String): Seq[StoredFeatureId]
  def getByName(name: String): Seq[GeocodeServingFeature]
  def getByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, GeocodeServingFeature]

  def getBySlugOrFeatureIds(ids: Seq[String]): Map[String, GeocodeServingFeature]

  def getMinS2Level: Int
  def getMaxS2Level: Int
  def getLevelMod: Int
  def getByS2CellId(id: Long): Seq[CellGeometry]
  def getPolygonByFeatureId(id: StoredFeatureId): Option[Geometry]
  def getPolygonByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Geometry]

  def hotfixesDeletes: Seq[StoredFeatureId] = Nil
  def hotfixesBoosts: Map[StoredFeatureId, Int] = Map.empty
}