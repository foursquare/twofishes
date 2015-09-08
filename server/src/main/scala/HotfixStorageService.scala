// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.util.StoredFeatureId
import com.vividsolutions.jts.geom.Geometry

trait HotfixStorageService {
  def getIdsToAddByName(name: String): Seq[StoredFeatureId]
  def getIdsToRemoveByName(name: String): Seq[StoredFeatureId]
  def getIdsToAddByNamePrefix(name: String): Seq[StoredFeatureId]
  def getIdsToRemoveByNamePrefix(name: String): Seq[StoredFeatureId]

  def getAddedOrModifiedFeatureLongIds(): Seq[Long]
  def getDeletedFeatureLongIds(): Seq[Long]

  def getByFeatureId(id: StoredFeatureId): Option[GeocodeServingFeature]

  def getAddedOrModifiedPolygonFeatureLongIds(): Seq[Long]
  def getDeletedPolygonFeatureLongIds(): Seq[Long]

  def getCellGeometriesByS2CellId(id: Long): Seq[CellGeometry]
  def getPolygonByFeatureId(id: StoredFeatureId): Option[Geometry]
  def getS2CoveringByFeatureId(id: StoredFeatureId): Option[Seq[Long]]
  def getS2InteriorByFeatureId(id: StoredFeatureId): Option[Seq[Long]]

  def resolveNewSlugToLongId(slug: String): Option[Long]

  def refresh(): Unit
}
