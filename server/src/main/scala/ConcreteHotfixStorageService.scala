// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util._
import com.google.common.geometry.S2CellId
import com.vividsolutions.jts.geom.{Geometry, Point}
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import com.weiglewilczek.slf4s.Logging
import java.nio.ByteBuffer
import org.geotools.geojson.geom.GeometryJSON

class ConcreteHotfixStorageService(
  source: HotfixSource,
  underlying: GeocodeStorageReadService
) extends HotfixStorageService
    with Logging
    with S2CoveringConstants
    with RevGeoConstants {
  var idsToAddByName = Map.empty[String, Seq[StoredFeatureId]]
  var idsToRemoveByName = Map.empty[String, Seq[StoredFeatureId]]
  var idsToAddByNamePrefix = Map.empty[String, Seq[StoredFeatureId]]
  var idsToRemoveByNamePrefix = Map.empty[String, Seq[StoredFeatureId]]

  var addedOrModifiedFeatureLongIds = Set.empty[Long]
  var deletedFeatureLongIds = Set.empty[Long]

  var addedOrModifiedPolygonFeatureLongIds = Set.empty[Long]
  var deletedPolygonFeatureLongIds = Set.empty[Long]

  var featureIndex = Map.empty[Long, GeocodeServingFeature]
  var polygonIndex = Map.empty[Long, Geometry]
  var s2CoveringIndex = Map.empty[Long, Seq[Long]]
  var s2InteriorIndex = Map.empty[Long, Seq[Long]]
  var s2Index = Map.empty[Long, Seq[CellGeometry]]

  var newSlugIndex = Map.empty[String, Long]

  val wktReader = new WKTReader()
  val wkbWriter = new WKBWriter()
  val geometryJSON = new GeometryJSON()

  def processLongListEdits(list: Seq[Long], edits: Seq[LongListEdit]): Seq[Long] = {
    var listCopy = list

    edits.foreach(edit => {
      edit.editType match {
        case EditType.Add => listCopy = listCopy.filterNot(_ == edit.value) :+ edit.value
        case EditType.Remove => listCopy = listCopy.filterNot(_ == edit.value)
        case _ =>
      }
    })
    listCopy
  }

  def processStringListEdits(list: Seq[String], edits: Seq[StringListEdit]): Seq[String] = {
    var listCopy = list
    edits.foreach(edit => {
      edit.editType match {
        case EditType.Add => listCopy = listCopy.filterNot(_ == edit.value) :+ edit.value
        case EditType.Remove => listCopy = listCopy.filterNot(_ == edit.value)
        case _ =>
      }
    })
    listCopy
  }

  def processFeatureNameFlagsListEdits(list: Seq[FeatureNameFlags], edits: Seq[FeatureNameFlagsListEdit]): Seq[FeatureNameFlags] = {
    var listCopy = list
    edits.foreach(edit => {
      edit.editType match {
        case EditType.Add => listCopy = listCopy.filterNot(_ == edit.value) :+ edit.value
        case EditType.Remove => listCopy = listCopy.filterNot(_ == edit.value)
        case _ =>
      }
    })
    listCopy
  }

  def addToNameIndex(id: StoredFeatureId, edit: FeatureNameListEdit) {
    val normalizedName = NameNormalizer.normalize(edit.name)
    val ids = idsToAddByName.getOrElse(normalizedName, Nil).filterNot(_ == id) :+ id
    idsToAddByName = idsToAddByName + (normalizedName -> ids)
  }

  def maybeAddToPrefixIndex(id: StoredFeatureId, edit: FeatureNameListEdit) {
    val normalizedName = NameNormalizer.normalize(edit.name)
    // NOTE(rahul): Strictly speaking, we add to the prefix index only when the PREFERRED or ALT_NAME flags
    // are present on a name in English or a local language.
    // To keep logic here simple, we unconditionally add to the prefix index when these flags are present, regardless
    // of language.
    val shouldAddToPrefixIndex = edit.flagsEdits.exists(e => {
      (e.editType == EditType.Add) &&
        (e.value == FeatureNameFlags.PREFERRED || e.value == FeatureNameFlags.ALT_NAME)
    })
    if (shouldAddToPrefixIndex) {
      for {
        length <- 1 to normalizedName.size
        prefix = normalizedName.substring(0, length)
      } {
        val prefixIds = idsToAddByNamePrefix.getOrElse(prefix, Nil).filterNot(_ == id) :+ id
        idsToAddByNamePrefix = idsToAddByNamePrefix + (prefix -> prefixIds)
      }
    }
  }

  def processFeatureNameListEdits(id: StoredFeatureId, list: Seq[FeatureName], edits: Seq[FeatureNameListEdit]): Seq[FeatureName] = {
    var listCopy = list
    edits.foreach(edit => {
      edit.editType match {
        case EditType.Add => {
          // TODO(rahul): compute LOCAL_LANG flag?
          listCopy = listCopy.filterNot(n => (n.name == edit.name && n.lang == edit.lang)) :+
            (FeatureName.newBuilder
              .name(edit.name)
              .lang(edit.lang)
              .flags(processFeatureNameFlagsListEdits(Nil, edit.flagsEdits))
              .result)

          // NOTE(rahul): By adding prefixes of this name to the hotfix name index, we
          // guarantee that it will be considered in autocomplete ranking for all prefixes
          // But once the change is removed from hotfixes and rolled into the next index build proper,
          // it has to compete for its place in the prefix index alongside all other features,
          // meaning it will likely not make it into the index for all prefixes.
          // I don't see an easy way out of this right now, though we can probably live with it anyway.
          addToNameIndex(id, edit)
          maybeAddToPrefixIndex(id, edit)
        }
        case EditType.Remove => {
          // support the * wildcard for deleting a name in all languages
          def removeNameFromList(nameToRemove: String, nameList: Seq[FeatureName]): Seq[FeatureName] = {
            nameList.filterNot(n => (n.name == nameToRemove && (n.lang == edit.lang || edit.lang == "*")))
          }
          listCopy = removeNameFromList(edit.name, listCopy)
          // remove deaccented name if different
          val deaccentedName = NameNormalizer.deaccent(edit.name)
          if (deaccentedName != edit.name) {
            listCopy = removeNameFromList(deaccentedName, listCopy)
          }

          // TODO(rahul): handle removing names and prefixes properly?
          // When a name is deleted, the name and prefix indexes should ideally be updated so that
          // for the given name and each of its prefixes, the id is removed from the list of ids
          // PROVIDED the feature doesn't have the same name in another language.
          // However, this is more trouble than it is worth as it requires calling the underlying
          // feature store and enumerating all the names of the feature and checking if there are
          // identical names in other languages. The extra id does no harm because the
          // feature will not actually have the name on it and will fail downstream checks.
          // Allow for this minor discrepancy between the name and feature indexes since this is
          // meant to be a hotfix, and should ideally be rolled into the next index build.
        }
        case EditType.Modify => {
          val existingNameOpt = listCopy.find(n => (n.name == edit.name && n.lang == edit.lang))
          existingNameOpt.foreach(existingName => {
            listCopy = listCopy.filterNot(n => (n.name == edit.name && n.lang == edit.lang)) :+
              (FeatureName.newBuilder
                .name(edit.name)
                .lang(edit.lang)
                .flags(processFeatureNameFlagsListEdits(existingName.flags, edit.flagsEdits))
                .result)
          })

          maybeAddToPrefixIndex(id, edit)
          // TODO(rahul): update prefix index for removal of PREFERRED/ALT_NAME flags?
          // Removing ids is generally more trouble than it is worth and does not actually affect
          // autocomplete ranking (see above).
        }
        case _ =>
      }
    })
    listCopy
  }

  def addCellGeometryToS2Index(cellId: Long, cellGeometry: CellGeometry) {
    val list = s2Index.getOrElse(cellId, Nil) :+ cellGeometry
    s2Index = s2Index + (cellId -> list)
  }

  def init() {
    for {
      edit <- source.getEdits
      if edit.editType != EditType.NoOp
    } {
      if (edit.editType == EditType.Remove) {
        // TODO(rahul): handle removing names and prefixes properly?
        // When a feature is deleted, the name and prefix indexes should ideally be updated so that
        // for each of its names (and each prefix of each name), its id is removed from the list of ids.
        // However, this is more trouble than it is worth as it requires calling the underlying
        // feature store and enumerating all the names of the feature. The extra id does no harm
        // because no feature can be retrieved from the feature index for this id.
        // Allow for this minor discrepancy between the name and feature indexes since this is
        // meant to be a hotfix, and should ideally be rolled into the next index build.
        deletedFeatureLongIds += edit.longId
        deletedPolygonFeatureLongIds += edit.longId
      } else {
        val servingFeatureMutableOpt = edit.editType match {
          case EditType.Add => Some(
            GeocodeServingFeature.newBuilder
              .longId(edit.longId)
              // set canGeocode to true otherwise it defeats the purpose of adding a feature
              .scoringFeatures(ScoringFeatures.newBuilder.canGeocode(true).result)
              .feature(
                GeocodeFeature.newBuilder
                  .longId(edit.longId)
                  .cc("")
                  .geometry(FeatureGeometry.newBuilder.center(GeocodePoint(0.0, 0.0)).result)
                  .result
              )
              .resultMutable)
          case EditType.Modify => for {
              id <- StoredFeatureId.fromLong(edit.longId)
              (resultId, feature) <- underlying.getByFeatureIds(List(id)).toList.headOption
            } yield {
              feature.mutableCopy
            }
          case _ => None
        }

        servingFeatureMutableOpt.foreach(servingFeatureMutable => {
          // scoringFeaturesCreateOrMerge
          if (edit.scoringFeaturesCreateOrMergeIsSet) {
            servingFeatureMutable.scoringFeatures_=({
              val copy = edit.scoringFeaturesCreateOrMergeOrThrow.mutableCopy
              copy.merge(servingFeatureMutable.scoringFeatures)
              copy
            })
          }

          // extraRelationsEdits
          if (edit.extraRelationsEditsIsSet) {
            val extraRelations = processLongListEdits(
              servingFeatureMutable.scoringFeatures.extraRelationIds,
              edit.extraRelationsEditsOrThrow)
            servingFeatureMutable.scoringFeatures_=({
              servingFeatureMutable.scoringFeatures.toBuilder
                .extraRelationIds(extraRelations)
                .result
            })
          }

          val featureMutable = servingFeatureMutable.feature.mutableCopy
          // cc
          if (edit.ccIsSet) {
            featureMutable.cc_=(edit.ccOrThrow)
          }

          // woeType
          if (edit.woeTypeIsSet) {
            featureMutable.woeType_=(edit.woeTypeOrThrow)
          }

          // role
          if (edit.roleIsSet) {
            featureMutable.role_=(edit.roleOrThrow)
          }

          // attributesCreateOrMerge
          if (edit.attributesCreateOrMergeIsSet) {
            featureMutable.attributes_=(if (featureMutable.attributesIsSet) {
              val copy = edit.attributesCreateOrMergeOrThrow.mutableCopy
              copy.merge(featureMutable.attributesOrThrow)
              copy
            } else {
              edit.attributesCreateOrMergeOrThrow
            })
          }

          // urlsEdits
          if (edit.urlsEditsIsSet) {
            val urls = processStringListEdits(
              if (featureMutable.attributesIsSet) {
                featureMutable.attributesOrThrow.urls
              } else {
                Nil
              },
              edit.urlsEditsOrThrow)
            featureMutable.attributes_=({
              val attributesBuilder = if (featureMutable.attributesIsSet) {
                featureMutable.attributesOrThrow.toBuilder
              } else {
                GeocodeFeatureAttributes.newBuilder
              }
              attributesBuilder
                .urls(urls)
                .result
            })
          }

          // parentIdsEdits
          // edit both feature and servingFeature.scoringFeatures
          if (edit.parentIdsEditsIsSet) {
            val parentIds =
              processLongListEdits(
                servingFeatureMutable.scoringFeatures.parentIds,
                edit.parentIdsEditsOrThrow
              )
            featureMutable.parentIds_=(parentIds)

            servingFeatureMutable.scoringFeatures_=({
              servingFeatureMutable.scoringFeatures.toBuilder
                .parentIds(parentIds)
                .result
            })
          }

          // slug
          if (edit.slugIsSet) {
            val slug = edit.slugOrThrow
            featureMutable.slug_=(slug)
            newSlugIndex = newSlugIndex + (slug -> edit.longId)
          }

          val geometryMutable = featureMutable.geometry.mutableCopy
          // center
          if (edit.centerIsSet) {
            geometryMutable.center_=(edit.centerOrThrow)
          }

          // bounds
          if (edit.boundsIsSet) {
            geometryMutable.bounds_=(edit.boundsOrThrow)
          }

          // displayBounds
          if (edit.displayBoundsIsSet) {
            geometryMutable.displayBounds_=(edit.displayBoundsOrThrow)
          }

          // wktGeometry or geojsonGeometry
          if (edit.wktGeometryIsSet || edit.geojsonGeometryIsSet) {
            def setHasPolyRankingFeature(hasPoly: Boolean) {
              servingFeatureMutable.scoringFeatures_=({
                servingFeatureMutable.scoringFeatures.toBuilder
                  .hasPoly(hasPoly)
                  .result
              })
            }

            if ((edit.wktGeometryIsSet && edit.wktGeometryOrThrow.isEmpty) ||
                (edit.geojsonGeometryIsSet && edit.geojsonGeometryOrThrow.isEmpty)) {
              deletedPolygonFeatureLongIds += edit.longId
              setHasPolyRankingFeature(false)
            } else {
              val geometry = if (edit.wktGeometryIsSet) {
                wktReader.read(edit.wktGeometryOrThrow)
              } else {
                geometryJSON.read(edit.geojsonGeometryOrThrow)
              }
              addedOrModifiedPolygonFeatureLongIds += edit.longId
              polygonIndex = polygonIndex + (edit.longId -> geometry)
              setHasPolyRankingFeature(true)
              geometryMutable.source_=("hotfix")

              val s2Covering = GeometryUtils.s2PolygonCovering(
                geometry, minS2LevelForS2Covering, maxS2LevelForS2Covering,
                levelMod = Some(defaultLevelModForS2Covering),
                maxCellsHintWhichMightBeIgnored = Some(defaultMaxCellsHintForS2Covering)
              ).toList.map(_.id())
              s2CoveringIndex = s2CoveringIndex + (edit.longId -> s2Covering)

              val s2Interior = GeometryUtils.s2PolygonCovering(
                geometry, minS2LevelForS2Covering, maxS2LevelForS2Covering,
                levelMod = Some(defaultLevelModForS2Covering),
                maxCellsHintWhichMightBeIgnored = Some(defaultMaxCellsHintForS2Covering),
                interior = true
              ).toList.map(_.id())
              s2InteriorIndex = s2InteriorIndex + (edit.longId -> s2Interior)

              val s2CoveringForRevGeo = GeometryUtils.s2PolygonCovering(
                geometry, minS2LevelForRevGeo, maxS2LevelForRevGeo,
                levelMod = Some(defaultLevelModForRevGeo),
                maxCellsHintWhichMightBeIgnored = Some(defaultMaxCellsHintForRevGeo)
              )

              s2CoveringForRevGeo.foreach((cellId: S2CellId) => {
                val cellGeometry = if (geometry.isInstanceOf[Point]) {
                  CellGeometry(ByteBuffer.wrap(wkbWriter.write(geometry)), featureMutable.woeType, full = false, edit.longId)
                } else {
                  val shape = geometry.buffer(0)
                  val preparedShape = PreparedGeometryFactory.prepare(shape)
                  val s2shape = ShapefileS2Util.fullGeometryForCell(cellId)
                  if (preparedShape.contains(s2shape)) {
                    CellGeometry.newBuilder
                      .wkbGeometry(None)
                      .woeType(featureMutable.woeType)
                      .full(true)
                      .longId(edit.longId)
                      .result
                  } else {
                    val intersection = s2shape.intersection(shape)
                    val geomToIndex = if (intersection.getGeometryType == "GeometryCollection") {
                      GeometryCleanupUtils.cleanupGeometryCollection(intersection)
                    } else {
                      intersection
                    }
                    CellGeometry(ByteBuffer.wrap(wkbWriter.write(geomToIndex)), featureMutable.woeType, full = false, edit.longId)
                  }
                }
                addCellGeometryToS2Index(cellId.id(), cellGeometry)
              })
            }
          }

          // namesEdits
          if (edit.namesEditsIsSet) {
            val idOpt = StoredFeatureId.fromLong(edit.longId)
            idOpt.foreach(id => {
              val names = processFeatureNameListEdits(id, featureMutable.names, edit.namesEditsOrThrow)
              featureMutable.names_=(names)
            })
          }

          featureMutable.geometry_=(geometryMutable)

          servingFeatureMutable.feature_=(featureMutable)
          addedOrModifiedFeatureLongIds += edit.longId
          featureIndex = featureIndex + (edit.longId -> servingFeatureMutable)
        })
      }
    }
  }

  def getIdsToAddByName(name: String): Seq[StoredFeatureId] = idsToAddByName.getOrElse(name, Nil)
  def getIdsToRemoveByName(name: String): Seq[StoredFeatureId] = idsToRemoveByName.getOrElse(name, Nil)
  def getIdsToAddByNamePrefix(name: String): Seq[StoredFeatureId] = idsToAddByNamePrefix.getOrElse(name, Nil)
  def getIdsToRemoveByNamePrefix(name: String): Seq[StoredFeatureId] = idsToRemoveByNamePrefix.getOrElse(name, Nil)

  def getAddedOrModifiedFeatureLongIds: Seq[Long] = addedOrModifiedFeatureLongIds.toSeq
  def getDeletedFeatureLongIds: Seq[Long] = deletedFeatureLongIds.toSeq

  def getByFeatureId(id: StoredFeatureId): Option[GeocodeServingFeature] = featureIndex.get(id.longId)

  def getAddedOrModifiedPolygonFeatureLongIds: Seq[Long] = addedOrModifiedPolygonFeatureLongIds.toSeq
  def getDeletedPolygonFeatureLongIds(): Seq[Long] = deletedPolygonFeatureLongIds.toSeq

  def getCellGeometriesByS2CellId(id: Long): Seq[CellGeometry] = s2Index.getOrElse(id, Nil)
  def getPolygonByFeatureId(id: StoredFeatureId): Option[Geometry] = polygonIndex.get(id.longId)
  def getS2CoveringByFeatureId(id: StoredFeatureId): Option[Seq[Long]] = s2CoveringIndex.get(id.longId)
  def getS2InteriorByFeatureId(id: StoredFeatureId): Option[Seq[Long]] = s2InteriorIndex.get(id.longId)

  def resolveNewSlugToLongId(slug: String): Option[Long] = newSlugIndex.get(slug)

  def refresh() {
    idsToAddByName = Map.empty[String, Seq[StoredFeatureId]]
    idsToRemoveByName = Map.empty[String, Seq[StoredFeatureId]]
    idsToAddByNamePrefix = Map.empty[String, Seq[StoredFeatureId]]
    idsToRemoveByNamePrefix = Map.empty[String, Seq[StoredFeatureId]]

    addedOrModifiedFeatureLongIds = Set.empty[Long]
    deletedFeatureLongIds = Set.empty[Long]

    addedOrModifiedPolygonFeatureLongIds = Set.empty[Long]
    deletedPolygonFeatureLongIds = Set.empty[Long]

    featureIndex = Map.empty[Long, GeocodeServingFeature]
    polygonIndex = Map.empty[Long, Geometry]
    s2CoveringIndex = Map.empty[Long, Seq[Long]]
    s2InteriorIndex = Map.empty[Long, Seq[Long]]
    s2Index = Map.empty[Long, Seq[CellGeometry]]

    newSlugIndex = Map.empty[String, Long]

    source.refresh()
    init()
  }

  init()
}
