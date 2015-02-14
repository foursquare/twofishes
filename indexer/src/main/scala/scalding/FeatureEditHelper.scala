// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._
import java.nio.ByteBuffer
import com.foursquare.twofishes.util.NameNormalizer
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import org.geotools.geojson.geom.GeometryJSON

object FeatureEditHelper {

  // TODO(rahul): refactor to share code with ConcreteHotfixStorageService

  def applyEdits(feature: GeocodeServingFeature, edits: GeocodeServingFeatureEdits): Option[GeocodeServingFeature] = {
    applyEdits(feature, edits.edits)
  }

  def applyEdits(feature: GeocodeServingFeature, edits: Seq[GeocodeServingFeatureEdit]): Option[GeocodeServingFeature] = {
    // if there is a Remove edit, return None right away
    if (edits.exists(edit => edit.editType == EditType.Remove)) {
      None
    } else {
      var featureMutable = feature.mutableCopy
      for {
        edit <- edits
        if edit.editType == EditType.Modify
      } {
        featureMutable = applyModifyEdit(featureMutable, edit)
      }
      Some(featureMutable)
    }
  }

  private val wktReader = new WKTReader()
  private val wkbWriter = new WKBWriter()
  private val geometryJSON = new GeometryJSON()

  private def processLongListEdits(list: Seq[Long], edits: Seq[LongListEdit]): Seq[Long] = {
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

  private def processStringListEdits(list: Seq[String], edits: Seq[StringListEdit]): Seq[String] = {
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

  private def processFeatureNameFlagsListEdits(list: Seq[FeatureNameFlags], edits: Seq[FeatureNameFlagsListEdit]): Seq[FeatureNameFlags] = {
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

  def processFeatureNameListEdits(list: Seq[FeatureName], edits: Seq[FeatureNameListEdit]): Seq[FeatureName] = {
    var listCopy = list
    edits.foreach(edit => {
      // if the edit adds the PREFERRED flag to a name, clear the PREFERRED flag from all
      // other names in the same language
      if (edit.flagsEdits.exists(flagsEdit => flagsEdit.editType == EditType.Add && flagsEdit.value == FeatureNameFlags.PREFERRED)) {
        listCopy = listCopy.map(name => {
          if (name.lang == edit.lang && name.flags.contains(FeatureNameFlags.PREFERRED)) {
            name.copy(flags = name.flags.filterNot(flag => flag == FeatureNameFlags.PREFERRED))
          } else {
            name
          }
        })
      }

      edit.editType match {
        case EditType.Add => {
          listCopy = listCopy.filterNot(n => (n.name == edit.name && n.lang == edit.lang)) :+
            (FeatureName.newBuilder
              .name(edit.name)
              .lang(edit.lang)
              .flags(processFeatureNameFlagsListEdits(Nil, edit.flagsEdits))
              .result)
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
        }
        case _ =>
      }
    })
    listCopy
  }

  private def applyModifyEdit(
    servingFeatureMutable: MutableGeocodeServingFeature,
    edit: GeocodeServingFeatureEdit
  ): MutableGeocodeServingFeature = {

    // scoringFeaturesCreateOrMerge
    if (edit.scoringFeaturesCreateOrMergeIsSet) {
      servingFeatureMutable.scoringFeatures_=({
        val copy = servingFeatureMutable.scoringFeatures.mutableCopy
        copy.merge(edit.scoringFeaturesCreateOrMergeOrThrow)
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
        val copy = featureMutable.attributesOrThrow.mutableCopy
        copy.merge(edit.attributesCreateOrMergeOrThrow)
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
      val parentIds = processLongListEdits(
        featureMutable.parentIds,
        edit.parentIdsEditsOrThrow)
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
        geometryMutable.wkbGeometryUnset()
        setHasPolyRankingFeature(false)
      } else {
        val geometry = if (edit.wktGeometryIsSet) {
          wktReader.read(edit.wktGeometryOrThrow)
        } else {
          geometryJSON.read(edit.geojsonGeometryOrThrow)
        }
        geometryMutable.wkbGeometry_=(ByteBuffer.wrap(wkbWriter.write(geometry)))
        setHasPolyRankingFeature(true)
        geometryMutable.source_=("index-build-override")
      }
    }

    // namesEdits
    if (edit.namesEditsIsSet) {
      val names = processFeatureNameListEdits(featureMutable.names, edit.namesEditsOrThrow)
      featureMutable.names_=(names)
    }

    featureMutable.geometry_=(geometryMutable)

    servingFeatureMutable.feature_=(featureMutable)

    servingFeatureMutable
  }
}
