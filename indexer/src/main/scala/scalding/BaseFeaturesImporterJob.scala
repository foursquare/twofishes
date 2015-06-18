// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.twofishes._
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.importers.geonames.GeonamesFeature
import com.foursquare.twofishes.util._
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}

class BaseFeaturesImporterJob(
  name: String,
  lineProcessor: (Int, String) => Option[GeonamesFeature],
  allowBuildings: Boolean = false,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  private def getDisplayNamesFromGeonamesFeature(feature: GeonamesFeature): List[DisplayName] = {
    def isAllDigits(x: String) = x.forall(Character.isDigit)

    // add primary name as English PREFERRED
    List(DisplayName("en", feature.name, FeatureNameFlags.PREFERRED.getValue)) ++
    // add ascii name as English DEACCENTED
    feature.asciiname.map(n => DisplayName("en", n, FeatureNameFlags.DEACCENT.getValue)).toList ++
    // add country code as abbreviation if this is a country
    (if (feature.featureClass.woeType.getValue == YahooWoeType.COUNTRY.getValue) {
      List(DisplayName("abbr", feature.countryCode, 0))
    } else {
      Nil
    }) ++
    // the admincode is the internal geonames admin code, but is very often the
    // same short name for the admin area that is actually used in the country
    (if (feature.featureClass.isAdmin1 || feature.featureClass.isAdmin2 || feature.featureClass.isAdmin3) {
      feature.adminCode.toList.flatMap(code => {
        if (!isAllDigits(code)) {
          Some(DisplayName("abbr", code, FeatureNameFlags.ABBREVIATION.getValue))
        } else {
          Some(DisplayName("", code, FeatureNameFlags.NEVER_DISPLAY.getValue))
        }
      })
    } else {
      Nil
    })
  }

  private def getEmbeddedPolygon(feature: GeonamesFeature): Option[Array[Byte]] = {
    feature.extraColumns.get("geometry").map(polygon => {
      val wktReader = new WKTReader()
      val wkbWriter = new WKBWriter()
      wkbWriter.write(wktReader.read(polygon))
    })
  }

  private def getEmbeddedBoundingBox(feature: GeonamesFeature): Option[BoundingBox] = {
    feature.extraColumns.get("bbox").flatMap(bboxStr => {
      // west, south, east, north
      val parts = bboxStr.split(",").map(_.trim)
      parts.toList match {
        case w :: s :: e :: n :: Nil => {
          Some(BoundingBox(Point(n.toDouble, e.toDouble), Point(s.toDouble, w.toDouble)))
        }
        case _ => {
          // logger.error("malformed bbox: " + bboxStr)
          None
        }
      }
    })
  }

  private def getEmbeddedBoost(feature: GeonamesFeature): Option[Int] = {
    feature.extraColumns.get("boost").map(_.toInt)
  }

  val countryInfoCachedFile = getCachedFileByRelativePath("downloaded/countryInfo.txt")
  val adminCodesCachedFile = getCachedFileByRelativePath("downloaded/adminCodes.txt")
  @transient lazy val adminCodeMap = InMemoryLookupTableHelper.buildAdminCodeMap(
    countryInfoCachedFile,
    adminCodesCachedFile)

  private def getEmbeddedParents(feature: GeonamesFeature): List[Long] = {
    def lookupAdminCode(p: String): Option[String] = {
      adminCodeMap.get(p)
    }

    val explicitParents = for {
      parentIdStrings <- feature.extraColumns.get("parents").toList
      fidString <- parentIdStrings.split(",").toList
      fid <- StoredFeatureId.fromHumanReadableString(fidString, Some(GeonamesNamespace))
    } yield {
      fid.longId
    }

    val adminCodeParents = for {
      parentAdminCode <- feature.parents.toList
      fidString <- lookupAdminCode(parentAdminCode)
      fid <- StoredFeatureId.fromHumanReadableString(fidString, Some(GeonamesNamespace))
    } yield {
      fid.longId
    }

    (explicitParents ++ adminCodeParents).distinct
  }

  private def getEmbeddedCanGeocode(feature: GeonamesFeature): Boolean = {
    feature.extraColumns.get("canGeocode").map(_.toInt).getOrElse(1) > 0
  }

  private def getEmbeddedAttributes(feature: GeonamesFeature): Option[GeocodeFeatureAttributes] = {
    var attributesSet = false
    lazy val attributesBuilder = {
      attributesSet = true
      GeocodeFeatureAttributes.newBuilder
    }

    if (feature.featureClass.isAdmin1Capital) {
      attributesBuilder.adm1cap(true)
    }

    feature.population.foreach(pop =>
      attributesBuilder.population(pop)
    )

    feature.extraColumns.get("sociallyRelevant").map(v =>
      attributesBuilder.sociallyRelevant(v.toBoolean)
    )

    feature.extraColumns.get("neighborhoodType").map(v =>
      attributesBuilder.neighborhoodType(NeighborhoodType.findByNameOrNull(v))
    )

    if (attributesSet) {
      Some(attributesBuilder.result)
    } else {
      None
    }
  }

  (for {
    line <- lines
    feature <- lineProcessor(0, line)
    // TODO: support config.shouldParseBuildings
    if feature.isValid && feature.shouldIndex && (!feature.featureClass.isBuilding || allowBuildings)
  } yield {

    val polygonOpt = getEmbeddedPolygon(feature)
    val polygonSource = if (polygonOpt.isDefined) {
      Some("self_point")
    } else {
      None
    }

    val geocodeRecord = GeocodeRecord(
      _id = feature.featureId.longId,
      names = Nil,
      cc = feature.countryCode,
      _woeType = feature.featureClass.woeType.getValue,
      lat = feature.latitude,
      lng = feature.longitude,
      // add basic minimal set of names from feature which might be removed/demoted later when merging alternate names
      displayNames = getDisplayNamesFromGeonamesFeature(feature),
      // add embedded polygon which might be replaced when merging external polygons
      polygon = polygonOpt,
      hasPoly = polygonOpt.isDefined,
      polygonSource = polygonSource,
      // will be supplemented by join with concordances
      ids = List(feature.featureId.longId),
      // will be supplemented by join with boosts
      boost = getEmbeddedBoost(feature),
      // will be supplemented by join with hierarchy
      parents = getEmbeddedParents(feature),
      //unresolvedParentCodes = feature.parents,
      population = feature.population,
      canGeocode = getEmbeddedCanGeocode(feature),
      // add embedded bounding box which might be replaced when merging external bounding boxes/polygons
      boundingbox = getEmbeddedBoundingBox(feature),
      // will be populated by subsequent joins
      displayBounds = None,
      slug = None,
      extraRelations = Nil
    )
    geocodeRecord.setAttributes(getEmbeddedAttributes(feature))

    val servingFeature = geocodeRecord.toGeocodeServingFeature()
    (new LongWritable(servingFeature.longId) -> servingFeature)
  }).group
    .head
    .write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](outputPath)))
}
