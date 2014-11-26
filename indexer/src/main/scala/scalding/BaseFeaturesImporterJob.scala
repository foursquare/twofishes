// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.twofishes._
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.importers.geonames.GeonamesFeature
import com.foursquare.twofishes.scalding.TwofishesImporterInputSpec
import com.foursquare.twofishes.scalding.DirectoryEnumerationSpec
import com.foursquare.twofishes.DisplayName

class BaseFeaturesImporterJob(
  name: String,
  lineProcessor: (Int, String) => Option[GeonamesFeature],
  allowBuildings: Boolean = false,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  private def getDisplayNamesFromGeonamesFeature(feature: GeonamesFeature): List[DisplayName] = {
    // add primary name as English PREFERRED
    List(DisplayName("en", feature.name, FeatureNameFlags.PREFERRED.getValue)) ++
    // add ascii name as English DEACCENTED
    feature.asciiname.map(n => DisplayName("en", n, FeatureNameFlags.DEACCENT.getValue)).toList ++
    // add country code as abbreviation if this is a country
    (if (feature.featureClass.woeType.getValue == YahooWoeType.COUNTRY.getValue) {
      List(DisplayName("abbr", feature.countryCode, 0))
    } else {
      Nil
    })
  }

  (for {
    line <- lines
    feature <- lineProcessor(0, line)
    // TODO: support ignore list and maybe config.shouldParseBuildings
    if feature.shouldIndex && (!feature.featureClass.isBuilding || allowBuildings)
  } yield {
    val geocodeRecord = GeocodeRecord(
      feature.featureId.longId,
      names = Nil,
      cc = feature.countryCode,
      _woeType = feature.featureClass.woeType.getValue,
      lat = feature.latitude,
      lng = feature.longitude,
      // TODO: populate parents
      parents = Nil,
      population = feature.population,
      // add basic minimal set of names from feature which might be removed/demoted later when merging alternate names
      displayNames = getDisplayNamesFromGeonamesFeature(feature),
      // joins
      boost = None,
      boundingbox = None,
      displayBounds = None
      // TODO: attributes? geometries?
    )
    val servingFeature = geocodeRecord.toGeocodeServingFeature()
    (new LongWritable(servingFeature.longId) -> servingFeature)
  }).write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](outputPath)))
}

class GeonamesFeaturesImporterJob(args: Args) extends BaseFeaturesImporterJob(
  name = "geonames_features_import",
  lineProcessor = GeonamesFeature.parseFromAdminLine,
  allowBuildings = false,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq("downloaded/allCountries.txt"),
    directories = Nil),
  args = args
)

class SupplementalFeaturesImporterJob(args: Args) extends BaseFeaturesImporterJob(
  name = "supplemental_features_import",
  lineProcessor = GeonamesFeature.parseFromAdminLine,
  allowBuildings = true,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Nil,
    directories = Seq(
      DirectoryEnumerationSpec("computed/features"),
      DirectoryEnumerationSpec("private/features"))),
  args = args
)

class PostalCodeFeaturesImporterJob(args: Args) extends BaseFeaturesImporterJob(
  name = "postcode_features_import",
  lineProcessor = GeonamesFeature.parseFromPostalCodeLine,
  allowBuildings = false,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq("downloaded/zip/allCountries.txt"),
    directories = Nil),
  args = args
)