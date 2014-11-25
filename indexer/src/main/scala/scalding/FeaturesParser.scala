// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.twofishes.{GeocodeRecord, GeocodeServingFeature}
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.importers.geonames.GeonamesFeature

class FeaturesParser(
  name: String,
  allowBuildings: Boolean = false,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  (for {
    line <- lines
    feature <- GeonamesFeature.parseFromAdminLine(0, line)
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
      parents = Nil,
      population = feature.population,
      displayNames = Nil,
      boost = None,
      boundingbox = None,
      displayBounds = None
    )
    val servingFeature = geocodeRecord.toGeocodeServingFeature()
    (new LongWritable(servingFeature.longId) -> servingFeature)
  }).write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](outputPath)))
}

class GeonamesFeaturesParser(args: Args) extends FeaturesParser(
  name = "geonames_features_import",
  allowBuildings = false,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq("downloaded/allCountries.txt"),
    directories = Nil),
  args = args
)

class SupplementalFeaturesParser(args: Args) extends FeaturesParser(
  name = "supplemental_features_import",
  allowBuildings = true,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Nil,
    directories = Seq(
      DirectoryEnumerationSpec("computed/features"),
      DirectoryEnumerationSpec("private/features"))),
  args = args
)