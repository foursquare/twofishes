// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.twofishes.{GeocodeRecord, GeocodeServingFeature}
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.importers.geonames.GeonamesFeature

class FeaturesParser(args: Args) extends TwofishesJob("features_import", args) {

  val files =
    getFilesByRelativePaths(Seq("downloaded/allCountries.txt")) ++
    getFilesInDirectoryByRelativePath("computed/features") ++
    getFilesInDirectoryByRelativePath("private/features")

  logger.info("Using input files: %s".format(files.toString))

  val lines: TypedPipe[String] = TypedPipe.from(MultipleTextLineFiles(files: _*))

  (for {
    line <- lines
    feature <- GeonamesFeature.parseFromAdminLine(0, line)
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