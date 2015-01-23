// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.vividsolutions.jts.io.WKBReader
import org.apache.commons.net.util.Base64

class BaseUnmatchedPolygonsS2CoverImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {

  def unflattenNames(namesString: String): Seq[FeatureName] = {
    for {
      nameLangPair <- namesString.split("\\|")
      parts = nameLangPair.split(":")
      lang <- parts.lift(0)
      name <- parts.lift(1)
    } yield {
      FeatureName.newBuilder
        .lang(lang)
        .name(name)
        .result
    }
  }

  def unflattenWoeTypes(woeTypesString: String): Seq[Seq[YahooWoeType]] = {
    woeTypesString.split("\\|").toSeq.map(_.split(",").toSeq.map(YahooWoeType.findByNameOrNull(_)))
  }

  (for {
    line <- lines
    if !line.startsWith("#")
    parts = line.split("\t")
    if parts.size == 6
    polygonId <- Helpers.TryO({ parts(0).toLong }).toList
    featureIdsString = parts(2)
    // no features matched
    if featureIdsString.isEmpty
    names = unflattenNames(parts(3))
    preferredWoeTypes = unflattenWoeTypes(parts(4))
    geometryBase64String = parts(5)
    geometry = new WKBReader().read(Base64.decodeBase64(geometryBase64String))
    (woeTypesAtLevel, preferenceLevel) <- preferredWoeTypes.zipWithIndex
    woeType <- woeTypesAtLevel
    s2Level = PolygonMatchingHelper.getS2LevelForWoeType(woeType)
    s2Cell <- GeometryUtils.s2PolygonCovering(geometry, s2Level, s2Level)
    s2CellId = s2Cell.id
  } yield {
    val matchingValue = PolygonMatchingValue.newBuilder
      .polygonId(polygonId)
      .names(names)
      .polygonWoeTypePreferenceLevel(preferenceLevel)
      .result
    val matchingKey = PolygonMatchingKey(s2CellId, woeType)
    (new PolygonMatchingKeyWritable(matchingKey) -> matchingValue)
  }).group
    .toList
    .mapValues({matchingValues: List[PolygonMatchingValue] => {
      PolygonMatchingValues(matchingValues)
    }})
    .write(TypedSink[(PolygonMatchingKeyWritable, PolygonMatchingValues)](SpindleSequenceFileSource[PolygonMatchingKeyWritable, PolygonMatchingValues](outputPath)))
}
