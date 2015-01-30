// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.GeoTools
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.io.WKBReader
import org.apache.hadoop.io.LongWritable

class BaseParentlessFeatureParentMatchingIntermediateJob(
  name: String,
  featureSources: Seq[String],
  revgeoIndexSources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, ParentMatchingValues](featureSources).group
  val revgeoIndex = getJobOutputsAsTypedPipe[LongWritable, CellGeometries](revgeoIndexSources).group

  def getParentIds(matchingValue: ParentMatchingValue, candidates: Seq[CellGeometry]): Seq[Long] = {
    val wkbReader = new WKBReader()
    for {
      center <- matchingValue.centerOption.toList
      preparedCenterGeom = PreparedGeometryFactory.prepare(GeoTools.pointToGeometry(center))
      woeType <- matchingValue.woeTypeOption.toList
      candidate <- candidates
      if (candidate.fullOption.has(true) || preparedCenterGeom.within(wkbReader.read(candidate.wkbGeometryByteArray)))
      // TODO(rahul): use woeType intelligently
      parentId <- candidate.longIdOption
    } yield {
      parentId
    }
  }

  val joined = revgeoIndex.join(features)
  (for {
    (k, (cellGeometries, matchingValues)) <- joined
    matchingValue <- matchingValues.values
    featureId <- matchingValue.featureIdOption.toList
    candidates = cellGeometries.cells
    parentId <- getParentIds(matchingValue, candidates)
  } yield {
    (new LongWritable(featureId) -> IntermediateDataContainer.newBuilder.longValue(parentId).result)
  }).group
    .toList
    .mapValues({parents: List[IntermediateDataContainer] => {
      IntermediateDataContainer.newBuilder.longList(parents.flatMap(_.longValueOption).distinct).result
    }})
    .write(TypedSink[(LongWritable, IntermediateDataContainer)](SpindleSequenceFileSource[LongWritable, IntermediateDataContainer](outputPath)))
}
