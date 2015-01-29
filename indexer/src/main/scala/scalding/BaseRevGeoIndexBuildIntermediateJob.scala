// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import java.nio.ByteBuffer

import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{GeometryCleanupUtils, GeometryUtils, RevGeoConstants}
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.io.{WKBWriter, WKBReader}
import org.apache.hadoop.io.{Text, LongWritable}

class BaseRevGeoIndexBuildIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources).group

  (for {
    (featureId, servingFeature) <- features
    if servingFeature.feature.geometry.wkbGeometryIsSet
    geometry = new WKBReader().read(servingFeature.feature.geometry.wkbGeometryByteArray)
    bufferedShape = geometry.buffer(0)
    preparedShape = PreparedGeometryFactory.prepare(bufferedShape)
    woeType = servingFeature.feature.woeTypeOrDefault
    cells = GeometryUtils.s2PolygonCovering(
      geometry,
      RevGeoConstants.minS2LevelForRevGeo,
      RevGeoConstants.maxS2LevelForRevGeo,
      levelMod = Some(RevGeoConstants.defaultLevelModForRevGeo),
      maxCellsHintWhichMightBeIgnored = Some(RevGeoConstants.defaultMaxCellsHintForRevGeo))
    cell <- cells
    cellId = cell.id
  } yield {
    val wkbWriter = new WKBWriter()
    val cellGeometry = if (geometry.isInstanceOf[Point]) {
      CellGeometry.newBuilder
        .wkbGeometry(ByteBuffer.wrap(wkbWriter.write(geometry)))
        .woeType(woeType)
        .full(false)
        .longId(featureId.get)
        .result
    } else {
      val s2Shape = ShapefileS2Util.fullGeometryForCell(cell)
      if (preparedShape.contains(s2Shape)) {
        CellGeometry.newBuilder
          .woeType(woeType)
          .full(true)
          .longId(featureId.get)
          .result
      } else {
        val intersection = s2Shape.intersection(bufferedShape)
        val geomToIndex = if (intersection.getGeometryType == "GeometryCollection") {
          GeometryCleanupUtils.cleanupGeometryCollection(intersection)
        } else {
          intersection
        }

        CellGeometry.newBuilder
          .wkbGeometry(ByteBuffer.wrap(wkbWriter.write(geomToIndex)))
          .woeType(woeType)
          .full(false)
          .longId(featureId.get)
          .result
      }
    }

    // HACK: cellIds seem to hash terribly and all go to the same reducer so use Text for now
    (new Text(cellId.toString) -> cellGeometry)
  }).group
    .toList
    .map({case (idText: Text, cells: List[CellGeometry]) => {
      (new LongWritable(idText.toString.toLong) -> CellGeometries(cells))
    }})
    .write(TypedSink[(LongWritable, CellGeometries)](SpindleSequenceFileSource[LongWritable, CellGeometries](outputPath)))
}
