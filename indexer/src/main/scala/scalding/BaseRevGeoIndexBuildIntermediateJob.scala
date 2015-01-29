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

  def profile[T](f: => T, stat: Stat): T = {
    val (rv, duration) = com.twitter.util.Duration.inMilliseconds(f)
    stat.incBy(duration.inMilliseconds)
    rv
  }

  val bufferStat = Stat("bufferTimeInMs")
  val prepareStat = Stat("prepareTimeInMs")
  val coverStat = Stat("coverTimeInMs")
  val s2GeomStat = Stat("s2GeomTimeInMs")
  val containsStat = Stat("containsTimeInMs")
  val intersectionStat = Stat("intersectionTimeInMs")
  val cleanupStat = Stat("cleanupTimeInMs")
  (for {
    (featureId, servingFeature) <- features
    if servingFeature.feature.geometry.wkbGeometryIsSet
    geometry = new WKBReader().read(servingFeature.feature.geometry.wkbGeometryByteArray)
    bufferedShape = profile({geometry.buffer(0)}, bufferStat)
    preparedShape = profile({PreparedGeometryFactory.prepare(bufferedShape)}, prepareStat)
    woeType = servingFeature.feature.woeTypeOrDefault
    cells = profile({GeometryUtils.s2PolygonCovering(
      geometry,
      RevGeoConstants.minS2LevelForRevGeo,
      RevGeoConstants.maxS2LevelForRevGeo,
      levelMod = Some(RevGeoConstants.defaultLevelModForRevGeo),
      maxCellsHintWhichMightBeIgnored = Some(RevGeoConstants.defaultMaxCellsHintForRevGeo))}, coverStat)
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
      val s2Shape = profile({ShapefileS2Util.fullGeometryForCell(cell)}, s2GeomStat)
      if (profile({preparedShape.contains(s2Shape)}, containsStat)) {
        CellGeometry.newBuilder
          .woeType(woeType)
          .full(true)
          .longId(featureId.get)
          .result
      } else {
        val intersection = profile({s2Shape.intersection(bufferedShape)}, intersectionStat)
        val geomToIndex = if (intersection.getGeometryType == "GeometryCollection") {
          profile({GeometryCleanupUtils.cleanupGeometryCollection(intersection)}, cleanupStat)
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
