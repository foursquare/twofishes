// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.{GeocodeBoundingBox, GeocodePoint}
import com.foursquare.twofishes.util.{GeonamesNamespace, StoredFeatureId}

class BoundingBoxParser(args: Args) extends TwofishesJob("bbox_import", args) {

  val files =
    getFilesInDirectoryByRelativePath("computed/bboxes") ++
    getFilesInDirectoryByRelativePath("private/bboxes")

  logger.info("Using input files: %s".format(files.toString))

  val lines: TypedPipe[String] = TypedPipe.from(MultipleTextLineFiles(files: _*))

  lines.filterNot(_.startsWith("#")).flatMap(line => {
    val parts = line.split("[\t ]")
    // 0: geonameid
    // 1->5:       // west, south, east, north
    if (parts.size != 5) {
      // logger.error("wrong # of parts: %d vs %d in %s".format(parts.size, 5, line))
      None
    } else {
      try {
        val id = parts(0)
        val w = parts(1).toDouble
        val s = parts(2).toDouble
        val e = parts(3).toDouble
        val n = parts(4).toDouble
        StoredFeatureId.fromHumanReadableString(id, Some(GeonamesNamespace)) match {
          case Some(fid) => {
            Some((
              new LongWritable(fid.longId),
              GeocodeBoundingBox(GeocodePoint(n, e), GeocodePoint(s, w))
              ))
          }
          case None => {
            // logger.error("%s: couldn't parse into StoredFeatureId".format(line))
            None
          }
        }
      } catch {
        case e: Throwable =>
          // logger.error("%s: %s".format(line, e))
          None
      }
    }
  }).write(TypedSink[(LongWritable, GeocodeBoundingBox)](SpindleSequenceFileSource[LongWritable, GeocodeBoundingBox](outputPath)))
}