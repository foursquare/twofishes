// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes._
import java.io.File

// I could not for the life of me get the java geojson libraries to work
// using a table I computer in python for the flickr bounding boxes.
//
// Please fix at some point.

class BoundingBoxTsvImporter(store: GeocodeStorageWriteService) extends LogHelper {
  def parse(filename: String) {
    val lines = scala.io.Source.fromFile(new File(filename)).getLines
    lines.foreach(line => {
      val parts = line.split("\t")
      // 0: geonameid
      // 1->5:       // west, south, east, north
      if (parts.size != 5) {
        logger.error("wrong # of parts: %d vs %d in %s".format(parts.size, 5, line))
      } else {
        try {
          val (namespace, id) = parts(0).split(":")
          val w = parts(1).toDouble
          val s = parts(2).toDouble
          val e = parts(3).toDouble
          val n = parts(4).toDouble
          // println("%s --> %s".format(geonameid, bbox))
          store.addBoundingBoxToRecord(
            StoredFeatureId(namespace, geonameid),
            // ne, sw
            BoundingBox(Point(n, e), Point(s, w))
          )
        } catch {
          case e => 
          logger.error("%s: %s".format(line, e))
        }
      }
    })
  }
}