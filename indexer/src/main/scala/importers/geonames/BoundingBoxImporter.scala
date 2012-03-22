// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder.importers.geonames

import com.foursquare.geocoder._
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
      // 1: woeid
      // 2: label
      // 3: bbox: [%f,%f,%f,%f]
      if (parts.size != 4) {
        logger.error("wrong # of parts: %d vs %d in %s".format(parts.size, 4, line))
      } else {
        val geonameid = parts(0)
        val bboxString = parts(3)
        val bbox = bboxString.replace("[", "").replace("]", "").split(",").toList.map(_.toDouble)
        // println("%s --> %s".format(geonameid, bbox))
        store.addBoundingBoxToRecord(
          StoredFeatureId(GeonamesParser.geonameIdNamespace, geonameid),
          BoundingBox(Point(bbox(0), bbox(1)), Point(bbox(2), bbox(3)))
        )
      }
    })
  }
}