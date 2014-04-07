// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes._
import com.foursquare.twofishes.mongo.GeocodeStorageWriteService
import com.foursquare.twofishes.util.{GeonamesNamespace, StoredFeatureId}
import com.weiglewilczek.slf4s.Logging
import java.io.File
import scala.collection.mutable.HashMap

// I could not for the life of me get the java geojson libraries to work
// using a table I computer in python for the flickr bounding boxes.
//
// Please fix at some point.

object BoundingBoxTsvImporter extends Logging {
  def parse(filenames: List[File]): HashMap[StoredFeatureId, BoundingBox] = {
    val map = new HashMap[StoredFeatureId, BoundingBox]
    filenames.foreach(file => {
     logger.info("processing bounding box file %s".format(file))
      val lines = scala.io.Source.fromFile(file).getLines
      lines.filterNot(_.startsWith("#")).foreach(line => {
        val parts = line.split("[\t ]")
        // 0: geonameid
        // 1->5:       // west, south, east, north
        if (parts.size != 5) {
          logger.error("wrong # of parts: %d vs %d in %s".format(parts.size, 5, line))
        } else {
          try {
            val id = parts(0)
            val w = parts(1).toDouble
            val s = parts(2).toDouble
            val e = parts(3).toDouble
            val n = parts(4).toDouble
            StoredFeatureId.fromHumanReadableString(id, Some(GeonamesNamespace)) match {
              case Some(fid) => {
                map(fid) = BoundingBox(Point(n, e), Point(s, w))
                logger.debug("bbox %s %s".format(fid, parts.drop(1).mkString(",")))
              }
              case None => logger.error("%s: couldn't parse into StoredFeatureId".format(line))
            }
          } catch {
            case e: Throwable =>
            logger.error("%s: %s".format(line, e))
          }
        }
      })
    })
    map
  }

  def batchUpdate(filenames: List[File], store: GeocodeStorageWriteService) = {
    parse(filenames).foreach({case (fid, bbox) => {
      store.addBoundingBoxToRecord(bbox, fid)
    }})
  }
}
