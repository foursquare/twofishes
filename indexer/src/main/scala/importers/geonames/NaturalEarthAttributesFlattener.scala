// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import java.io.{File, FileWriter, Writer}

import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.weiglewilczek.slf4s.Logging

// Tool to flatten NaturalEarth Populated Places Shapefile to a single text file
// to simplify scalding index build
// run from twofishes root directory using the following command:
// ./sbt "indexer/run-main com.foursquare.twofishes.importers.geonames.NaturalEarthAttributesFlattener"
//
// NOTE(rahul): This is a temporary workaround until I find/write an implementation
// of FileInputFormat and RecordReader for shapefiles that can split
// https://github.com/mraad/Shapefile works but cannot split yet
object NaturalEarthAttributesFlattener extends Logging {

  def main(args: Array[String]): Unit = {
    val fileWriter = new FileWriter("data/downloaded/flattenedAttributes.txt", false)

    var features = 0
    val iterator = new ShapefileIterator("data/downloaded/ne_10m_populated_places_simple.shp")

    for {
      f <- iterator
      geonameidString <- f.propMap.get("geonameid").toList
      // remove .000
      geonameId = geonameidString.toDouble.toInt
      if geonameId != -1
      adm0cap = f.propMap.getOrElse("adm0cap", "0").toDouble.toInt
      worldcity = f.propMap.getOrElse("worldcity", "0").toDouble.toInt
      scalerank = f.propMap.getOrElse("scalerank", "20").toInt
      natscale = f.propMap.getOrElse("natscale", "0").toInt
      labelrank = f.propMap.getOrElse("labelrank", "0").toInt
    } {
      fileWriter.write("%d\t%d\t%d\t%d\t%d\t%d\n".format(
        geonameId,
        adm0cap,
        worldcity,
        scalerank,
        natscale,
        labelrank))

      features += 1
      if (features % 1000 == 0) {
        logger.info("processed %d features".format(features))
      }
    }

    fileWriter.close()
    logger.info("Done.")
  }
}
