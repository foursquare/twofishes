// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder.geonames

import com.foursquare.geocoder.{Helpers, LogHelper}
import java.io.File
//import org.slf4j._

class GeonamesParser extends LogHelper {
  def parseFeature(feature: GeonamesFeature) {

  }

  def parseFromFile(filename: String) {
    val lines = scala.io.Source.fromFile(new File(filename)).getLines
    lines.zipWithIndex.foreach({case (line, index) => {
      if (index % 1000 == 0) {
        logger.info("imported %d features so far".format(index))
      }
      val feature = GeonamesFeature.parseFromAdminLine(index, line)
      feature.foreach(f => {
        if (!f.featureClass.isBuilding || GeonamesImporterConfig.shouldParseBuildings) {
          parseFeature(f)
        }
      })
    }})
  }
}