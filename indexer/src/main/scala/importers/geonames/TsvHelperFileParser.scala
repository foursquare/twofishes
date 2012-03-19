// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder.importers.geonames

import com.foursquare.geocoder.LogHelper
import java.io.File

class TsvHelperFileParser(filename: String) extends LogHelper {
  class TableEntry(val values: List[String]) {
    var used = false
    def markUsed { used = true}
  }

  val lines = scala.io.Source.fromFile(new File(filename)).getLines
  val gidMap = lines.flatMap(line => {
    val parts = line.split("\t")
    if (parts.length != 2) {
      logger.error("Broken line in %s: %s (%d parts, needs 2)".format(filename, line, parts.length))
      None
    } else {
      Some(parts(0), new TableEntry(parts(1).split(",").toList))
    }
  }).toMap

  def logUnused {
    gidMap.foreach({case (k, v) => {
      if (!v.used) {
        logger.error("%s:%s in %s went unused".format(k, v, filename))
      }
    }})
  }

  def get(key: String): List[String] = {
    gidMap.get(key) match {
      case Some(v) => {
        v.markUsed
        v.values
      }
      case None => Nil
    }
  }
}