// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder.geonames

import com.foursquare.geocoder.LogHelper
import java.io.File

class BasicGidTableParser(filename: String) extends LogHelper {
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


  def getGid(gid: String): List[String] = {
    gidMap.get(gid) match {
      case Some(v) => {
        v.markUsed
        v.values
      }
      case None => Nil
    }
  }
}

class RewritesParser(filename: String) {
  val lines = scala.io.Source.fromFile(new File(filename)).getLines
}



