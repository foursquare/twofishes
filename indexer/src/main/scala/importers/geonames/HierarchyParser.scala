// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import java.io.File
import scala.collection.mutable.HashMap

object HierarchyParser {
  def parseHierarchy(filenames: List[String]): HashMap[String, List[String]] = {
    val map = new HashMap[String, List[String]]
    for {
      filename <- filenames
      if new File(filename).exists
    } {
      val lines = scala.io.Source.fromFile(filename).getLines
      lines.foreach(l => {
        val parts = l.split("\\t")
        val parentId = parts(0)
        val childId = parts(1)
        val hierarchyType = parts.lift(2).getOrElse("")

        if (hierarchyType == "ADM") {
          if (!map.contains(childId)) {
            map(childId) = Nil
          }
          map(childId) = parentId :: map(childId)
        }
      })
    }

    map
  }
}
