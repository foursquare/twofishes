// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes.util.{GeonamesId, StoredFeatureId}
import java.io.File
import scala.collection.mutable.HashMap

object HierarchyParser {
  def parseHierarchy(filenames: List[String]): HashMap[StoredFeatureId, List[StoredFeatureId]] = {
    val map = new HashMap[StoredFeatureId, List[StoredFeatureId]]
    for {
      filename <- filenames
      if new File(filename).exists
    } {
      val lines = scala.io.Source.fromFile(filename).getLines
      lines.foreach(l => {
        val parts = l.split("\\t")
        val parentId = GeonamesId(parts(0).toLong)
        val childId = GeonamesId(parts(1).toLong)
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
