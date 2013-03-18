// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes._
import java.io.File
import scala.collection.mutable.HashMap

case class AlternateNameEntry(
  nameId: String,
  lang: String,
  name: String,
  isPrefName: Boolean,
  isShortName: Boolean
)

object AlternateNamesReader extends SimplePrintLogger {
  def readAlternateNamesFiles(filenames: List[String]): HashMap[StoredFeatureId, List[AlternateNameEntry]] = {
    val alternateNamesMap = new HashMap[StoredFeatureId, List[AlternateNameEntry]]
    filenames.foreach(filename => {
      val lines = scala.io.Source.fromFile(new File(filename)).getLines
      lines.zipWithIndex.foreach({case (line, index) => {
        if (index % 100000 == 0) {
          logger.info("imported %d alternateNames from %s so far".format(index, filename))
        }

        val parts = line.split("\t").toList
        if (parts.size < 3) {
            logger.error("line %d didn't have 4 parts: %s -- %s".format(index, line, parts.mkString(",")))
          } else {
            val nameid = parts(0)
            val geonameid = parts(1)
            val lang = parts.lift(2).getOrElse("")

            if (lang != "post") {
              val name = parts(3)
              val isPrefName = parts.lift(4).exists(_ == "1")
              val isShortName = parts.lift(5).exists(_ == "1")

              val fid = StoredFeatureId.fromString(geonameid, Some(GeonamesParser.geonameIdNamespace))
              val names = alternateNamesMap.getOrElseUpdate(fid, Nil)
              alternateNamesMap(fid) = AlternateNameEntry(
                nameId = nameid,
                name = name,
                lang = lang,
                isPrefName = isPrefName,
                isShortName = isShortName
              ) :: names
            }
        }
      }})
    })
    alternateNamesMap
  }
}
