// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes.StoredFeatureId
import com.foursquare.twofishes.LogHelper
import java.io.File

trait TsvHelperFileParserLogger {
  def logUnused
}

class GeoIdTsvHelperFileParser(defaultNamespace: String, filenames: String*) extends TsvHelperFileParserLogger with LogHelper {
  class TableEntry(val values: List[String]) {
    var used = false
    def markUsed { used = true}
  }

  lazy val gidMap = new scala.collection.mutable.HashMap[String,TableEntry]()
  def parseInput() {
    filenames.foreach(filename => {
      if (new File(filename).exists()) {
        val fileSource = scala.io.Source.fromFile(new File(filename))
        val lines = fileSource.getLines.filterNot(_.startsWith("#"))

        lines.foreach(line => {
          var parts = line.split("\\|")
          if (line.contains("\t")) {
            parts = line.split("\t")
          }
          if (parts.length != 2) {
            logger.error("Broken line in %s: %s (%d parts, needs 2)".format(filename, line, parts.length))
          } else {
            val key = {
              if (parts(0).contains(":")) {
                parts(0)
              } else {
                defaultNamespace + ":" + parts(0)
              }
            }
            var values: List[String] = parts(1).split(",").toList

            gidMap.get(key).foreach(te => 
              values ++= te.values
            )
            gidMap += (key -> new TableEntry(values))
          }
        })
      }
    })
  }

  def get(key: StoredFeatureId): List[String] = {
    gidMap.get(key.toString) match {
      case Some(v) => {
        v.markUsed
        v.values
      }
      case None => Nil
    }
  }

  override def logUnused {
    gidMap.foreach({case (k, v) => {
      if (!v.used) {
        logger.error("%s:%s in %s went unused".format(k, v, filenames.mkString(",")))
      }
    }})
  }
}

class TsvHelperFileParser(filenames: String*) extends TsvHelperFileParserLogger with LogHelper {
  class TableEntry(val values: List[String]) {
    var used = false
    def markUsed { used = true}
  }

  lazy val gidMap = new scala.collection.mutable.HashMap[String,TableEntry]()

  def parseInput() {
    filenames.foreach(filename => {
      if (new File(filename).exists()) {
        val fileSource = scala.io.Source.fromFile(new File(filename))
        val lines = fileSource.getLines.filterNot(_.startsWith("#"))

        lines.foreach(line => {
          var parts = line.split("\\|")
          if (line.contains("\t")) {
            parts = line.split("\t")
          }
          if (parts.length != 2) {
            logger.error("Broken line in %s: %s (%d parts, needs 2)".format(filename, line, parts.length))
          } else {
            val key = parts(0)
            var values: List[String] = parts(1).split(",").toList

            gidMap.get(key).foreach(te => 
              values ++= te.values
            )
            gidMap += (key -> new TableEntry(values))
          }
        })
      }
    })
  }

  parseInput()

  override def logUnused {
    gidMap.foreach({case (k, v) => {
      if (!v.used) {
        logger.error("%s:%s in %s went unused".format(k, v, filenames.mkString(",")))
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