// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes.LogHelper
import com.foursquare.twofishes.util.{FeatureNamespace, StoredFeatureId}
import java.io.File

trait TsvHelperFileParserLogger {
  def logUnused
}

class GeoIdTsvHelperFileParser(defaultNamespace: FeatureNamespace, fileSeq: Any*) extends TsvHelperFileParserLogger with LogHelper {
  val files = fileSeq.head match {
    case s: String => fileSeq.map(f => new File(f.asInstanceOf[String]))
    case s: File => fileSeq.asInstanceOf[Seq[File]]
    case _ => throw new Exception("ha!")
  }

  def this(defaultNamespace: FeatureNamespace, fileSeq: List[Any]) = this(defaultNamespace, fileSeq: _*)

  class TableEntry(val values: List[String]) {
    var used = false
    def markUsed { used = true}
  }

  if (files.isEmpty) {
    throw new Exception("no filenames specified for parse, maybe you forgot to add defaultNamespace")
  }

  lazy val gidMap = new scala.collection.mutable.HashMap[String,TableEntry]()
  def parseInput() {
    files.foreach(file => {
      if (file.exists()) {
        val fileSource = scala.io.Source.fromFile(file)
        val lines = fileSource.getLines.filterNot(_.startsWith("#"))

        lines.foreach(line => {
          var parts = line.split("\\|")
          if (line.contains("\t")) {
            parts = line.split("\t")
          }
          if (parts.length != 2) {
            logger.error("Broken line in %s: %s (%d parts, needs 2)".format(file, line, parts.length))
          } else {
            StoredFeatureId.fromHumanReadableString(parts(0), Some(defaultNamespace)).foreach(key => {
              var values: List[String] = parts(1).split(",").toList

              gidMap.get(key.humanReadableString).foreach(te =>
                values ++= te.values
              )
              gidMap += (key.humanReadableString -> new TableEntry(values))
            })
          }
        })
      }
    })
  }

  parseInput()

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
        logger.error("%s:%s in %s went unused".format(k, v, files.mkString(",")))
      }
    }})
  }
}

class TsvHelperFileParser(fileSeq: Any*) extends TsvHelperFileParserLogger with LogHelper {
  val files = fileSeq.head match {
    case s: String => fileSeq.map(f => new File(f.asInstanceOf[String]))
    case s: File => fileSeq.asInstanceOf[Seq[File]]
    case _ => throw new Exception("ha!")
  }

  def this(fileSeq: List[Any]) = this(fileSeq: _*)

  class TableEntry(val values: List[String]) {
    var used = false
    def markUsed { used = true}
  }

  lazy val gidMap = new scala.collection.mutable.HashMap[String,TableEntry]()

  if (files.isEmpty) {
    throw new Exception("no filenames specified for parse, maybe you forgot to add defaultNamespace")
  }

  def parseInput() {
    files.foreach(file => {
      if (file.exists()) {
        val fileSource = scala.io.Source.fromFile(file)
        val lines = fileSource.getLines.filterNot(_.startsWith("#"))

        lines.foreach(line => {
          var parts = line.split("\\|")
          if (line.contains("\t")) {
            parts = line.split("\t")
          }
          if (parts.length != 2) {
            logger.error("Broken line in %s: %s (%d parts, needs 2)".format(file, line, parts.length))
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
        logger.error("%s:%s in %s went unused".format(k, v, files.mkString(",")))
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
