// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.common.thrift.json.TReadableJSONProtocol
import com.weiglewilczek.slf4s.Logging
import java.io.File
import org.apache.thrift.TDeserializer

class JsonHotfixSource(originalPath: String) extends HotfixSource with Logging {
  val deserializer = new TDeserializer(new TReadableJSONProtocol.Factory())
  var path = ""
  var allEdits: Seq[GeocodeServingFeatureEdit] = Seq()

  def init() {
    // reread canonical path in case symlink was modified between refreshes
    path = new File(originalPath).getCanonicalPath
    val dir = new File(path)
    if (dir.exists && dir.isDirectory) {
      allEdits = dir.listFiles.filter(f => f.getName.endsWith(".json")).flatMap(file => {
        val edits = new RawGeocodeServingFeatureEdits
        deserializer.deserialize(edits, scala.io.Source.fromFile(file).getLines().toList.mkString("").getBytes)
        edits.edits
      })
    } else {
      logger.warn("invalid hotfix directory: %s".format(path))
    }
  }

  def getEdits(): Seq[GeocodeServingFeatureEdit] = allEdits

  def refresh() {
    init()
  }

  init()
}
