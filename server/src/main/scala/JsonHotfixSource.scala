// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.common.thrift.json.TReadableJSONProtocol
import java.io.File
import org.apache.thrift.TDeserializer

class JsonHotfixSource(originalPath: String) extends HotfixSource {
  val deserializer = new TDeserializer(new TReadableJSONProtocol.Factory())
  var path = ""
  var allEdits: Seq[GeocodeServingFeatureEdit] = Seq()

  def init() {
    // reread canonical path in case symlink was modified between refreshes
    path = new File(originalPath).getCanonicalPath()
    val dir = new File(path)
    allEdits = dir.listFiles.filter(f => f.getName.endsWith(".json")).flatMap(file => {
      val edits = new RawGeocodeServingFeatureEdits
      deserializer.deserialize(edits, scala.io.Source.fromFile(file).getLines.toList.mkString("").getBytes)
      edits.edits
    })

  }

  def getEdits(): Seq[GeocodeServingFeatureEdit] = allEdits

  def refresh() {
    init()
  }

  init()
}
