// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.common.thrift.json.TReadableJSONProtocol
import java.io.File
import org.apache.thrift.TDeserializer

class JsonHotfixSource(originalPath: String) extends HotfixSource {
  val deserializer = new TDeserializer(new TReadableJSONProtocol.Factory())
  var path = ""
  var edits = new RawGeocodeServingFeatureEdits

  def init() {
    // reread canonical path in case symlink was modified between refreshes
    path = new File(originalPath).getCanonicalPath()
    edits = new RawGeocodeServingFeatureEdits
    deserializer.deserialize(edits, scala.io.Source.fromFile(path).getLines.toList.mkString("").getBytes)
  }

  def getEdits(): Seq[GeocodeServingFeatureEdit] = edits.edits

  def refresh() {
    init()
  }

  init()
}
