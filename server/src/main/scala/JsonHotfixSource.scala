// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.common.thrift.json.TReadableJSONProtocol
import java.io.File
import org.apache.thrift.TDeserializer

class JsonHotfixSource(originalPath: String) extends HotfixSource {
  val path = new File(originalPath).getCanonicalPath()

  val deserializer = new TDeserializer(new TReadableJSONProtocol.Factory())

  val edits = new RawGeocodeServingFeatureEdits
  deserializer.deserialize(edits, scala.io.Source.fromFile(path).getLines.toList.mkString("").getBytes)

  def getEdits(): Seq[GeocodeServingFeatureEdit] = edits.edits
}
