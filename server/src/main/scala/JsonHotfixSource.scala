// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import java.io.File
import org.apache.thrift.{TDeserializer, TSerializer}
import com.foursquare.common.thrift.json.TReadableJSONProtocol

class JsonHotfixSource(originalPath: String) extends HotfixSource {
  val path = new File(originalPath).getCanonicalPath()

  //val serializer = new TSerializer(new TReadableJSONProtocol.Factory(true))
  val deserializer = new TDeserializer(new TReadableJSONProtocol.Factory())

  //val string = serializer.toString(GeocodeServingFeatureEdits(DummyHotfixSource.edits))
  val edits = new RawGeocodeServingFeatureEdits
  deserializer.deserialize(edits, scala.io.Source.fromFile(path).getLines.toList.mkString("").getBytes)

  def getEdits(): Seq[GeocodeServingFeatureEdit] = edits.edits
}