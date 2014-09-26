// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import java.io.{FileOutputStream, File}
import org.apache.thrift.TSerializer
import com.foursquare.common.thrift.json.TReadableJSONProtocol

// TODO(rahul): come up with better tooling for building hotfix files
object JsonHotfixFileBuilder {
  // build individual objects here and run build-hotfixes.py to produce json file
  // DO NOT push commits with changes to this list back to twofishes
  val edits: List[GeocodeServingFeatureEdit] = List(
    
  )

  val editsWrapper = GeocodeServingFeatureEdits(edits)

  def main(args: Array[String]) {
    if (args.size > 0) {
      val output = new FileOutputStream(new File(args(0)), false)
      val serializer = new TSerializer(new TReadableJSONProtocol.Factory(true))
      output.write(serializer.serialize(editsWrapper))
      output.close()
    }
  }
}