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
    GeocodeServingFeatureEdit.newBuilder
      .editType(EditType.Modify)
      .longId(7650L)
      .wktGeometry("POLYGON((-73.97759914398193 40.752036`29031076,-73.98133277893066 40.74719193776603,-73.97176265716553 40.743030203074504,-73.9678144454956 40.748069770416734,-73.97759914398193 40.752036129031076))")
      .namesEdits(List(
      FeatureNameListEdit(
        EditType.Add,
        "Curry Hill",
        "en",
        List(
          FeatureNameFlagsListEdit(EditType.Add, FeatureNameFlags.ALT_NAME),
          FeatureNameFlagsListEdit(EditType.Add, FeatureNameFlags.LOCAL_LANG)
        ))
    ))
      .result,
    GeocodeServingFeatureEdit.newBuilder
      .editType(EditType.Remove)
      .longId(2211L)
      .result,
    GeocodeServingFeatureEdit.newBuilder
      .editType(EditType.Modify)
      .longId(72057594043319895L)
      .namesEdits(List(
      FeatureNameListEdit(
        EditType.Modify,
        "Frisco",
        "en",
        List(
          FeatureNameFlagsListEdit(EditType.Add, FeatureNameFlags.ALT_NAME)
        ))
    ))
      .result,
    GeocodeServingFeatureEdit.newBuilder
      .editType(EditType.Add)
      .longId(72057594039182724L)
      .scoringFeaturesCreateOrMerge(ScoringFeatures.newBuilder.population(35286757).result)
      .cc("IN")
      .center(GeocodePoint(17.38405,78.45636))
      .bounds(GeocodeBoundingBox(GeocodePoint(20.262197124246534,81.89208984375), GeocodePoint(16.25686733062344,76.22314453125)))
      .wktGeometry("POLYGON((78.5302734375 19.91138351415555,77.607421875 18.56294744288831,77.32177734375 15.834535741221552,80.44189453125 16.93070509876554,81.14501953125 17.853290114098012,79.7607421875 19.02057711096681,80.00244140625 19.414792438099557,78.5302734375 19.91138351415555))")
      .woeType(YahooWoeType.ADMIN1)
      .namesEdits(List(
      FeatureNameListEdit(
        EditType.Add,
        "Telangana",
        "en",
        List(
          FeatureNameFlagsListEdit(EditType.Add, FeatureNameFlags.PREFERRED),
          FeatureNameFlagsListEdit(EditType.Add, FeatureNameFlags.LOCAL_LANG)
        ))
    ))
      .attributesCreateOrMerge(GeocodeFeatureAttributes.newBuilder.population(35286757).result)
      .urlsEdits(List(StringListEdit(EditType.Add, "http://en.wikipedia.org/wiki/Telangana")))
      .parentIdsEdits(List(
      LongListEdit(EditType.Add, 72057594039197686L)
    ))
      .result,
    GeocodeServingFeatureEdit.newBuilder
      .editType(EditType.Modify)
      .longId(72057594039197779L)
      .parentIdsEdits(List(
      LongListEdit(EditType.Remove, 72057594039206565L),
      LongListEdit(EditType.Add, 72057594039182724L)
    ))
      .result
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