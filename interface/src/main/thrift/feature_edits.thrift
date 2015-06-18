namespace java com.foursquare.twofishes

include "geocoder.thrift"

// not all EditTypes are defined in every situation
enum EditType {
  NoOp = 0 // useful if hotfixes are serialized to text and individual edits need to be quickly/reversibly invalidated
  Add = 1
  Modify = 2
  Remove = 3
}

struct LongListEdit {
  1: EditType editType
  2: i64 value
}

struct StringListEdit {
  1: EditType editType
  2: string value
}

struct FeatureNameFlagsListEdit {
  1: EditType editType
  2: geocoder.FeatureNameFlags value
}

struct FeatureNameListEdit {
  1: EditType editType
  2: string name
  3: string lang
  4: optional list<FeatureNameFlagsListEdit> flagsEdits = []
}

struct GeocodeServingFeatureEdit {
  1: EditType editType
  2: i64 longId

  3: optional geocoder.ScoringFeatures scoringFeaturesCreateOrMerge
  4: optional list<LongListEdit> extraRelationsEdits = []

  // GeocodeFeature and FeatureGeometry contain required fields so cannot do createOrMerge in the same way as the rest
  5: optional string cc
  6: optional geocoder.GeocodePoint center
  7: optional geocoder.GeocodeBoundingBox bounds
  8: optional geocoder.GeocodeBoundingBox displayBounds
  9: optional string wktGeometry
  10: optional string geojsonGeometry
  11: optional geocoder.YahooWoeType woeType
  12: optional geocoder.YahooWoeType role
  13: optional list<FeatureNameListEdit> namesEdits = []
  14: optional geocoder.GeocodeFeatureAttributes attributesCreateOrMerge
  15: optional list<StringListEdit> urlsEdits = []
  16: optional list<LongListEdit> parentIdsEdits = []

  // for backwards compatibility, slugs can only be added, not removed
  17: optional string slug
}

struct GeocodeServingFeatureEdits {
  1: optional list<GeocodeServingFeatureEdit> edits
}
