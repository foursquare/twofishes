namespace java com.foursquare.twofishes

include "geocoder.thrift"

// index-only data structures to hold transient data produced by scalding pipeline jobs

struct IntermediateDataContainer {
  1: optional i64 longValue
  2: optional list<i64> longList
  3: optional string stringValue
  4: optional list<string> stringList
  5: optional i32 intValue
  6: optional list<i32> intList
  7: optional bool boolValue
  8: optional list<bool> boolList
  9: optional binary bytes
}

struct PolygonMatchingKey {
  1: optional i64 s2CellId
  2: optional geocoder.YahooWoeType woeType
}

struct PolygonMatchingValue {
  1: optional i64 featureId
  2: optional i64 polygonId
  3: optional list<geocoder.FeatureName> names
  4: optional i32 polygonWoeTypePreferenceLevel
  5: optional string wkbGeometryBase64String
  6: optional string source
}

struct PolygonMatchingValues {
  1: optional list<PolygonMatchingValue> values
}

struct ParentMatchingValue {
  1: optional i64 featureId
  2: optional geocoder.GeocodePoint center
  3: optional geocoder.YahooWoeType woeType
}

struct ParentMatchingValues {
  1: optional list<ParentMatchingValue> values
}