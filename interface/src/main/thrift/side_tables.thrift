namespace java com.foursquare.twofishes

include "geocoder.thrift"

struct ChildEntry {
  1: optional string name,
  2: optional string slug
  3: optional string id,
  4: optional geocoder.YahooWoeType woeType
}

struct ChildEntries {
  1: optional list<ChildEntry> entries
}

struct CellGeometry {
  1: optional binary oid,
  2: optional binary wkbGeometry,
  3: optional geocoder.YahooWoeType woeType,
  4: optional bool full
}

struct CellGeometries {
  1: optional list<CellGeometry> cells
}