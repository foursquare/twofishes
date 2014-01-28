namespace java com.foursquare.twofishes

include "geocoder.thrift"

struct CellGeometry {
  2: optional binary wkbGeometry,
  3: optional geocoder.YahooWoeType woeType,
  4: optional bool full
  5: optional i64 longId
}

struct CellGeometries {
  1: optional list<CellGeometry> cells
}
