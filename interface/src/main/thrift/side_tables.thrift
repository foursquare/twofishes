namespace java com.foursquare.twofishes

include "geocoder.thrift"

struct CellGeometry {
  1: optional binary oid,
  2: optional binary wkbGeometry,
  3: optional geocoder.YahooWoeType woeType,
  4: optional bool full
}

struct CellGeometries {
  1: optional list<CellGeometry> cells
}
