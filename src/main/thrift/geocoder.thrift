namespace java com.foursquare.geocoder

struct GeocodePoint {
  1: double lat,
  2: double lng
}

struct GeocodeFeature {
  1: GeocodePoint center
  2: string cc,
  3: optional string name,
  4: optional string admin1,
  5: optional string admin2,
  6: optional string admin3,
  7: optional string admin4,
  8: optional string displayName,
  9: optional i32 woeType
}

struct GeocodeInterpretation {
  1: string what,
  2: string where,
  3: GeocodeFeature feature
}

struct GeocodeResponse {
  1: list<GeocodeInterpretation> interpretations
}

struct GeocodeRequest {
  1: string query
}

service Geocoder {
  GeocodeResponse geocode(1: GeocodeRequest r)
}
