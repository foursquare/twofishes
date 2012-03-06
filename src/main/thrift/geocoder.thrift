namespace java com.foursquare.geocoder

struct GeocodePoint {
  1: double lat,
  2: double lng
}

struct GeocodeBoundingBox {
  1: GeocodePoint ne,
  2: GeocodePoint sw
}

struct GeocodeFeature {
  1: GeocodePoint center
  2: string cc,
  3: optional string name,
  4: optional string displayName,
  5: optional i32 woeType,
  6: optional GeocodeBoundingBox bounds
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
  1: string query,
  2: optional string cc,
  3: optional string lang = "en",
  4: optional GeocodePoint ll
}

service Geocoder {
  GeocodeResponse geocode(1: GeocodeRequest r)
}
