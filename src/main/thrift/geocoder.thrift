namespace java com.foursquare.geocoder

struct GeocodePoint {
  1: double lat,
  2: double lng
}

struct GeocodeFeature {
  1: GeocodePoint center
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
