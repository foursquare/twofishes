namespace java com.foursquare.geocoder

struct GeocodeInterpretation {
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
