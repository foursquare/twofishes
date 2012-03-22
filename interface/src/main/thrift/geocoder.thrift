namespace java com.foursquare.geocoder

// comments from http://developer.yahoo.com/geo/geoplanet/guide/concepts.html
enum YahooWoeType {
  UNKNOWN = 0

  // One of the major populated places within a country.
  // This category includes incorporated cities and towns, major unincorporated towns and villages.
  TOWN = 7,

  // One of the primary administrative areas within a country.
  // Place type names associated with this place type include:
  // State, Province, Prefecture, Country, Region, Federal District.
  ADMIN1 = 8,

  // One of the secondary administrative areas within a country.
  // Place type names associated with this place type include:
  // County, Province, Parish, Department, District.
  ADMIN2 = 9,

  // One of the tertiary administrative areas within a country.
  // Place type names associated with this place type include:
  // Commune, Municipality, District, Ward.
  ADMIN3 = 10,

  POSTAL_CODE = 11,
  COUNTRY = 12,

  AIRPORT = 14,

  // One of the subdivisions within a town. This category includes suburbs, neighborhoods, wards.
  SUBURB = 22,

  // UNOFFICIAL
  STREET = 100
}

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
  5: optional YahooWoeType woeType,
  6: optional GeocodeBoundingBox bounds,
}

struct GeocodeInterpretation {
  1: string what,
  2: string where,
  3: GeocodeFeature feature
  4: optional list<GeocodeFeature> parents
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
