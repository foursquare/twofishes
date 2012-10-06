namespace java com.foursquare.twofishes

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
  ISLAND = 13,
  AIRPORT = 14,
  DRAINAGE = 15,
  PARK = 16,
  POI = 20,

  // One of the subdivisions within a town. This category includes suburbs, neighborhoods, wards.
  SUBURB = 22,

  SPORT = 23,
  COLLOQUIAL = 24,
  ZONE = 25,
  HISTORICAL_STATE = 26,
  HISTORICAL_COUNTY = 27,
  TIMEZONE = 31,

  HISTORICAL_TOWN = 35,

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

struct FeatureId {
  1: string source,
  2: string id
}

enum FeatureNameFlags {
  PREFERRED = 0x1,
  ABBREVIATION = 0x2,
  DEACCENT = 0x4,
  ALIAS = 0x8,
  LOCAL_LANG =  0x16
}

struct FeatureName {
  1: string name,
  2: string lang,
  3: optional list<FeatureNameFlags> flags = [],
  4: optional string highlightedName
}

struct FeatureGeometry {
  1: GeocodePoint center,
  2: optional GeocodeBoundingBox bounds
}

struct ScoringFeatures {
  1: optional i32 population = 0,
  2: optional i32 boost = 0,
  3: optional list<string> parents = []
}

struct GeocodeFeature {
  1: string cc,
  2: FeatureGeometry geometry,
  3: optional string name,
  4: optional string displayName,
  5: optional YahooWoeType woeType,
  6: optional list<FeatureId> ids,
  7: optional list<FeatureName> names,
  8: optional list<string> attribution,
  9: optional list<string> timezones,

  11: optional string highlightedName,
  12: optional string matchedName,
}

struct GeocodeServingFeature {
  1: string id
  2: ScoringFeatures scoringFeatures,
  3: GeocodeFeature feature 
}

struct GeocodeInterpretation {
  1: string what,
  2: string where,
  3: GeocodeFeature feature
  4: optional list<GeocodeFeature> parents,
  5: optional ScoringFeatures scoringFeatures,
}

struct GeocodeResponse {
  1: list<GeocodeInterpretation> interpretations,
  2: optional list<string> debugLines,
}

struct GeocodeRequest {
  1: string query,
  2: optional string cc,
  3: optional string lang = "en",
  4: optional GeocodePoint ll,
  5: optional bool full = 0,
  6: optional i32 debug = 0,
  7: optional bool autocomplete = 0
  8: optional list<YahooWoeType> woeHint = [],
  9: optional list<YahooWoeType> woeRestrict = []
}

service Geocoder {
  GeocodeResponse geocode(1: GeocodeRequest r)
}
