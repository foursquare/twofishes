namespace java com.foursquare.twofishes

struct ThriftStringWrapper {
  1: optional string str
}

struct ChildEntry {
  1: optional string name,
  2: optional string slug
  3: optional string id
}

struct ChildEntries {
  1: optional list<ChildEntry> entries
}

// "woe types" ie "where on earth types," these are represent various types
// of geographic features in the world
//
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
  CONTINENT = 29,
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

// These are a bit of a mishmash
enum FeatureNameFlags {
  // A preferred name is one that is most often referred to a place
PREFERRED = 1,
  ABBREVIATION = 2,

  // a brain-dead deaccenting of a name with diacritics, rendered down to ascii
  DEACCENT = 4,

  // names which were not on the original feature, but got there through indexer
  // transforms via rewrites.txt and other hacks
  ALIAS = 8,

  // is this name in one of the local languages of this country
  LOCAL_LANG = 16,

  // Names coming from aliases.txt
  ALT_NAME = 32

  COLLOQUIAL = 64
}

struct FeatureName {
  1: string name,
  2: string lang,
  3: optional list<FeatureNameFlags> flags = [],

  // Mainly used for autocomplete, return the name with the parts that
  // match the query wrapped in <b></b> tags
  4: optional string highlightedName
}

struct FeatureGeometry {
  1: GeocodePoint center,
  2: optional GeocodeBoundingBox bounds,

  // "well known binary" 
  // only present if we have a polygon for this feature
  3: optional binary wkbGeometry,
  4: optional string wktGeometry
}

enum GeocodeRelationType {
  PARENT = 1,

  // These aren't directly in the admin hierarchy, and shouldn't be used for
  // address formatting. TODO: come up with a better name
  HIERARCHY = 2
}

struct GeocodeRelation {
  1: optional GeocodeRelationType relationType,
  2: optional string relatedId
}

// index-only data structure
struct ScoringFeatures {
  1: optional i32 population = 0,
  2: optional i32 boost = 0,
  3: optional list<string> parents = [],
  5: optional bool canGeocode = 1
}

struct GeocodeFeature {
  // country code
  1: string cc,
  2: FeatureGeometry geometry,

  // 
  3: optional string name,
  4: optional string displayName,
  5: optional YahooWoeType woeType,
  6: optional list<FeatureId> ids,
  7: optional list<FeatureName> names,
  8: optional list<string> attribution,
  9: optional list<string> timezones,

  11: optional string highlightedName,
  12: optional string matchedName,

  13: optional string slug
  14: optional string id
}

struct GeocodeServingFeature {
  1: string id
  2: ScoringFeatures scoringFeatures,
  3: GeocodeFeature feature,
  4: optional list<GeocodeFeature> parents,
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

  // only present if debug > 0 in request
  2: optional list<string> debugLines,
}

struct GeocodeRequest {
  1: optional string query,

  // country code hint -- results will be biased towards this country
  2: optional string cc

  // langugage hint, used to format displayName in response
  3: optional string lang = "en",

  // lat/lng hint -- results will be biased towards this location
  4: optional GeocodePoint ll,

  // whether or not to return fully expanded names and parents
  5: optional bool full = 0,

  // debug information, currently 0 or 1
  6: optional i32 debug = 0,

  // Is this an autocomplete request? i.e. should we treat this as prefix matching
  7: optional bool autocomplete = 0

  // bias the results towards these woe types
  8: optional list<YahooWoeType> woeHint = [],

  // restrict the results towards these woe types
  9: optional list<YahooWoeType> woeRestrict = [],

  // supercedes ll for hinting, things in the box get boosted uniformly
  10: optional GeocodeBoundingBox bounds,

  // This can be either a slug or a namespace:id featureid for now
  11: optional string slug

  12: optional bool includePolygon = 0
  13: optional bool wktGeometry = 0
}

service Geocoder {
  GeocodeResponse geocode(1: GeocodeRequest r)
}