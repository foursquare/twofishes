namespace java com.foursquare.twofishes

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

enum NeighborhoodType {
  MACRO = 2,
  NEIGHBORHOOD = 3,
  SUB = 4,
}

struct GeocodeRelation {
  1: optional GeocodeRelationType relationType,
  2: optional string relatedId
}

struct GeocodeFeatureAttributes {
  1: optional bool adm0cap = 0,
  2: optional bool adm1cap = 0,
  3: optional i32 scalerank = 0,
  4: optional i32 labelrank = 0,
  5: optional i32 natscale = 0,
  6: optional i32 population = 0,
  7: optional bool sociallyRelevant = 0,
  8: optional NeighborhoodType neighborhoodType
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

  13: optional string slug,
  14: optional string id,

  15: optional GeocodeFeatureAttributes attributes
}

struct GeocodeServingFeature {
  1: string id
  2: ScoringFeatures scoringFeatures,
  3: GeocodeFeature feature,
  4: optional list<GeocodeFeature> parents,
}

struct InterpretationScoringFeatures {
//  1: optional i32 population = 0,
  2: optional double percentOfRequestCovered = 0.0,
  3: optional double percentOfFeatureCovered = 0.0
}

struct DebugScoreComponent {
  1: required string key
  2: required i32 value
}

struct InterpretationDebugInfo {
  1: optional list<DebugScoreComponent> scoreComponents,
  2: optional i32 finalScore
}

struct GeocodeInterpretation {
  1: string what,
  2: string where,
  3: GeocodeFeature feature
  4: optional list<GeocodeFeature> parents,
  5: optional ScoringFeatures scoringFeatures_DEPRECATED,
  6: optional InterpretationScoringFeatures scores,
  7: optional InterpretationDebugInfo debugInfo
}

struct GeocodeResponse {
  1: list<GeocodeInterpretation> interpretations,

  // only present if debug > 0 in request
  2: optional list<string> debugLines,
  3: optional string requestWktGeometry
}

enum ResponseIncludes {
  // include as much as we possibly can. Everything below here is true.
  EVERYTHING,
  // include parents in response
  PARENTS,
  // include all names on base feature
  ALL_NAMES,
  // include all names on all parents
  PARENT_ALL_NAMES,
  // include geometry in wkb or wkt format if available
  WKB_GEOMETRY,
  WKT_GEOMETRY,
  // include geometry coverage information (revgeo only)
  REVGEO_COVERAGE,
  // controls if we should fetch parents to construct string likes "New York, NY"
  // for legacy reasons, this is automatically turned on for geocode queries for now.
  // it's mainly here because reverse geocode clients often don't need it
  DISPLAY_NAME
}

struct GeocodeRequest {
  1: optional string query,

  // country code hint -- results will be biased towards this country
  2: optional string cc

  // langugage hint, used to format displayName in response
  3: optional string lang = "en",

  // lat/lng hint -- results will be biased towards this location
  // in revgeo mode, this is the point that is searched for
  4: optional GeocodePoint ll,

  5: optional bool full_DEPRECATED = 0,

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

  12: optional bool includePolygon_DEPRECATED = 0

  // radius in meters, ll+radius is an alternative to boundingbox
  14: optional i32 radius = 0

  // If set to <= 0, means unlimited in revgeo, and ~3 in geocode or autocomplete
  16: optional i32 maxInterpretations = 0

  // if set, then restrict to features where the source in one of the ids is in the list
  17: optional list<string> allowedSources

  // replaces full, includePolygon, wktGeometry, calculateCoverage
  18: optional list<ResponseIncludes> responseIncludes = []
}

// I'd like to replace most of the params in geocoderequest with one instance of this,
// that can be shared by multiple request types.
struct CommonGeocodeRequestParams {
  1: optional i32 debug = 0

  // bias the results towards these woe types
  2: optional list<YahooWoeType> woeHint = [],
  3: optional list<YahooWoeType> woeRestrict = []

  // country code hint -- results will be biased towards this country
  4: optional string cc

  // langugage hint, used to format displayName in response
  5: optional string lang = "en"

  6: optional list<ResponseIncludes> responseIncludes = []

  // if set, then restrict to features where the source in one of the ids is in the list
  7: optional list<string> allowedSources

  8: optional GeocodePoint llHint

  // supercedes ll for hinting, things in the box get boosted uniformly
  9: optional GeocodeBoundingBox bounds,
}

struct BulkReverseGeocodeRequest {
  1: optional list<GeocodePoint> latlngs
  2: optional CommonGeocodeRequestParams params
}

struct BulkReverseGeocodeResponse {
  1: required map<i32, GeocodeInterpretation> interpretationMap
}

service Geocoder {
  GeocodeResponse geocode(1: GeocodeRequest r)
  GeocodeResponse reverseGeocode(1: GeocodeRequest r)
//  GeocodeResponse bulkReverseGeocode(1: BulkReverseGeocodeRequest r)
}