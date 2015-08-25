namespace java com.foursquare.twofishes

// "woe types" ie "where on earth types," these are represent various types
// of geographic features in the world
//
// comments from http://developer.yahoo.com/geo/geoplanet/guide/concepts.html
enum YahooWoeType {
  UNKNOWN = 0

  // One of the major populated places within a country.
  // This category includes incorporated cities and towns, major unincorporated towns and villages.
  TOWN = 7

  // One of the primary administrative areas within a country.
  // Place type names associated with this place type include:
  // State, Province, Prefecture, Country, Region, Federal District.
  ADMIN1 = 8

  // One of the secondary administrative areas within a country.
  // Place type names associated with this place type include:
  // County, Province, Parish, Department, District.
  ADMIN2 = 9

  // One of the tertiary administrative areas within a country.
  // Place type names associated with this place type include:
  // Commune, Municipality, District, Ward.
  ADMIN3 = 10

  POSTAL_CODE = 11
  COUNTRY = 12
  ISLAND = 13
  AIRPORT = 14
  DRAINAGE = 15
  PARK = 16
  POI = 20

  // One of the subdivisions within a town. This category includes suburbs, neighborhoods, wards.
  SUBURB = 22

  SPORT = 23
  COLLOQUIAL = 24
  ZONE = 25
  HISTORICAL_STATE = 26
  HISTORICAL_COUNTY = 27
  CONTINENT = 29
  TIMEZONE = 31

  HISTORICAL_TOWN = 35

  // UNOFFICIAL
  STREET = 100
}

struct GeocodePoint {
  1: double lat
  2: double lng
}

struct GeocodeBoundingBox {
  1: GeocodePoint ne
  2: GeocodePoint sw
}

struct FeatureId {
  1: string source
  2: string id
}

// These are a bit of a mishmash
enum FeatureNameFlags {
  // A preferred name is one that is most often referred to a place
  PREFERRED = 1
  ABBREVIATION = 2

  // a brain-dead deaccenting of a name with diacritics, rendered down to ascii
  DEACCENT = 4

  // names which were not on the original feature, but got there through indexer
  // transforms via rewrites.txt and other hacks
  ALIAS = 8

  // is this name in one of the local languages of this country
  LOCAL_LANG = 16

  // Names coming from aliases.txt
  ALT_NAME = 32

  COLLOQUIAL = 64

  SHORT_NAME = 128

  NEVER_DISPLAY = 256

  LOW_QUALITY = 512

  HISTORIC = 1024

  // New flags should take over one of these unused enum names&values. Due to
  // the way thrift serde works, adding new values to an enum isn't
  // wire-compatible with old readers, so you need to make sure that readers
  // can read your new flags before you start writing them. Adding these unused
  // flags makes it easier to add new flags in the future.
  UNUSED1 = 2048
  UNUSED2 = 4096
  UNUSED3 = 8192
  UNUSED4 = 16384
}

struct FeatureName {
  1: string name
  2: string lang
  3: optional list<FeatureNameFlags> flags = []

  // Mainly used for autocomplete, return the name with the parts that
  // match the query wrapped in <b></b> tags
  4: optional string highlightedName
}

struct FeatureNames {
  1: optional list<FeatureName> names
}

struct FeatureGeometry {
  1: GeocodePoint center
  2: optional GeocodeBoundingBox bounds

  // "well known binary"
  // only present if we have a polygon for this feature
  3: optional binary wkbGeometry
  4: optional string wktGeometry

  5: optional binary wkbGeometrySimplified
  6: optional string wktGeometrySimplified

  7: optional GeocodeBoundingBox displayBounds

  8: optional string source

  9: optional list<i64> s2Covering
  10: optional list<i64> s2Interior
}

enum GeocodeRelationType {
  PARENT = 1

  // These aren't directly in the admin hierarchy, and shouldn't be used for
  // address formatting. TODO: come up with a better name
  HIERARCHY = 2
}

enum NeighborhoodType {
  MACRO = 2
  NEIGHBORHOOD = 3
  SUB = 4
}

struct GeocodeRelation {
  1: optional GeocodeRelationType relationType
  2: optional string relatedId
}

struct GeocodeFeatureAttributes {
  1: optional bool adm0cap = 0
  2: optional bool adm1cap = 0
  3: optional i32 scalerank = 20
  4: optional i32 labelrank = 0
  5: optional i32 natscale = 0
  6: optional i32 population = 0
  7: optional bool sociallyRelevant = 0
  8: optional NeighborhoodType neighborhoodType
  9: optional list<string> urls
  10: optional bool worldcity = 0
}

// index-only data structure
struct ScoringFeatures {
  1: optional i32 population = 0
  2: optional i32 boost = 0
  6: optional list<i64> parentIds = []
  5: optional bool canGeocode = 1
  7: optional bool hasPoly = 0
  8: optional list<i64> extraRelationIds = []

  3: optional list<string> DEPRECATED_parents = []
}

struct GeocodeFeature {
  // country code
  1: string cc
  2: FeatureGeometry geometry

  //
  3: optional string name
  4: optional string displayName
  5: optional YahooWoeType woeType = YahooWoeType.UNKNOWN
  6: optional list<FeatureId> ids
  7: optional list<FeatureName> names
  8: optional list<string> attribution
  9: optional list<string> timezones

  11: optional string highlightedName
  12: optional string matchedName

  13: optional string slug
  14: optional string id

  15: optional GeocodeFeatureAttributes attributes

  16: optional i64 longId
  17: optional list<i64> longIds

  18: optional list<i64> parentIds

  19: optional YahooWoeType role
}

struct GeocodeFeatures {
  1: optional list<GeocodeFeature> features
}

struct GeocodeServingFeature {
  5: i64 longId
  2: ScoringFeatures scoringFeatures
  3: GeocodeFeature feature
  4: optional list<GeocodeFeature> parents
  6: optional list<string> slugs

  1: optional string DEPRECATED_id
}

struct InterpretationScoringFeatures {
//  1: optional i32 population = 0
  2: optional double percentOfRequestCovered = 0.0
  3: optional double percentOfFeatureCovered = 0.0
  4: optional double featureToRequestCenterDistance = 0.0
}

struct DebugScoreComponent {
  1: required string key
  2: required i32 value
}

struct InterpretationDebugInfo {
  1: optional list<DebugScoreComponent> scoreComponents
  2: optional i32 finalScore
}

struct GeocodeInterpretation {
  1: optional string what
  2: optional string where
  3: GeocodeFeature feature
  4: optional list<GeocodeFeature> parents
  5: optional ScoringFeatures scoringFeatures_DEPRECATED
  6: optional InterpretationScoringFeatures scores
  7: optional InterpretationDebugInfo debugInfo
  8: optional list<i64> parentLongIds
}

struct GeocodeResponse {
  1: list<GeocodeInterpretation> interpretations

  // only present if debug > 0 in request
  2: optional list<string> debugLines
  3: optional string requestWktGeometry
}

enum ResponseIncludes {
  // include as much as we possibly can. Everything below here is true.
  EVERYTHING
  // include parents in response
  PARENTS
  // include all names on base feature
  ALL_NAMES
  // include all names on all parents
  PARENT_ALL_NAMES
  // include geometry in wkb or wkt format if available
  WKB_GEOMETRY
  WKT_GEOMETRY
  // include geometry coverage information (revgeo only)
  REVGEO_COVERAGE
  // controls if we should fetch parents to construct a string like "New York, NY"
  // for legacy reasons, this is automatically turned on for geocode queries for now.
  // it's mainly here because reverse geocode clients often don't need it
  DISPLAY_NAME
  // include (11m tolerance simplified) geometry in wkb or wkt format if available
  // make display in json much more pleasant
  WKB_GEOMETRY_SIMPLIFIED
  WKT_GEOMETRY_SIMPLIFIED
  // include s2 covering of geometry as a list of s2 cell ids
  S2_COVERING
  S2_INTERIOR
}

enum AutocompleteBias {
  // no bias
  NONE = 0
  // mix local and globally relevant results
  BALANCED = 1
  // prefer locally relevant results
  LOCAL = 2
  // prefer globally relevant results
  GLOBAL = 3
}

struct GeocodeRequest {
  1: optional string query

  // country code hint -- results will be biased towards this country
  2: optional string cc

  // langugage hint, used to format displayName in response
  3: optional string lang = "en"

  // lat/lng hint -- results will be biased towards this location
  // in revgeo mode, this is the point that is searched for
  4: optional GeocodePoint ll

  5: optional bool full_DEPRECATED = 0

  // debug information, currently 0 or 1
  6: optional i32 debug = 0

  // Is this an autocomplete request? i.e. should we treat this as prefix matching
  7: optional bool autocomplete = 0

  // bias the results towards these woe types
  8: optional list<YahooWoeType> woeHint = []

  // restrict the results towards these woe types
  9: optional list<YahooWoeType> woeRestrict = []

  // supercedes ll for hinting, things in the box get boosted uniformly
  10: optional GeocodeBoundingBox bounds

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

  // in geocoding mode, requires all results to fall within the bounds/radius specified
  19: optional bool strict = 0

  // in autocomplete mode, specifies how strongly locally relevant results are preferred
  20: optional AutocompleteBias autocompleteBias = AutocompleteBias.BALANCED
}

// I'd like to replace most of the params in geocoderequest with one instance of this
// that can be shared by multiple request types.
struct CommonGeocodeRequestParams {
  1: optional i32 debug = 0

  // bias the results towards these woe types
  2: optional list<YahooWoeType> woeHint = []
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
  9: optional GeocodeBoundingBox bounds

  // If set to <= 0, means unlimited in revgeo, and ~3 in geocode or autocomplete
  10: optional i32 maxInterpretations = 0
}

struct BulkReverseGeocodeRequest {
  1: optional list<GeocodePoint> latlngs
  2: optional CommonGeocodeRequestParams params
}

struct BulkReverseGeocodeResponse {
  1: required map<i32, list<GeocodeInterpretation>> DEPRECATED_interpretationMap

  3: required list<GeocodeInterpretation> interpretations

  // this list will be the same size as the input array of latlngs
  // intepretationIndexes[0] is a list of indexes into interpretations that represent the
  //   reverse geocodes of latlng[0]
  // interpretationIndexes[1] are the interpretations that revgeo latlng[1]
  // latlngs that had no revgeo matches will have an empty array in the corresponding position
  //   ... and so on
  4: list<list<i32>> interpretationIndexes

  // currently unused
  5: optional list<GeocodeFeature> parentFeatures

  // only present if debug > 0 in request
  2: optional list<string> debugLines
}

struct BulkSlugLookupRequest {
  1: optional list<string> slugs
  2: optional CommonGeocodeRequestParams params
}

struct BulkSlugLookupResponse {
  1: required list<GeocodeInterpretation> interpretations

  // this list will be the same size as the input array of slugs
  // intepretationIndexes[0] is a list of indexes into interpretations that represent the
  //   lookup of slugs[0]
  // interpretationIndexes[1] are the interpretations that map to slugs[1]
  //   ... and so on
  2: list<list<i32>> interpretationIndexes

  // currently unused
  4: optional list<GeocodeFeature> parentFeatures

  // only present if debug > 0 in request
  3: optional list<string> debugLines
}

struct RefreshStoreRequest {
  1: optional string token
}

struct RefreshStoreResponse {
  1: optional bool success
}

struct S2CellInfoRequest {
  1: optional list<string> cellIdsAsStrings
}

struct S2CellIdInfo {
  1: optional i64 id
  2: optional i32 level
  3: optional string wktGeometry
}

struct S2CellInfoResponse {
  1: optional list<S2CellIdInfo> cellInfos = []
}

service Geocoder {
  GeocodeResponse geocode(1: GeocodeRequest r)
  GeocodeResponse reverseGeocode(1: GeocodeRequest r)
  BulkReverseGeocodeResponse bulkReverseGeocode(1: BulkReverseGeocodeRequest r)
  BulkSlugLookupResponse bulkSlugLookup(1: BulkSlugLookupRequest r)
  RefreshStoreResponse refreshStore(1: RefreshStoreRequest r)
  S2CellInfoResponse getS2CellInfos(1: S2CellInfoRequest r)
}
