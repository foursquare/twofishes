namespace java com.foursquare.twofishes

struct ChildEntry {
  1: optional string name,
  2: optional string slug
  3: optional string id
}

struct ChildEntries {
  1: optional list<ChildEntry> entries
}