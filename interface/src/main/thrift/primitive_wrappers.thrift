namespace java com.foursquare.base.thrift

include "types.thrift"

typedef types.ThriftObjectId ThriftObjectId

struct StringWrapper {
  1: optional string value,
}

struct ObjectIdWrapper {
  1: optional ThriftObjectId value,
}

struct ObjectIdListWrapper {
  1: optional list<ThriftObjectId> values,
}

struct LongWrapper {
  1: optional i64 value,
}
