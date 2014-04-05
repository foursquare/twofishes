package com.foursquare.twofishes

import com.foursquare.twofishes.util.{GeonamesId, GeonamesZip, MaponicsId}
import org.bson.types.ObjectId
import org.specs2.mutable.Specification

class SerdeSpec extends Specification {
  def test[T](serde: Serde[T], v: T) = {
    serde.fromBytes(serde.toBytes(v)) must_== v
  }

  "serde" should {
    "work" in {
      import Serde._

      test(LongSerde, 0L)
      test(LongSerde, 1L)
      test(LongSerde, 1L << 40)
      test(LongSerde, (1L << 40) + 1)

      test(LongListSerde, Nil)
      test(LongListSerde, List(0L))
      test(LongListSerde, (0L to 10L).toList)
      test(LongListSerde, ((1L << 40) to ((1L << 40) + 5)).toList)

      test(StringSerde, "")
      test(StringSerde, "hello")
      test(StringSerde, "hello world")
      test(StringSerde, "\u2603")

      test(ObjectIdSerde, new ObjectId)

      test(ObjectIdListSerde, Nil)
      test(ObjectIdListSerde, List(new ObjectId))
      test(ObjectIdListSerde, (0 to 10).map(_ => new ObjectId))

      test(StoredFeatureIdSerde, GeonamesId(1))
      test(StoredFeatureIdSerde, MaponicsId(2))
      test(StoredFeatureIdSerde, new GeonamesZip("US", "10003"))

      test(StoredFeatureIdListSerde, Nil)
      test(StoredFeatureIdListSerde, List(GeonamesId(1), MaponicsId(2), new GeonamesZip("US", "10003")))

      test(TrivialSerde, Array[Byte]())
      test(TrivialSerde, Array[Byte](1))

      test(ThriftSerde(Unit => CellGeometry.newBuilder.result), CellGeometry.newBuilder.full(true).result)
    }
  }
}
