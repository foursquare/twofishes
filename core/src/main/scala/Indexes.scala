package com.foursquare.twofishes

import com.foursquare.twofishes.util.{ByteUtils, StoredFeatureId}
import java.nio.ByteBuffer
import java.util.Arrays
import org.apache.thrift.{TBase, TBaseHelper, TDeserializer, TFieldIdEnum, TSerializer}
import org.apache.thrift.protocol.TCompactProtocol
import org.bson.types.ObjectId

sealed abstract class Serde[T] {
  def toBytes(t: T): Array[Byte]
  def fromBytes(bytes: Array[Byte]): T
}

object Serde {
  case object LongSerde extends Serde[Long] {
    override def toBytes(t: Long): Array[Byte] = ByteUtils.longToBytes(t)
    override def fromBytes(bytes: Array[Byte]): Long =
      ByteUtils.getLongFromBytes(bytes)
  }

  case object StringSerde extends Serde[String] {
    override def toBytes(t: String): Array[Byte] = t.getBytes("UTF-8")
    override def fromBytes(bytes: Array[Byte]): String = new String(bytes)
  }

  case object ObjectIdSerde extends Serde[ObjectId] {
    override def toBytes(t: ObjectId): Array[Byte] = t.toByteArray
    override def fromBytes(bytes: Array[Byte]): ObjectId = new ObjectId(bytes)
  }

  case object ObjectIdListSerde extends Serde[Seq[ObjectId]] {
    override def toBytes(t: Seq[ObjectId]): Array[Byte] = {
      val buf = ByteBuffer.allocate(t.size * 12)
      t.foreach(oid => buf.put(oid.toByteArray))
      TBaseHelper.byteBufferToByteArray(buf)
    }
    override def fromBytes(bytes: Array[Byte]): Seq[ObjectId] = {
      0.until(bytes.length / 12).map(i => {
        new ObjectId(Arrays.copyOfRange(bytes, i * 12, (i + 1) * 12))
      })
    }
  }

  case object TrivialSerde extends Serde[Array[Byte]] {
    override def toBytes(t: Array[Byte]): Array[Byte] = t
    override def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
  }

  case class ThriftSerde[T <: TBase[_ <: TBase[_ <: AnyRef, _ <: TFieldIdEnum], _ <: TFieldIdEnum]](
      factory: Unit => T) extends Serde[T] {
    val deserializer = new TDeserializer(new TCompactProtocol.Factory())
    val serializer = new TSerializer(new TCompactProtocol.Factory())

    def toBytes(t: T): Array[Byte] = {
      serializer.serialize(t)
    }
    def fromBytes(bytes: Array[Byte]): T = {
      val s = factory()
      deserializer.deserialize(s, bytes)
      s
    }
  }
}

sealed abstract class Index[K, V](val filename: String, val keySerde: Serde[K], val valueSerde: Serde[V])

object Indexes {
  type WKBGeometry = Array[Byte]

  case object GeometryIndex extends Index[ObjectId, WKBGeometry](
    "geometry", Serde.ObjectIdSerde, Serde.TrivialSerde)

  case object FeatureIndex extends Index[ObjectId, GeocodeServingFeature](
    "features", Serde.ObjectIdSerde, Serde.ThriftSerde(Unit => new GeocodeServingFeature))

  case object IdMappingIndex extends Index[String, ObjectId](
    "id-mapping", Serde.StringSerde, Serde.ObjectIdSerde)

  case object S2Index extends Index[Long, CellGeometries](
    "s2_index", Serde.LongSerde, Serde.ThriftSerde(Unit => new CellGeometries))

  case object PrefixIndex extends Index[String, Seq[ObjectId]](
    "prefix_index", Serde.StringSerde, Serde.ObjectIdListSerde)

  case object NameIndex extends Index[String, Seq[ObjectId]](
    "name_index.hfile", Serde.StringSerde, Serde.ObjectIdListSerde)
}


