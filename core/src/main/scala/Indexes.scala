package com.foursquare.twofishes

import com.foursquare.twofishes.util.{ByteUtils, StoredFeatureId}
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import java.io.File
import java.nio.ByteBuffer
import java.util.Arrays
import org.apache.thrift.{TBase, TDeserializer, TFieldIdEnum, TSerializer}
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

  case object LongListSerde extends Serde[Seq[Long]] {
    override def toBytes(t: Seq[Long]): Array[Byte] = {
      val buf = ByteBuffer.wrap(new Array[Byte](t.size * 8))
      t.foreach(l => buf.putLong(l))
      buf.array()
    }

    override def fromBytes(bytes: Array[Byte]): Seq[Long] = {
      val buf = ByteBuffer.wrap(bytes)
      0.until(bytes.length / 8).map(i => buf.getLong)
    }
  }

  case object StringSerde extends Serde[String] {
    override def toBytes(t: String): Array[Byte] = t.getBytes("UTF-8")
    override def fromBytes(bytes: Array[Byte]): String = new String(bytes, "UTF-8")
  }

  case object ObjectIdSerde extends Serde[ObjectId] {
    override def toBytes(t: ObjectId): Array[Byte] = t.toByteArray
    override def fromBytes(bytes: Array[Byte]): ObjectId = new ObjectId(bytes)
  }

  case object ObjectIdListSerde extends Serde[Seq[ObjectId]] {
    override def toBytes(t: Seq[ObjectId]): Array[Byte] = {
      val buf = ByteBuffer.wrap(new Array[Byte](t.size * 12))
      t.foreach(oid => buf.put(oid.toByteArray))
      buf.array()
    }
    override def fromBytes(bytes: Array[Byte]): Seq[ObjectId] = {
      0.until(bytes.length / 12).map(i => {
        new ObjectId(Arrays.copyOfRange(bytes, i * 12, (i + 1) * 12))
      })
    }
  }

  case object StoredFeatureIdSerde extends Serde[StoredFeatureId] {
    val impl = LongSerde

    override def toBytes(t: StoredFeatureId): Array[Byte] = impl.toBytes(t.longId)
    override def fromBytes(bytes: Array[Byte]): StoredFeatureId = {
      val id = impl.fromBytes(bytes)
      StoredFeatureId.fromLong(id).getOrElse(
        throw new RuntimeException("couldn't deserialize StoredFeatureId from %s".format(id)))
    }
  }

  case object StoredFeatureIdListSerde extends Serde[Seq[StoredFeatureId]] {
    val impl = LongListSerde

    override def toBytes(t: Seq[StoredFeatureId]): Array[Byte] = {
      impl.toBytes(t.map(_.longId))
    }
    override def fromBytes(bytes: Array[Byte]): Seq[StoredFeatureId] = {
      val ids = impl.fromBytes(bytes)
      ids.map(id =>
        StoredFeatureId.fromLong(id).getOrElse(
          throw new RuntimeException("couldn't deserialize StoredFeatureId from %s".format(id))))
    }
  }

  case object GeometrySerde extends Serde[Geometry] {
    override def toBytes(t: Geometry): Array[Byte] = {
      val wkbWriter = new WKBWriter
      wkbWriter.write(t)
    }
    override def fromBytes(bytes: Array[Byte]): Geometry = {
      val wkbReader = new WKBReader
      wkbReader.read(bytes)
    }
  }

  case object TrivialSerde extends Serde[Array[Byte]] {
    override def toBytes(t: Array[Byte]): Array[Byte] = t
    override def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
  }

  case class ThriftSerde[T <: TBase[_ <: TBase[_ <: AnyRef, _ <: TFieldIdEnum], _ <: TFieldIdEnum]](
      factory: Unit => T) extends Serde[T] {
    val protFactory = new TCompactProtocol.Factory

    def toBytes(t: T): Array[Byte] = {
      val serializer = new TSerializer(protFactory)
      serializer.serialize(t)
    }

    def fromBytes(bytes: Array[Byte]): T = {
      val s = factory()
      val deserializer = new TDeserializer(protFactory)
      deserializer.deserialize(s, bytes)
      s
    }
  }
}

sealed abstract class Index[K, V](val filename: String, val keySerde: Serde[K], val valueSerde: Serde[V]) {
  def exists(basepath: String) = {
    new File(basepath, filename).exists
  }
}

object Indexes {
  case object GeometryIndex extends Index[StoredFeatureId, Geometry](
    "geometry", Serde.StoredFeatureIdSerde, Serde.GeometrySerde)

  case object S2CoveringIndex extends Index[StoredFeatureId, Seq[Long]](
    "s2_covering", Serde.StoredFeatureIdSerde, Serde.LongListSerde)

  case object S2InteriorIndex extends Index[StoredFeatureId, Seq[Long]](
    "s2_interior", Serde.StoredFeatureIdSerde, Serde.LongListSerde)

  case object FeatureIndex extends Index[StoredFeatureId, GeocodeServingFeature](
    "features", Serde.StoredFeatureIdSerde, Serde.ThriftSerde(Unit => new RawGeocodeServingFeature))

  case object IdMappingIndex extends Index[String, StoredFeatureId](
    "id-mapping", Serde.StringSerde, Serde.StoredFeatureIdSerde)

  case object S2Index extends Index[Long, CellGeometries](
    "s2_index", Serde.LongSerde, Serde.ThriftSerde(Unit => new RawCellGeometries))

  case object PrefixIndex extends Index[String, Seq[StoredFeatureId]](
    "prefix_index", Serde.StringSerde, Serde.StoredFeatureIdListSerde)

  case object NameIndex extends Index[String, Seq[StoredFeatureId]](
    "name_index.hfile", Serde.StringSerde, Serde.StoredFeatureIdListSerde)
}


