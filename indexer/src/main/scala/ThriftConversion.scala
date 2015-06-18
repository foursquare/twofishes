// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.common.thrift

import com.foursquare.common.reflection.ScalaReflection
import com.foursquare.spindle.{Record, RecordProvider}
import java.lang.reflect.Modifier
import org.apache.thrift.{TBase, TDeserializer, TFieldIdEnum, TSerializer}
import org.apache.thrift.protocol.{TCompactProtocol, TProtocolFactory}

trait ThriftDeserializer[T <: ThriftConverter.TType] {
  def deserialize(raw: Array[Byte]): T
  /** Deserialize the bytes into T */
  def deserialize(raw: Array[Byte], t: T): Unit
}

trait ThriftSerializer[T <: ThriftConverter.TType] {
  def serialize(t: T): Array[Byte]
}

/** Uses the given RecordProvider to generate records to deserialize into. Can also override protocolFactory, which
defaults to TCompactProtocol. */
class ThriftConverter[T <: ThriftConverter.TType](rp: RecordProvider[T],
    protocolFactory: TProtocolFactory = new TCompactProtocol.Factory) extends ThriftDeserializer[T]
    with ThriftSerializer[T] {

  val deserializer = new TDeserializer(protocolFactory)
  val serializer = new TSerializer(protocolFactory)

  def this()(implicit m: Manifest[T]) = this(ThriftConverter.recordProvider(m.runtimeClass.asInstanceOf[Class[T]]))
  def this(clazz: Class[T]) = this(ThriftConverter.recordProvider(clazz))
  def this(protocolFactory: TProtocolFactory)(implicit m: Manifest[T]) =
    this(ThriftConverter.recordProvider(m.runtimeClass.asInstanceOf[Class[T]]), protocolFactory)
  def this(clazz: Class[T], protocolFactory: TProtocolFactory) =
    this(ThriftConverter.recordProvider(clazz), protocolFactory)

  def deserialize(raw: Array[Byte]): T = {
    val t = rp.createRecord
    deserializer.deserialize(t, raw)
    t
  }
  def deserialize(raw: Array[Byte], t: T): Unit = {
    t.clear()
    deserializer.deserialize(t, raw)
  }

  def serialize(t: T): Array[Byte] = {
    serializer.serialize(t)
  }
}

object ThriftConverter {
  type TType = TBase[_ <: TBase[_ <: AnyRef, _ <: TFieldIdEnum], _ <: TFieldIdEnum] with Record[_]

  /** Get the RecordProvider (companion object) given the class.
    * The class can be either a Raw type (e.g. RawCheckinModel) or a non-Raw type (e.g. CheckinModel).
    */
  def recordProvider[T <: TType](clazz: Class[T]): RecordProvider[T] = {
    // This is a hack to support Raw types. It seems that this is the easiest way to get access to the companion obj
    // for a Raw type, even though it's horrible and circular.
    if (!Modifier.isAbstract(clazz.getModifiers)) {
      clazz.newInstance.meta.asInstanceOf[RecordProvider[T]]
    } else {
      ScalaReflection.objectFromName(clazz.getName).asInstanceOf[RecordProvider[T]]
    }
  }
}
