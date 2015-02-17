// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.hadoop.cascading.io

import cascading.flow.FlowProcess
import cascading.scheme.{Scheme, SinkCall, SourceCall}
import cascading.tap.Tap
import cascading.tuple.Fields
import com.foursquare.common.thrift.ThriftConverter
import org.apache.hadoop.io.{BytesWritable, NullWritable, Writable}
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader, SequenceFileInputFormat,
    SequenceFileOutputFormat}
import scala.reflect.runtime.universe._

/**
 * Basic implementation of [[cascading.scheme.Scheme]] that can read and write Hadoop sequence files containing
 * Spindle records. The implementation merges aspects of [[cascading.scheme.hadoop.SequenceFile]] and
 * [[cascading.scheme.hadoop.WritableSequenceFile]].
 *
 * Limitations:
 * - The Thrift record is stored as a single field in the [[cascading.tuple.Tuple]] by the deserializer. The
 *   serializer expects a single field in the outgoing [[cascading.tuple.Tuple]] of the expected record type.
 */
class SpindleSequenceFile[K <: Writable: Manifest, T <: ThriftConverter.TType](
  val classOfK: Class[K],
  val classOfT: Class[T])
  extends Scheme[
    JobConf,
    RecordReader[K, BytesWritable],
    OutputCollector[K, BytesWritable],
    SpindleSequenceFile.Context[T],
    SpindleSequenceFile.Context[T]
    ](
      new Fields(0, 1),
      new Fields(0, 1)
    ) {

  private def makeContext(): SpindleSequenceFile.Context[T] = {
    SpindleSequenceFile.Context(new ThriftConverter(classOfT))
  }

  //
  // Input Methods (read from a sequence file)
  //

  @throws[java.io.IOException]
  override def sourceConfInit(
      flowProcess: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[K, BytesWritable], OutputCollector[K, BytesWritable]],
      conf: JobConf): Unit = {
    conf.setInputFormat(classOf[SequenceFileInputFormat[K, BytesWritable]])
  }

  override def sourcePrepare(
      flowProcess: FlowProcess[JobConf],
      sourceCall: SourceCall[SpindleSequenceFile.Context[T], RecordReader[K, BytesWritable]]): Unit = {
    sourceCall.setContext(makeContext())
  }

  @throws[java.io.IOException]
  override def source(
      flowProcess: FlowProcess[JobConf],
      sourceCall: SourceCall[SpindleSequenceFile.Context[T], RecordReader[K, BytesWritable]]): Boolean = {

    val key = manifest[K].runtimeClass.newInstance.asInstanceOf[K]
    val valueAsBytes = new BytesWritable()

    val result = sourceCall.getInput.next(key, valueAsBytes)

    if (result) {
      val context = sourceCall.getContext
      val record = context.thriftConverter.deserialize(valueAsBytes.getBytes)

      val tupleEntry = sourceCall.getIncomingEntry
      tupleEntry.setObject(0, key)
      tupleEntry.setObject(1, record)

      true
    } else {
      false
    }
  }

  override def sourceCleanup(
      flowProcess: FlowProcess[JobConf],
      sourceCall: SourceCall[SpindleSequenceFile.Context[T], RecordReader[K, BytesWritable]]): Unit = {
    sourceCall.setContext(null)
  }


  //
  // Output Methods (write to a sequence file)
  //

  @throws[java.io.IOException]
  override def sinkConfInit(
      flowProcess: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[K, BytesWritable], OutputCollector[K, BytesWritable]],
      conf: JobConf): Unit = {
    conf.setOutputKeyClass(classOfK)
    conf.setOutputValueClass(classOf[BytesWritable])
    conf.setOutputFormat(classOf[SequenceFileOutputFormat[K, BytesWritable]])
  }

  override def sinkPrepare(
      flowProcess: FlowProcess[JobConf],
      sinkCall: SinkCall[SpindleSequenceFile.Context[T], OutputCollector[K, BytesWritable]]): Unit = {
    sinkCall.setContext(makeContext())
  }

  @throws[java.io.IOException]
  override def sink(
      flowProcess: FlowProcess[JobConf],
      sinkCall: SinkCall[SpindleSequenceFile.Context[T], OutputCollector[K, BytesWritable]]): Unit = {
    val tupleEntry = sinkCall.getOutgoingEntry

    // we should do the fancy "if nullwritable is the K, th"
    val isNullKey = classOfK == classOf[NullWritable]
    val keyValue = if (isNullKey) {
      NullWritable.get.asInstanceOf[K]
    } else {
      tupleEntry.getObject(0).asInstanceOf[K]
    }

    val valueAsSpindle = {
      if (isNullKey && tupleEntry.size() != 1) {
        throw new IllegalArgumentException("Tuples to sequence files must contain only the record to output if NullWritable is the keytype.")
      }

      //println("SIZE: " + tupleEntry.size)
      //println("ENTRY: " + tupleEntry)
      if (!isNullKey && tupleEntry.size() != 2) {
        throw new IllegalArgumentException("Tuples to sequence files must contain a key and a record.")
      }

      val valueAsAnyRef: AnyRef = if (isNullKey) {
        tupleEntry.getObject(0)
      } else {
        tupleEntry.getObject(1)
      }
      if (classOfT.isAssignableFrom(valueAsAnyRef.getClass)) {
        valueAsAnyRef.asInstanceOf[T]
      } else {
        throw new IllegalArgumentException(s"Value of type ${valueAsAnyRef.getClass.getName} does not conform to ${classOfT.getName}.")
      }
    }

    val valueAsBytes = {
      val context = sinkCall.getContext
      context.thriftConverter.serialize(valueAsSpindle)
    }

    sinkCall.getOutput.collect(keyValue, new BytesWritable(valueAsBytes))
  }

  override def sinkCleanup(
      flowProcess: FlowProcess[JobConf],
      sinkCall: SinkCall[SpindleSequenceFile.Context[T], OutputCollector[K, BytesWritable]]): Unit = {
    sinkCall.setContext(null)
  }

  //
  // Scheme requires a well-formed .equals() and .hashCode().
  //

  override def equals(that: Any): Boolean = that match {
    case null => false
    case that: SpindleSequenceFile[_, _] => this == that || (
      this.classOfT.isAssignableFrom(that.classOfT) &&
      this.classOfK.isAssignableFrom(that.classOfK))
    case _ => false
  }

  override def hashCode(): Int = {
    val hash = super.hashCode()
    31 * hash + classOfT.hashCode() + classOfK.hashCode()
  }
}

object SpindleSequenceFile {
  /**
   * Context used to hold state during calls to the source and sink methods.
   *
   * The ThriftConverter[T] is stored in this context to ensure there are no race conditions since it is not
   * thread-safe.
   */
  case class Context[T <: ThriftConverter.TType](thriftConverter: ThriftConverter[T])

  def apply[K <: Writable: Manifest, T <: ThriftConverter.TType](clazzK: Class[K], clazz: Class[T]): SpindleSequenceFile[K, T] = {
    new SpindleSequenceFile[K, T](clazzK, clazz)
  }
}
