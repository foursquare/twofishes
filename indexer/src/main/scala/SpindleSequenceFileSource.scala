// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.hadoop.scalding

import cascading.scheme.Scheme
import com.foursquare.common.thrift.ThriftConverter
import com.foursquare.hadoop.cascading.io.SpindleSequenceFile
import com.twitter.scalding._
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader}
import org.apache.hadoop.io.{BytesWritable, NullWritable, Writable}

case class SpindleSequenceFileSource[K <: Writable: Manifest, T <: ThriftConverter.TType](paths: Seq[String])(implicit mf: Manifest[T], conv: TupleConverter[(K, T)], tset: TupleSetter[(K, T)])
  extends FixedPathSource(paths: _*) with Mappable[(K, T)] with TypedSink[(K, T)] {

  // TODO(blackmad): Now that we're passing manifests, I don't think we really need these anymore
  val classOfT: Class[T] = manifest[T].runtimeClass.asInstanceOf[Class[T]]
  val classOfK: Class[K] = manifest[K].runtimeClass.asInstanceOf[Class[K]]

  override def hdfsScheme = (SpindleSequenceFile[K, T](classOfK, classOfT)).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

  // Apparently Cascading doesn't support sequence files in local mode???
  override def localScheme = ???

  override def converter[U >: (K, T)]: TupleConverter[U] = TupleConverter.asSuperConverter[(K, T), U](conv)

  override def setter[U <: (K, T)]: TupleSetter[U] = TupleSetter.asSubSetter[(K, T), U](tset)
}

object SpindleSequenceFileSource {
  def apply[K <: Writable : Manifest, T <: ThriftConverter.TType: Manifest: TupleConverter](path: String) = new SpindleSequenceFileSource[K, T](Seq(path))
}
