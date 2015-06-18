// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding.output

import com.foursquare.common.thrift.ThriftConverter
import com.foursquare.twofishes._
import com.foursquare.twofishes.output._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Writable, BytesWritable}
import org.apache.hadoop.io.SequenceFile.Reader

object IndexOutputType extends Enumeration {
  val HFILE_OUTPUT,
      MAPFILE_OUTPUT = Value
}

case class IndexerOptions(
  outputType: IndexOutputType.Value,
  info: Map[String, String] = Map.empty,
  mapFileIndexInterval: Option[Int] = None)

class BaseIndexer[
  ScaldingK <: Writable: Manifest,
  ScaldingV <: ThriftConverter.TType: Manifest,
  IndexK: Manifest,
  IndexV: Manifest
](
  inputBaseDir: String,
  outputBaseDir: String,
  index: Index[IndexK, IndexV],
  scaldingIntermediateJobName: String,
  options: IndexerOptions,
  processor: (ScaldingK, ScaldingV) => Option[(IndexK, IndexV)]
) extends Indexer {

  override def basepath: String = outputBaseDir
  override val fidMap: FidMap = null
  override val outputs = Seq(index)

  val conf = new Configuration
  val inputPathString = inputBaseDir + "/" + scaldingIntermediateJobName + "/part-00000"
  val inputPath = new Path(inputPathString)
  val reader = new Reader(conf, Reader.file(inputPath))

  val thriftConverter = new ThriftConverter(manifest[ScaldingV].runtimeClass.asInstanceOf[Class[ScaldingV]])

  private def createKey: ScaldingK = manifest[ScaldingK].runtimeClass.newInstance.asInstanceOf[ScaldingK]

  override def writeIndexImpl(): Unit = {
    val key = createKey
    val value = new BytesWritable()

    val writer: WrappedWriter[IndexK, IndexV] = options.outputType match {
      case IndexOutputType.HFILE_OUTPUT => buildHFileV1Writer(index, options.info)
      case _ => buildMapFileWriter(index, options.info, options.mapFileIndexInterval)
    }

    while (reader.next(key, value)) {
      processor(key, thriftConverter.deserialize(value.getBytes))
        .foreach({case (indexKey, indexValue) => writer.append(indexKey, indexValue)})
    }
    writer.close()
  }
}
