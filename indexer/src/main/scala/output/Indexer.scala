  package com.foursquare.twofishes.output

import com.foursquare.twofishes.{Index, MapFileUtils}
import com.foursquare.twofishes.util.{DurationUtils, StoredFeatureId}
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.io._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.{Compression, HFile, TwofishesFoursquareCacheConfig}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.hadoop.io.{BytesWritable, MapFile}
import org.apache.thrift.protocol.TCompactProtocol
import scalaj.collection.Implicits._

trait WrappedWriter[K, V] {
  def append(k: K, v: V)
  def close()
}

class WrappedByteMapWriter[K, V](writer: MapFile.Writer, index: Index[K, V]) extends WrappedWriter[K, V] {
  override def append(k: K, v: V) {
    writer.append(new BytesWritable(index.keySerde.toBytes(k)), new BytesWritable(index.valueSerde.toBytes(v)))
  }

  override def close() { writer.close() }
}

class WrappedHFileWriter[K, V](writer: HFile.Writer, index: Index[K, V]) extends WrappedWriter[K, V] {
  override def append(k: K, v: V) {
    writer.append(index.keySerde.toBytes(k), index.valueSerde.toBytes(v))
  }

  override def close() { writer.close() }
}

abstract class Indexer extends DurationUtils {
  def basepath: String
  def fidMap: FidMap

  def outputs: Seq[Index[_, _]]

  def writeIndexImpl(): Unit

  def writeIndex() {
    val name = this.getClass.getName

    if (outputs.forall(_.exists(basepath))) {
      logger.info("had all indexes for %s, skipping this phase".format(name))
    } else {
      logger.info("starting indexing for %s".format(name))
      logDuration(name) { writeIndexImpl() }
      logger.info("done indexing for %s".format(name))
    }
  }

  val ThriftClassValue: String = "value.thrift.class"
  val ThriftClassKey: String = "key.thrift.class"
  val ThriftEncodingKey: String = "thrift.protocol.factory.class"

  val factory = new TCompactProtocol.Factory()

  // this is all weirdly 4sq specific logic :-(
  def fixThriftClassName(n: String) = {
    if (n.contains("com.foursquare.twofishes.gen")) {
      n
    } else {
      n.replace("com.foursquare.twofishes", "com.foursquare.twofishes.gen")
        .replace("com.foursquare.base.thrift", "com.foursquare.base.gen")

    }
  }

  def buildHFileV1Writer[K, V](index: Index[K, V],
                         info: Map[String, String] = Map.empty): WrappedHFileWriter[K, V] = {
    val conf = new Configuration()
    val blockSizeKey = "hbase.mapreduce.hfileoutputformat.blocksize"
    val compressionKey = "hfile.compression"

    val blockSize = 16384
    val compressionAlgo = Compression.Algorithm.NONE.getName

    val fs = new LocalFileSystem()
    val path = new Path(new File(basepath, index.filename).toString)
    fs.initialize(URI.create("file:///"), conf)
    val hadoopConfiguration: Configuration = new Configuration()

    val compressionAlgorithm: Compression.Algorithm =
      Compression.getCompressionAlgorithmByName("none")

    val writer = HFile.getWriterFactory(hadoopConfiguration, new TwofishesFoursquareCacheConfig(hadoopConfiguration))
      .withPath(fs, path)
      .withBlockSize(blockSize)
      .withCompression(compressionAlgorithm)
      .create()

    info.foreach({case (k, v) => writer.appendFileInfo(k.getBytes("UTF-8"), v.getBytes("UTF-8")) })

    new WrappedHFileWriter(writer, index)
  }

  def buildMapFileWriter[K : Manifest, V : Manifest](
      index: Index[K, V],
      info: Map[String, String] = Map.empty,
      indexInterval: Option[Int] = None) = {

    val keyClassName = fixThriftClassName(manifest[K].erasure.getName)
    val valueClassName = fixThriftClassName(manifest[V].erasure.getName)

    val finalInfoMap = info ++ Map(
      (ThriftClassKey, keyClassName),
      (ThriftClassValue, valueClassName),
      (ThriftEncodingKey, factory.getClass.getName)
    )

    val opts = indexInterval.map(i => MapFileUtils.DefaultByteKeyValueWriteOptions.copy(indexInterval = i))
      .getOrElse(MapFileUtils.DefaultByteKeyValueWriteOptions)

    new WrappedByteMapWriter(
      MapFileUtils.writerToLocalPath((new File(basepath, index.filename)).toString, finalInfoMap, opts),
      index
    )
  }

  val comp = new ByteArrayComparator()

  def lexicalSort(a: String, b: String) = {
    comp.compare(a.getBytes(), b.getBytes()) < 0
  }

  def fidsToCanonicalFids(fids: List[StoredFeatureId]): Seq[StoredFeatureId] = {
    fids.flatMap(fid => fidMap.get(fid)).distinct
  }
}
