package com.foursquare.twofishes

import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.util.Arrays

import org.apache.hadoop.conf.Configuration 
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.KeyValue.KeyComparator
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, Compression, HFile, HFileScanner}
import org.apache.hadoop.hbase.util.Bytes._

import org.apache.thrift.{TDeserializer}
import org.apache.thrift.protocol.TBinaryProtocol

import org.bson.types.ObjectId

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class HFileStorageService(basepath: String) extends GeocodeStorageReadService {
  val nameMap = new NameIndexHFileInput(basepath)
  val oidMap = new GeocodeRecordHFileInput(basepath)

  def getByNamePrefix(name: String): Seq[GeocodeServingFeature] = {
    nameMap.getPrefix(name).flatMap(oid => {
      oidMap.get(oid)
    })
  }

  def getByName(name: String): Seq[GeocodeServingFeature] = {
    nameMap.get(name).flatMap(oid => {
      oidMap.get(oid)
    })
  }

  def getByObjectIds(oids: Seq[ObjectId]): Map[ObjectId, GeocodeServingFeature] = {
    oids.flatMap(oid => oidMap.get(oid).map(r => (oid -> r))).toMap
  }
}

abstract class HFileInput(basepath: String, filename: String) {
  val conf = new Configuration()
  val fs = new LocalFileSystem()
  fs.initialize(URI.create("file:///"), conf)

  val path = new Path(new File(basepath, filename).toString)
  val cacheConfig = new CacheConfig(conf)
  println(cacheConfig)
  val reader = HFile.createReader(fs, path, cacheConfig)
  reader.loadFileInfo()

  def lookup(key: ByteBuffer): Option[ByteBuffer] = {
    val scanner: HFileScanner = reader.getScanner(true, true)
    if (scanner.reseekTo(key.array, key.position, key.remaining) == 0) {
      Some(scanner.getValue.duplicate())
    } else {
      None
    }
  }

  import scala.collection.mutable.ListBuffer
  
  def lookupPrefix(key: String): Seq[Array[Byte]] = {
    val scanner: HFileScanner = reader.getScanner(true, true)
    scanner.seekTo(key.getBytes())
    scanner.next()

    val ret: ListBuffer[Array[Byte]] = new ListBuffer()

    while (scanner.getKeyValue().getKeyString().startsWith(key)) {
      ret.append(scanner.getKeyValue().getValue())
    }

    ret
  }
}

class NameIndexHFileInput(basepath: String) extends HFileInput(basepath, "name_index.hfile") {
  def decodeObjectIds(bytes: Array[Byte]): Seq[ObjectId] = {
    0.until(bytes.length / 12).map(i => {
      new ObjectId(Arrays.copyOfRange(bytes, i * 12, (i + 1) * 12))
    })
  }

  def get(name: String): List[ObjectId] = {
    val buf = ByteBuffer.wrap(name.getBytes())
    lookup(buf).toList.flatMap(b => {
      val bytes = new Array[Byte](b.capacity())
      b.get(bytes, 0, bytes.length);
      decodeObjectIds(bytes)
    })
  }

  def getPrefix(name: String): Seq[ObjectId] = {
    lookupPrefix(name).flatMap(bytes => {
      decodeObjectIds(bytes)
    })
  }
}

class GeocodeRecordHFileInput(basepath: String) extends HFileInput(basepath, "features.hfile") {
  import java.io._
  def deserializeBytes(bytes: Array[Byte]) = {
    val deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    val feature = new GeocodeServingFeature();
    deserializer.deserialize(feature, bytes);
    feature
  }

  def get(oid: ObjectId): Option[GeocodeServingFeature] = {
    val buf = ByteBuffer.wrap(oid.toByteArray())
    lookup(buf).map(b => {
      val bytes = new Array[Byte](b.capacity())
      b.get(bytes, 0, bytes.length);
      deserializeBytes(bytes)
    })
  }
}