package com.foursquare.twofish

import org.apache.hadoop.hbase.io.hfile.{Compression, HFile}
import org.apache.hadoop.hbase.io.hfile.HFileWriterV2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path

import com.novus.salat._
import com.novus.salat.global._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection

import org.apache.hadoop.conf.Configuration 
import org.apache.hadoop.hbase.io.hfile.CacheConfig

import java.net.URI
import java.util.Arrays

import org.apache.hadoop.fs.permission.FsPermission

import java.nio.ByteBuffer
import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFile, HFileScanner}

import org.apache.thrift.TSerializer
import org.apache.thrift.TDeserializer
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.protocol.TSimpleJSONProtocol
import org.apache.thrift.transport.TIOStreamTransport

class HFileStorageService extends GeocodeStorageReadService {
  val nameMap = new NameIndexHFileInput
  val fidMap = new FidIndexHFileInput
  val oidMap = new GeocodeRecordHFileInput

  def getByName(name: String): Iterator[GeocodeFeature] = {
    nameMap.get(name).flatMap(oid => {
      oidMap.get(oid)
    }).iterator
  }

  def getByObjectIds(oids: Seq[ObjectId]): Iterator[GeocodeFeature] = {
    oids.flatMap(oid => oidMap.get(oid)).iterator
  }
  def getByIds(fids: Seq[String]): Iterator[GeocodeFeature] = {
    fids.flatMap(fid => {
      fidMap.get(fid).flatMap(oid => oidMap.get(oid))
    }).iterator
  }

  def getById(id: StoredFeatureId): Iterator[GeocodeFeature] = {
    getByIds(List(id.toString))
  }
}


abstract class HFileInput(hfile: String) {
  val conf = new Configuration()
  val fs = new LocalFileSystem()
  fs.initialize(URI.create("file:///"), conf)
  val path = new Path(hfile)
  val cacheConfig = new CacheConfig(conf)
  println(cacheConfig)
  val reader = HFile.createReader(fs, path, cacheConfig)
  reader.loadFileInfo()

  // def readSome(limit: Int) {
  //   val scanner: HFileScanner = reader.getScanner(true, true)
  //   var count = 0
  //   scanner.seekTo()

  //   0.to(limit).foreach(i => {
  //     println(new ObjectIdscanner.getKeyString())
  //     val b = scanner.getValue()
  //     val bytes = new Array[Byte](b.capacity())
  //     b.get(bytes, 0, bytes.length);
  //     println(new ObjectId(bytes))
  //     scanner.next()
  //   })

  // }

  def lookup(key: ByteBuffer): Option[ByteBuffer] = {
    val scanner: HFileScanner = reader.getScanner(true, true)
    if (scanner.reseekTo(key.array, key.position, key.remaining) == 0) {
      Some(scanner.getValue.duplicate())
    } else {
      None
    }
  }
}

class NameIndexHFileInput extends HFileInput("/export/hdc3/appdata/geonames-hfile/name_index.hfile") {
  def get(name: String): List[ObjectId] = {
    val buf = ByteBuffer.wrap(name.getBytes())
    lookup(buf).toList.flatMap(b => {
      val bytes = new Array[Byte](b.capacity())
      b.get(bytes, 0, bytes.length);

      0.until(bytes.length / 12).map(i => {
        new ObjectId(Arrays.copyOfRange(bytes, i * 12, (i + 1) * 12))
      })
    })
  }
}

class FidIndexHFileInput extends HFileInput("/export/hdc3/appdata/geonames-hfile/fid_index.hfile") {
  def get(fid: String): Option[ObjectId] = {
    val buf = ByteBuffer.wrap(fid.getBytes())
    lookup(buf).map(b => {
      val bytes = new Array[Byte](b.capacity())
      b.get(bytes, 0, bytes.length);
      new ObjectId(bytes)
    })
  }
}

class GeocodeRecordHFileInput extends HFileInput("/export/hdc3/appdata/geonames-hfile/features.hfile") {
  import java.io._
  def deserializeBytes(bytes: Array[Byte]) = {
    val deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    val feature = new GeocodeFeature();
    deserializer.deserialize(feature, bytes);
    feature
  }

  def get(oid: ObjectId): Option[GeocodeFeature] = {
    val buf = ByteBuffer.wrap(oid.toByteArray())
    lookup(buf).map(b => {
      val bytes = new Array[Byte](b.capacity())
      b.get(bytes, 0, bytes.length);
      deserializeBytes(bytes)
    })
  }
}

object OutputHFile {
  val blockSizeKey = "hbase.mapreduce.hfileoutputformat.blocksize"
  val compressionKey = "hfile.compression"

  val blockSize = HFile.DEFAULT_BLOCKSIZE
  val compressionAlgo = Compression.Algorithm.NONE.getName

  // val fs = new RawLocalFileSystem() 
  // val path = new Path("/export/hdc3/appdata/geonames-hfile/fid_index.hfile")
  // val writer = new HFile.Writer(fs, path, blockSize, compressionAlgo, null)
  // var fidCount = 0
  // val fidSize = FidIndexDAO.collection.count
  // val fidCursor = FidIndexDAO.find(MongoDBObject())
  // fidCursor.foreach(f => {
  //   writer.append(f.fid.getBytes(), f.oid.toByteArray())
  //   fidCount += 1
  //   if (fidCount % 1000 == 0) {
  //     println("processed %d of %d fids".format(fidCount, fidSize))
  //   }
  // })
  // writer.close()

  val conf = new Configuration();
  val cconf = new CacheConfig(conf);

  def writeCollection[T <: AnyRef, K <: Any](
    fpath: String,
    callback: (T) => (Array[Byte], Array[Byte]),
    dao: SalatDAO[T, K],
    sortField: String
  ) {
    val fs = new LocalFileSystem() 
    val path = new Path(fpath)
    fs.initialize(URI.create("file:///"), conf)
    val writer = new HFileWriterV2(conf, cconf, fs, path, blockSize, compressionAlgo, null)
    var fidCount = 0
    val fidSize = dao.collection.count
    val fidCursor = dao.find(MongoDBObject())
      .sort(orderBy = MongoDBObject(sortField -> 1)) // sort by _id desc
    fidCursor.foreach(f => {
      val (k, v) = callback(f)
      writer.append(k, v)
      fidCount += 1
      if (fidCount % 1000 == 0) {
        println("processed %d of %d %s".format(fidCount, fidSize, path))
      }
    })
    writer.close()
  }

  import scala.collection.mutable.HashMap

  import org.apache.hadoop.hbase.KeyValue.KeyComparator

  import org.apache.hadoop.hbase.util.Bytes._

  def writeNames() {
    val comp = new ByteArrayComparator()
    val nameMap = new HashMap[Array[Byte], List[String]]
    var nameCount = 0
    val nameSize = NameIndexDAO.collection.count
    val nameCursor = NameIndexDAO.find(MongoDBObject())
    nameCursor.foreach(n => {
      nameMap(n.name.getBytes()) = n.fids
      nameCount += 1
      if (nameCount % 100000 == 0) {
        println("processed %d of %d names".format(nameCount, nameSize))
      }
    })

    val fpath = "/export/hdc3/appdata/geonames-hfile/name_index.hfile"
    val fs = new LocalFileSystem() 
    val path = new Path(fpath)
    fs.initialize(URI.create("file:///"), conf)
    val writer = new HFileWriterV2(conf, cconf, fs, path, blockSize, compressionAlgo, null)

    println("sorting")

    val sortedMap = nameMap.keys.toList.sort((a, b) => {
        comp.compare(a, b) < 0
    })

    println("sorted")

    sortedMap.map(n => {
      val fids = nameMap(n)
      val oids = fids.flatMap(fid => fidMap.get(fid)).toSet
      val os = new ByteArrayOutputStream(12 * oids.size)
      oids.foreach(oid =>
        os.write(oid.toByteArray)
      )

      writer.append(n, os.toByteArray())
    })
    writer.close()
    println("done")
  }

  import java.io._
  def serializeBytes(g: GeocodeRecord) = {
    val serializer = new TSerializer(new TBinaryProtocol.Factory());
    serializer.serialize(g.toGeocodeFeature(Map.empty, true, None))
  }

  val fidMap = new HashMap[String, ObjectId]

  def main(args: Array[String]) {
    writeCollection("/export/hdc3/appdata/geonames-hfile/fid_index.hfile",
      (f: FidIndex) => (f.fid.getBytes(), f.oid.toByteArray()),
      FidIndexDAO, "_id")

    var fidCount = 0
    val fidSize = FidIndexDAO.collection.count
    val fidCursor = FidIndexDAO.find(MongoDBObject())
    fidCursor.foreach(f => {
      fidMap(f.fid) = f.oid
      fidCount += 1
      if (fidCount % 50000 == 0) {
        println("processed %d of %d fids in memory".format(fidCount, fidSize))
      }
    })

    writeNames()

    // writeCollection("/export/hdc3/appdata/geonames-hfile/name_index.hfile",
    //   (n: NameIndex) => (n.name.getBytes(), n.fids.mkString(",").getBytes()),
    //   NameIndexDAO, "nameBytes")

    writeCollection("/export/hdc3/appdata/geonames-hfile/features.hfile",
      (g: GeocodeRecord) => 
        (g._id.toByteArray(), serializeBytes(g)),
      MongoGeocodeDAO, "_id")
  }
}