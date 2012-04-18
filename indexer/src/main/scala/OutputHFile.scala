package com.foursquare.twofishes

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.util.Arrays

import org.apache.hadoop.conf.Configuration 
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.KeyValue.KeyComparator
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, Compression, HFile, HFileScanner, HFileWriterV2}
import org.apache.hadoop.hbase.util.Bytes._

import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TBinaryProtocol

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class OutputHFile(basepath: String) {
  val blockSizeKey = "hbase.mapreduce.hfileoutputformat.blocksize"
  val compressionKey = "hfile.compression"

  val blockSize = HFile.DEFAULT_BLOCKSIZE
  val compressionAlgo = Compression.Algorithm.NONE.getName

  val conf = new Configuration();
  val cconf = new CacheConfig(conf);

  def writeCollection[T <: AnyRef, K <: Any](
    filename: String,
    callback: (T) => (Array[Byte], Array[Byte]),
    dao: SalatDAO[T, K],
    sortField: String
  ) {
    val fs = new LocalFileSystem() 
    val path = new Path(new File(basepath, filename).toString)
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

  def writeNames() {
    val comp = new ByteArrayComparator()
    val nameMap = new HashMap[Array[Byte], List[String]]
    var nameCount = 0
    val nameSize = NameIndexDAO.collection.count
    val nameCursor = NameIndexDAO.find(MongoDBObject())
    nameCursor.filterNot(_.name.isEmpty).foreach(n => {
      nameMap(n.name.getBytes()) = n.fids
      nameCount += 1
      if (nameCount % 100000 == 0) {
        println("processed %d of %d names".format(nameCount, nameSize))
      }
    })

    val fpath = new File(basepath, "name_index.hfile").toString
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
    val f = g.toGeocodeServingFeature()
    val parentOids = f.scoringFeatures.parents.flatMap(fid => fidMap.get(fid)).map(_.toString)
    f.scoringFeatures.setParents(parentOids)
    serializer.serialize(f)
  }

  val fidMap = new HashMap[String, ObjectId]

  def process() {
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

    writeCollection("features.hfile",
      (g: GeocodeRecord) => 
        (g._id.toByteArray(), serializeBytes(g)),
      MongoGeocodeDAO, "_id")
  }
}