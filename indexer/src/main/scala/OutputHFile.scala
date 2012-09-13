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
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, Compression, HFile, HFileScanner, HFileWriterV1, HFileWriterV2}
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

  def buildV2Writer(filename: String) = {
    val fs = new LocalFileSystem() 
    val path = new Path(new File(basepath, filename).toString)
    fs.initialize(URI.create("file:///"), conf)
    new HFileWriterV2(conf, cconf, fs, path, blockSize, compressionAlgo, null)
  }

  def buildV1Writer(filename: String) = {
    val fs = new LocalFileSystem() 
    val path = new Path(new File(basepath, filename).toString)
    fs.initialize(URI.create("file:///"), conf)
    new HFileWriterV1(conf, cconf, fs, path, blockSize, compressionAlgo, null)
  }
  
  def writeCollection[T <: AnyRef, K <: Any](
    filename: String,
    callback: (T) => (Array[Byte], Array[Byte]),
    dao: SalatDAO[T, K],
    sortField: String
  ) {
    val writer = buildV2Writer(filename)
    var fidCount = 0
    val fidSize = dao.collection.count
    val fidCursor = dao.find(MongoDBObject())
      .sort(orderBy = MongoDBObject(sortField -> 1)) // sort by _id desc
    fidCursor.foreach(f => {
      val (k, v) = callback(f)
      writer.append(k, v)
      fidCount += 1
      if (fidCount % 1000 == 0) {
        println("processed %d of %d %s".format(fidCount, fidSize, filename))
      }
    })
    writer.close()
  }

  val comp = new ByteArrayComparator()
  def lexicalSort(a: Array[Byte], b: Array[Byte]) = {
    comp.compare(a, b) < 0
  }

  def writeNames() {
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

    val writer = buildV2Writer("name_index.hfile")

    println("sorting")

    val sortedMap = nameMap.keys.toList.sort(lexicalSort)

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

  type IdFixer = (String) => Option[String]

  val factory = new TBinaryProtocol.Factory()

  def serializeBytes(g: GeocodeRecord, fixParentId: IdFixer) = {
    val serializer = new TSerializer(factory);
    val f = g.toGeocodeServingFeature()
    val parentOids = f.scoringFeatures.parents.flatMap(f => fixParentId(f))
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

    def fixParentId(fid: String) = fidMap.get(fid).map(_.toString)

    writeCollection("features.hfile",
      (g: GeocodeRecord) => 
        (g._id.toByteArray(), serializeBytes(g, fixParentId)),
      MongoGeocodeDAO, "_id")
  }

  val ThriftClassValueBytes: Array[Byte] = "value.thrift.class".getBytes("UTF-8")
  val ThriftClassKeyBytes: Array[Byte] = "key.thrift.class".getBytes("UTF-8")
  val ThriftEncodingKeyBytes: Array[Byte] = "thrift.protocol.factory.class".getBytes("UTF-8")

  def processForGeoId() {
    val geoCursor = MongoGeocodeDAO.find(MongoDBObject())

    def pickBestId(g: GeocodeRecord): String = {
      g.ids.find(_.startsWith("geonameid")).getOrElse(g.ids(0))
    }
    
    val gidMap = new HashMap[String, String]

    val ids = MongoGeocodeDAO.find(MongoDBObject()).map(pickBestId)
      .map(_.getBytes("UTF-8")).toList.sort(lexicalSort)

    geoCursor.foreach(g => {
      if (g.ids.size > 1) {
        val bestId = pickBestId(g)
        g.ids.foreach(id => {
          if (id != bestId) {
            gidMap(id) = bestId
          }
        })
      }
    })

    def fixParentId(fid: String) = Some(gidMap.getOrElse(fid, fid))

    val filename = "gid-features.hfile"
    val writer = buildV1Writer(filename)
    writer.appendFileInfo(ThriftClassValueBytes,
      classOf[GeocodeServingFeature].getName.getBytes("UTF-8"))
    writer.appendFileInfo(ThriftEncodingKeyBytes,
      factory.getClass.getName.getBytes("UTF-8"))

    var fidCount = 0
    val fidSize = ids.size
    ids.grouped(2000).foreach(chunk => {
      val records = MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> chunk)))
      records.foreach(g => {
        val (k, v) =
          (pickBestId(g).getBytes("UTF-8"), serializeBytes(g, fixParentId))
        writer.append(k, v)
        fidCount += 1
        if (fidCount % 1000 == 0) {
          println("processed %d of %d %s".format(fidCount, fidSize, filename))
        }
      })
    })
  }
}