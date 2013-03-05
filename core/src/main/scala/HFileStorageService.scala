package com.foursquare.twofishes

import com.foursquare.twofishes.util.GeometryUtils
import com.twitter.util.{Duration, FuturePool}
import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hbase.io.hfile.{BlockCache, CacheConfig, FoursquareCacheConfig, HFile, HFileScanner}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.thrift.{TBase, TBaseHelper, TDeserializer, TFieldIdEnum}
import org.apache.thrift.protocol.TCompactProtocol
import org.bson.types.ObjectId
import scala.collection.JavaConversions._
import scalaj.collection.Implicits._

class HFileStorageService(basepath: String, shouldPreload: Boolean) extends GeocodeStorageReadService {
  val nameMap = new NameIndexHFileInput(basepath, shouldPreload)
  val oidMap = new GeocodeRecordHFileInput(basepath, shouldPreload)
  val geomMapOpt = GeometryHFileInput.readInput(basepath, shouldPreload)
  val s2mapOpt = ReverseGeocodeMapFileInput.readInput(basepath, shouldPreload)
  val slugFidMap = new SlugFidHFileInput(basepath, shouldPreload)

  // will only be hit if we get a reverse geocode query
  lazy val s2map = s2mapOpt.getOrElse(
    throw new Exception("s2/revgeo index not built, please build s2_index.hfile"))

  def getIdsByNamePrefix(name: String): Seq[ObjectId] = {
    nameMap.getPrefix(name)
  }

  def getIdsByName(name: String): Seq[ObjectId] = {
    nameMap.get(name)
  }

  def getByName(name: String): Seq[GeocodeServingFeature] = {
    getByObjectIds(nameMap.get(name)).map(_._2).toSeq
  }

  def getByObjectIds(oids: Seq[ObjectId]): Map[ObjectId, GeocodeServingFeature] = {
    oidMap.getByObjectIds(oids)
  }

  def getBySlugOrFeatureIds(ids: Seq[String]) = {
    val oidMap = (for {
      id <- ids
      oid <- slugFidMap.get(id)
    } yield { (oid, id) }).toMap

    getByObjectIds(oidMap.keys.toList).map({
      case (k, v) => (oidMap(k), v)
    })
  }

  def getByS2CellId(id: Long): Seq[CellGeometry] = {
    s2map.get(id)
  }

  def getPolygonByObjectId(id: ObjectId): Option[Array[Byte]] = {
    geomMapOpt.flatMap(_.get(id))
  }

  def getMinS2Level: Int = s2map.minS2Level
  def getMaxS2Level: Int = s2map.maxS2Level
  override  def getLevelMod: Int = s2map.levelMod
}

class HFileInput(basepath: String, filename: String, shouldPreload: Boolean) {
  val conf = new Configuration()
  val fs = new LocalFileSystem()
  fs.initialize(URI.create("file:///"), conf)

  val path = new Path(new File(basepath, filename).getAbsolutePath())
  val cache = new FoursquareCacheConfig()

  val reader = HFile.createReader(path.getFileSystem(conf), path, cache)

  val fileInfo = reader.loadFileInfo().asScala

  // prefetch the hfile
  if (shouldPreload) {
    val (rv, duration) = Duration.inMilliseconds({
      val scanner = reader.getScanner(true, false) // Seek, caching.
      scanner.seekTo()
      while(scanner.next()) {}
    })

    println("took %s seconds to read %s".format(duration.inSeconds, filename))
  }

  def lookup(key: ByteBuffer): Option[ByteBuffer] = {
    val scanner: HFileScanner = reader.getScanner(true, true)
    if (scanner.reseekTo(key.array, key.position, key.remaining) == 0) {
      Some(scanner.getValue.duplicate())
    } else {
      None
    }
  }

  import scala.collection.mutable.ListBuffer

  def lookupPrefix(key: String, minPrefixRatio: Double = 0.5): Seq[Array[Byte]] = {
    val scanner: HFileScanner = reader.getScanner(true, true)
    scanner.seekTo(key.getBytes())
    if (!new String(scanner.getKeyValue().getKey()).startsWith(key)) {
      scanner.next()
    }


    val ret: ListBuffer[Array[Byte]] = new ListBuffer()

    // I hate to encode this logic here, but I don't really want to thread it
    // all the way through the storage logic.
    while (new String(scanner.getKeyValue().getKey()).startsWith(key)) {
      if ((key.size >= 3) ||
          (key.size*1.0 / new String(scanner.getKeyValue().getKey()).size) >= minPrefixRatio) {
        ret.append(scanner.getKeyValue().getValue())
      }
      scanner.next()
    }

    ret
  }
}

class MapFileInput(basepath: String, dirname: String, shouldPreload: Boolean) {
  val (reader, fileInfo) = {
    val (rv, duration) = Duration.inMilliseconds({
      MapFileUtils.readerAndInfoFromLocalPath(new File(basepath, dirname).toString, shouldPreload)
    })
    println("took %s seconds to read %s".format(duration.inSeconds, dirname))
    rv
  }

  def lookup(key: Array[Byte]): Option[Array[Byte]] = {
    val value = new BytesWritable
    if (reader.get(new BytesWritable(key), value) != null) {
      Some(value.getBytes)
    } else {
      None
    }
  }
}

trait ByteReaderUtils {
  def decodeObjectIds(bytes: Array[Byte]): Seq[ObjectId] = {
    0.until(bytes.length / 12).map(i => {
      new ObjectId(Arrays.copyOfRange(bytes, i * 12, (i + 1) * 12))
    })
  }

  def deserializeBytes[T <: TBase[_ <: TBase[_ <: AnyRef, _ <: TFieldIdEnum], _ <: TFieldIdEnum]](
      s: T, bytes: Array[Byte]): T = {
    val deserializer = new TDeserializer(new TCompactProtocol.Factory());
    deserializer.deserialize(s, bytes);
    s
  }
}

class NameIndexHFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val nameIndex = new HFileInput(basepath, "name_index.hfile", shouldPreload)
  val prefixMapOpt = PrefixIndexHFileInput.readInput(basepath, shouldPreload)

  def get(name: String): List[ObjectId] = {
    val buf = ByteBuffer.wrap(name.getBytes())
    nameIndex.lookup(buf).toList.flatMap(b => {
      val bytes = TBaseHelper.byteBufferToByteArray(b)
      decodeObjectIds(bytes)
    })
  }

  def getPrefix(name: String): Seq[ObjectId] = {
    prefixMapOpt match {
      case Some(prefixMap) if (name.length <= prefixMap.maxPrefixLength) => {
        prefixMap.get(name)
      }
      case _  => {
        nameIndex.lookupPrefix(name).flatMap(bytes => {
          decodeObjectIds(bytes)
        })
      }
    }
  }
}

object PrefixIndexHFileInput {
  def readInput(basepath: String, shouldPreload: Boolean) = {
    if (new File(basepath, "prefix_index.hfile").exists()) {
      Some(new PrefixIndexHFileInput(basepath, shouldPreload))
    } else {
      None
    }
  }
}

class PrefixIndexHFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val prefixIndex = new HFileInput(basepath, "prefix_index.hfile", shouldPreload)
  val maxPrefixLength = 5 // TODO: pull from hfile metadata

  def get(name: String): List[ObjectId] = {
    val buf = ByteBuffer.wrap(name.getBytes())
    prefixIndex.lookup(buf).toList.flatMap(b => {
      val bytes = TBaseHelper.byteBufferToByteArray(b)
      decodeObjectIds(bytes)
    })
  }
}

object ReverseGeocodeMapFileInput {
  def readInput(basepath: String, shouldPreload: Boolean) = {
    if (new File(basepath, "s2_index").exists()) {
      Some(new ReverseGeocodeMapFileInput(basepath, shouldPreload))
    } else {
      None
    }
  }
}

class ReverseGeocodeMapFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val s2Index = new MapFileInput(basepath, "s2_index", shouldPreload)

  lazy val minS2Level = s2Index.fileInfo.getOrElse(
    "minS2Level",
    throw new Exception("missing minS2Level")).toInt

  lazy val maxS2Level = s2Index.fileInfo.getOrElse(
    "maxS2Level",
    throw new Exception("missing maxS2Level")).toInt

  lazy val levelMod = s2Index.fileInfo.getOrElse(
    "levelMod",
    throw new Exception("missing levelMod")).toInt

  def get(cellid: Long): List[CellGeometry] = {
    s2Index.lookup(GeometryUtils.getBytes(cellid)).toList.flatMap(bytes => {
      val geometries = new CellGeometries()
      deserializeBytes(geometries, bytes)
      geometries.cells
    })
  }
}

object GeometryHFileInput {
  def readInput(basepath: String, shouldPreload: Boolean) = {
    if (new File(basepath, "geometry.hfile").exists()) {
      Some(new GeometryHFileInput(basepath, shouldPreload))
    } else {
      None
    }
  }
}

class GeometryHFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val geometryIndex = new HFileInput(basepath, "geometry.hfile", shouldPreload)

  def get(oid: ObjectId): Option[Array[Byte]] = {
    val buf = ByteBuffer.wrap(oid.toByteArray())
    geometryIndex.lookup(buf).map(b => {
      TBaseHelper.byteBufferToByteArray(b)
    })
  }
}

class SlugFidHFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val idMappingIndex = new HFileInput(basepath, "id-mapping.hfile", shouldPreload)
  def get(s: String): Option[ObjectId] = {
    val buf = ByteBuffer.wrap(s.getBytes("UTF-8"))
    idMappingIndex.lookup(buf).flatMap(b => {
      val bytes = TBaseHelper.byteBufferToByteArray(b)
      decodeObjectIds(bytes).headOption
    })
  }
}

class GeocodeRecordHFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val featureIndex = new HFileInput(basepath, "features.hfile", shouldPreload)

  def decodeFeature(b: ByteBuffer) = {
    val bytes = TBaseHelper.byteBufferToByteArray(b)
    deserializeBytes(new GeocodeServingFeature(), bytes)
  }

  def getByObjectIds(oids: Seq[ObjectId]): Map[ObjectId, GeocodeServingFeature] = {
    val comp = new ByteArrayComparator()
    val sortedOids = oids.map(oid => (oid.toByteArray(), oid)).toList.sortWith((a, b) => {
      comp.compare(a._1, b._1) < 0
    })

    val scanner: HFileScanner = featureIndex.reader.getScanner(true, true)
    def find(b: Array[Byte]) = {
      val key = ByteBuffer.wrap(b)
      if (scanner.seekTo(key.array, key.position, key.remaining) == 0) {
        Some(scanner.getValue.duplicate())
      } else {
        None
      }
    }

    sortedOids.flatMap({case (oidBytes, oid) => find(oidBytes).map(f => (oid, decodeFeature(f)))}).toMap
  }

  def get(oid: ObjectId): Option[GeocodeServingFeature] = {
    val buf = ByteBuffer.wrap(oid.toByteArray())
    featureIndex.lookup(buf).map(decodeFeature)
  }
}
