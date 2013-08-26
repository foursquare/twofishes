package com.foursquare.twofishes

import com.foursquare.twofishes.util.{ByteUtils, GeometryUtils, StoredFeatureId}
import com.twitter.ostrich.stats.Stats
import com.twitter.util.Duration
import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.util.Arrays
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.{FoursquareCacheConfig, HFile, HFileScanner}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.hadoop.io.BytesWritable
import org.apache.thrift.{TBase, TBaseHelper, TDeserializer, TFieldIdEnum, TSerializer}
import org.apache.thrift.protocol.TCompactProtocol
import org.bson.types.ObjectId
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBReader
import scalaj.collection.Implicits._

class HFileStorageService(basepath: String, shouldPreload: Boolean) extends GeocodeStorageReadService {
  val nameMap = new NameIndexHFileInput(basepath, shouldPreload)
  val oidMap = new GeocodeRecordMapFileInput(basepath, shouldPreload)
  val geomMapOpt = GeometryMapFileInput.readInput(basepath, shouldPreload)
  val s2mapOpt = ReverseGeocodeMapFileInput.readInput(basepath, shouldPreload)
  val slugFidMap = SlugFidMapFileInput.readInput(basepath, shouldPreload)

  val infoFile = new File(basepath, "upload-info")
  if (infoFile.exists) {
    scala.io.Source.fromFile(infoFile).getLines.foreach(line => {
      val parts = line.split(": ")
      if (parts.size != 2) {
        println("badly formatted info line: " + line)
      }
      for {
        key <- parts.lift(0)
        value <- parts.lift(1)
      } {
        Stats.setLabel(key, value)
      }
    })
  }

  // will only be hit if we get a reverse geocode query
  lazy val s2map = s2mapOpt.getOrElse(
    throw new Exception("s2/revgeo index not built, please build s2_index"))

  def getIdsByNamePrefix(name: String): Seq[StoredFeatureId] = {
    nameMap.getPrefix(name)
  }

  def getIdsByName(name: String): Seq[StoredFeatureId] = {
    nameMap.get(name)
  }

  def getByName(name: String): Seq[GeocodeServingFeature] = {
    getByFeatureIds(nameMap.get(name)).map(_._2).toSeq
  }

  def getByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, GeocodeServingFeature] = {
    oidMap.getByFeatureIds(ids)
  }

  def getBySlugOrFeatureIds(ids: Seq[String]) = {
    val idMap = (for {
      id <- ids
      fid <- StoredFeatureId.fromUserInputString(id).orElse(slugFidMap.flatMap(_.get(id)))
    } yield { (fid, id) }).toMap

    getByFeatureIds(idMap.keys.toList).map({
      case (k, v) => (idMap(k), v)
    })
  }

  def getByS2CellId(id: Long): Seq[CellGeometry] = {
    s2map.get(id)
  }

  def getPolygonByFeatureId(id: StoredFeatureId): Option[Geometry] = {
    geomMapOpt.flatMap(_.get(id))
  }

  def getPolygonByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Geometry] = {
    (for {
      id <- ids
      polygon <- getPolygonByFeatureId(id)
    } yield {
      (id -> polygon)
    }).toMap
  }

  def getMinS2Level: Int = s2map.minS2Level
  def getMaxS2Level: Int = s2map.maxS2Level
  override def getLevelMod: Int = s2map.levelMod

  override val hotfixesDeletes: Seq[StoredFeatureId] = {
    val file = new File(basepath, "hotfixes_deletes.txt")
    if (file.exists()) {
      scala.io.Source.fromFile(file).getLines.toList.flatMap(i => StoredFeatureId.fromLegacyObjectId(new ObjectId(i)))
    } else {
      Nil
    }
  }

  override val hotfixesBoosts: Map[StoredFeatureId, Int] = {
    val file = new File(basepath, "hotfixes_boosts.txt")
    if (file.exists()) {
      scala.io.Source.fromFile(file).getLines.toList.map(l => {
        val parts = l.split("[\\|\t, ]")
        try {
          (StoredFeatureId.fromLegacyObjectId(new ObjectId(parts(0))).get, parts(1).toInt)
        } catch {
          case _ => throw new Exception("malformed boost line: %s --> %s".format(l, parts.toList))
        }
      }).toMap
    } else {
      Map.empty
    }
  }
}

class HFileInput[V](basepath: String, index: Index[String, V], shouldPreload: Boolean) {
  val conf = new Configuration()
  val fs = new LocalFileSystem()
  fs.initialize(URI.create("file:///"), conf)

  val path = new Path(new File(basepath, index.filename).getAbsolutePath())
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

    println("took %s seconds to read %s".format(duration.inSeconds, index.filename))
  }

  def lookup(keyStr: String): Option[V] = {
    val key = ByteBuffer.wrap(keyStr.getBytes)
    val scanner: HFileScanner = reader.getScanner(true, true)
    if (scanner.reseekTo(key.array, key.position, key.remaining) == 0) {
      Some(index.valueSerde.fromBytes(TBaseHelper.byteBufferToByteArray(scanner.getValue.duplicate())))
    } else {
      None
    }
  }

  import scala.collection.mutable.ListBuffer

  def lookupPrefix(key: String, minPrefixRatio: Double = 0.5): Seq[V] = {
    val scanner: HFileScanner = reader.getScanner(true, true)
    scanner.seekTo(key.getBytes)
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

    ret.map(index.valueSerde.fromBytes _)
  }
}

class MapFileInput[K, V](basepath: String, index: Index[K, V], shouldPreload: Boolean) {
  val (reader, fileInfo) = {
    val (rv, duration) = Duration.inMilliseconds({
      MapFileUtils.readerAndInfoFromLocalPath(new File(basepath, index.filename).toString, shouldPreload)
    })
    println("took %s seconds to read %s".format(duration.inSeconds, index.filename))
    rv
  }

  def lookup(key: K): Option[V] = {
    val valueBytes = new BytesWritable
    val (rv, duration) = Duration.inMilliseconds {
      val keyBytes = index.keySerde.toBytes(key)
      if (reader.get(new BytesWritable(keyBytes), valueBytes) != null) {
        Some(index.valueSerde.fromBytes(valueBytes.getBytes))
      } else {
        None
      }
    }

    Stats.addMetric("mapfile-%s-lookup_msec".format(index.filename), duration.inMilliseconds.toInt)
    // This might just end up logging GC pauses, but it's possible we have
    // degenerate keys/values as well.
    if (duration.inMilliseconds > 100) {
      println("reading key '%s' from index '%s' took %s milliseconds. valueOpt is %s bytes long".format(
        key.toString, index.filename, duration.inMilliseconds, rv.map(_ => valueBytes.getLength)))
    }

    rv
  }
}

class NameIndexHFileInput(basepath: String, shouldPreload: Boolean) {
  val nameIndex = new HFileInput(basepath, Indexes.NameIndex, shouldPreload)
  val prefixMapOpt = PrefixIndexMapFileInput.readInput(basepath, shouldPreload)

  def get(name: String): List[StoredFeatureId] = {
    nameIndex.lookup(name).toList.flatten
  }

  def getPrefix(name: String): Seq[StoredFeatureId] = {
    prefixMapOpt match {
      case Some(prefixMap) if (name.length <= prefixMap.maxPrefixLength) =>
        prefixMap.get(name)
      case _  =>
        nameIndex.lookupPrefix(name).flatten
    }
  }
}


object PrefixIndexMapFileInput {
  def readInput(basepath: String, shouldPreload: Boolean) = {
    if (new File(basepath, "prefix_index").exists()) {
      Some(new PrefixIndexMapFileInput(basepath, shouldPreload))
    } else {
      None
    }
  }
}

class PrefixIndexMapFileInput(basepath: String, shouldPreload: Boolean) {
  val prefixIndex = new MapFileInput(basepath, Indexes.PrefixIndex, shouldPreload)
  val maxPrefixLength = prefixIndex.fileInfo.getOrElse(
    "MAX_PREFIX_LENGTH",
    throw new Exception("missing MAX_PREFIX_LENGTH")).toInt

  def get(name: String): List[StoredFeatureId] = {
    prefixIndex.lookup(name).toList.flatten
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


class ReverseGeocodeMapFileInput(basepath: String, shouldPreload: Boolean) {
  val s2Index = new MapFileInput(basepath, Indexes.S2Index, shouldPreload)

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
    s2Index.lookup(cellid).toList.flatMap(_.cells.asScala)
  }
}

object GeometryMapFileInput {
  def readInput(basepath: String, shouldPreload: Boolean) = {
    if (new File(basepath, "geometry").exists()) {
      Some(new GeometryMapFileInput(basepath, shouldPreload))
    } else {
      None
    }
  }
}

class GeometryMapFileInput(basepath: String, shouldPreload: Boolean) {
  val geometryIndex = new MapFileInput(basepath, Indexes.GeometryIndex, shouldPreload)

  def get(id: StoredFeatureId): Option[Geometry] = {
    geometryIndex.lookup(id)
  }
}

object SlugFidMapFileInput {
  def readInput(basepath: String, shouldPreload: Boolean) = {
    if (new File(basepath, "id-mapping").exists()) {
      Some(new SlugFidMapFileInput(basepath, shouldPreload))
    } else {
      None
    }
  }
}

class SlugFidMapFileInput(basepath: String, shouldPreload: Boolean) {
  val idMappingIndex = new MapFileInput(basepath, Indexes.IdMappingIndex, shouldPreload)

  def get(s: String): Option[StoredFeatureId] = {
    if (s.contains(":")) {
      StoredFeatureId.fromHumanReadableString(s)
    } else {
      idMappingIndex.lookup(s)
    }
  }
}

class GeocodeRecordMapFileInput(basepath: String, shouldPreload: Boolean) {
  // This index is a contention point, and as a (very) short-term solution,
  // this thread-local will allow us to have 4 copies of the index in-memory
  // (and 4 locks).  Since we use mmap() behind the scenes, we'll only pay the
  // cost of loading the index 4 times onto the heap, not the data. The real
  // fix for this is to either make MapFile less contend-y and to use Future's.
  val featureIndexes = (0 to 3).map(_ => new MapFileInput(basepath, Indexes.FeatureIndex, shouldPreload))
  val featureIndexQueue = new LinkedBlockingQueue(featureIndexes.asJava)

  def getByFeatureIds(oids: Seq[StoredFeatureId]): Map[StoredFeatureId, GeocodeServingFeature] = {
    (for {
      oid <- oids
      f <- get(oid)
    } yield {
      (oid, f)
    }).toMap
  }

  def doWithIndex[T](f: MapFileInput[StoredFeatureId, GeocodeServingFeature] => T): T = {
    var index: MapFileInput[StoredFeatureId, GeocodeServingFeature] = null
    try {
      index = Stats.time("obtain-features-index") { featureIndexQueue.take() }
      f(index)
    } finally {
      featureIndexQueue.put(index)
    }
  }

  def get(id: StoredFeatureId): Option[GeocodeServingFeature] = {
    doWithIndex(_.lookup(id))
  }
}
