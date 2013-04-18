package com.foursquare.twofishes

import com.foursquare.twofishes.util.{ByteUtils, GeometryUtils, StoredFeatureId}
import com.twitter.ostrich.stats.Stats
import com.twitter.util.Duration
import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.{FoursquareCacheConfig, HFile, HFileScanner}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.hadoop.io.BytesWritable
import org.apache.thrift.{TBase, TBaseHelper, TDeserializer, TFieldIdEnum, TSerializer}
import org.apache.thrift.protocol.TCompactProtocol
import org.bson.types.ObjectId
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
      fid <- slugFidMap.flatMap(_.get(id))
    } yield { (fid, id) }).toMap

    getByFeatureIds(idMap.keys.toList).map({
      case (k, v) => (idMap(k), v)
    })
  }

  def getByS2CellId(id: Long): Seq[CellGeometry] = {
    s2map.get(id)
  }

  def getPolygonByFeatureId(id: StoredFeatureId): Option[Array[Byte]] = {
    geomMapOpt.flatMap(_.get(id))
  }

  def getPolygonByFeatureIds(ids: Seq[StoredFeatureId]): Map[StoredFeatureId, Array[Byte]] = {
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

class HFileInput[K, V](basepath: String, index: Index[K, V], shouldPreload: Boolean) {
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

sealed abstract class Serde[T] {
  def toBytes(t: T): ByteBuffer
  def fromBytes(bytes: ByteBuffer): T
}
object Serde {
  case object LongSerde extends Serde[Long] {
    override def toBytes(t: Long): ByteBuffer = ByteBuffer.wrap(ByteUtils.longToBytes(t))
    override def fromBytes(bytes: ByteBuffer): Long =
      ByteUtils.getLongFromBytes(TBaseHelper.byteBufferToByteArray(bytes))
  }

  case object StringSerde extends Serde[String] {
    override def toBytes(t: String): ByteBuffer = ByteBuffer.wrap(t.getBytes("UTF-8"))
    override def fromBytes(bytes: ByteBuffer): String = new String(TBaseHelper.byteBufferToByteArray(bytes))
  }

  case object ObjectIdSerde extends Serde[ObjectId] {
    override def toBytes(t: ObjectId): ByteBuffer = ByteBuffer.wrap(t.toByteArray)
    override def fromBytes(bytes: ByteBuffer): ObjectId = new ObjectId(TBaseHelper.byteBufferToByteArray(bytes))
  }

  case object ObjectIdListSerde extends Serde[Seq[ObjectId]] {
    override def toBytes(t: Seq[ObjectId]): ByteBuffer = {
      val buf = ByteBuffer.allocate(t.size * 12)
      t.foreach(oid => buf.put(oid.toByteArray))
      buf
    }
    override def fromBytes(bytes: ByteBuffer): Seq[ObjectId] = {
      val arr = new Array[Byte](12)
      0.until(bytes.remaining / 12).map(_ => {
        bytes.get(arr)
        new ObjectId(arr)
      })
    }
  }

  case object TrivialByteBufferSerde extends Serde[ByteBuffer] {
    override def toBytes(t: ByteBuffer): ByteBuffer = t
    override def fromBytes(bytes: ByteBuffer): ByteBuffer = bytes
  }

  case class ThriftSerde[T <: TBase[_ <: TBase[_ <: AnyRef, _ <: TFieldIdEnum], _ <: TFieldIdEnum]](
      factory: Unit => T) extends Serde[T] {
    val deserializer = new TDeserializer(new TCompactProtocol.Factory())
    val serializer = new TSerializer(new TCompactProtocol.Factory())

    def toBytes(t: T): ByteBuffer = {
      ByteBuffer.wrap(serializer.serialize(t))
    }
    def fromBytes(byteBuf: ByteBuffer): T = {
      val s = factory()
      val bytes = TBaseHelper.byteBufferToByteArray(byteBuf)
      deserializer.deserialize(s, bytes)
      s
    }
  }
}

sealed abstract class Index[K, V](val filename: String, val keySerde: Serde[K], val valueSerde: Serde[V])
object Indexes {
  type WKBGeometry = ByteBuffer

  case object GeometryIndex extends Index[ObjectId, WKBGeometry](
    "geometry", Serde.ObjectIdSerde, Serde.TrivialByteBufferSerde)

  case object FeatureIndex extends Index[ObjectId, GeocodeServingFeature](
    "features", Serde.ObjectIdSerde, Serde.ThriftSerde(Unit => new GeocodeServingFeature))

  case object IdMappingIndex extends Index[String, ObjectId](
    "id-mapping", Serde.StringSerde, Serde.ObjectIdSerde)

  case object S2Index extends Index[Long, CellGeometries](
    "s2_index", Serde.LongSerde, Serde.ThriftSerde(Unit => new CellGeometries))

  case object PrefixIndex extends Index[String, Seq[ObjectId]](
    "prefix_index", Serde.StringSerde, Serde.ObjectIdListSerde)

  case object NameIndex extends Index[String, Seq[ObjectId]](
    "name_index.hfile", Serde.StringSerde, Serde.ObjectIdListSerde)
}

class MapFileInput[K, V](basepath: String, index: Index[K, V], shouldPreload: Boolean) {
  val (reader, fileInfo) = {
    val (rv, duration) = Duration.inMilliseconds({
      MapFileUtils.readerAndInfoFromLocalPath(new File(basepath, index.filename).toString, shouldPreload)
    })
    println("took %s seconds to read %s".format(duration.inSeconds, index.filename))
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
  def decodeFeatureIds(bytes: Array[Byte]): Seq[StoredFeatureId] = {
    0.until(bytes.length / 12).flatMap(i => {
      StoredFeatureId.fromLegacyObjectId(new ObjectId(Arrays.copyOfRange(bytes, i * 12, (i + 1) * 12)))
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
  val nameIndex = new HFileInput(basepath, Indexes.NameIndex, shouldPreload)
  val prefixMapOpt = PrefixIndexMapFileInput.readInput(basepath, shouldPreload)

  def get(name: String): List[StoredFeatureId] = {
    val buf = ByteBuffer.wrap(name.getBytes())
    nameIndex.lookup(buf).toList.flatMap(b => {
      val bytes = TBaseHelper.byteBufferToByteArray(b)
      decodeFeatureIds(bytes)
    })
  }

  def getPrefix(name: String): Seq[StoredFeatureId] = {
    prefixMapOpt match {
      case Some(prefixMap) if (name.length <= prefixMap.maxPrefixLength) => {
        prefixMap.get(name)
      }
      case _  => {
        nameIndex.lookupPrefix(name).flatMap(bytes => {
          decodeFeatureIds(bytes)
        })
      }
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

class PrefixIndexMapFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val prefixIndex = new MapFileInput(basepath, Indexes.PrefixIndex, shouldPreload)
  val maxPrefixLength = prefixIndex.fileInfo.getOrElse(
    "MAX_PREFIX_LENGTH",
    throw new Exception("missing MAX_PREFIX_LENGTH")).toInt

  def get(name: String): List[StoredFeatureId] = {
    prefixIndex.lookup(name.getBytes).toList.flatMap(bytes => {
      decodeFeatureIds(bytes)
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
    s2Index.lookup(GeometryUtils.getBytes(cellid)).toList.flatMap(bytes => {
      val geometries = new CellGeometries()
      deserializeBytes(geometries, bytes)
      geometries.cells.asScala
    })
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

class GeometryMapFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val geometryIndex = new MapFileInput(basepath, Indexes.GeometryIndex, shouldPreload)

  def get(id: StoredFeatureId): Option[Array[Byte]] = {
    val buf = id.legacyObjectId.toByteArray()
    geometryIndex.lookup(buf)
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

class SlugFidMapFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val idMappingIndex = new MapFileInput(basepath, Indexes.IdMappingIndex, shouldPreload)

  def get(s: String): Option[StoredFeatureId] = {
    if (s.contains(":")) {
      StoredFeatureId.fromHumanReadableString(s)
    } else {
      val buf = s.getBytes("UTF-8")
      idMappingIndex.lookup(buf).flatMap(b => {
        decodeFeatureIds(b).headOption
      })
    }
  }
}

class GeocodeRecordMapFileInput(basepath: String, shouldPreload: Boolean) extends ByteReaderUtils {
  val featureIndex = new MapFileInput(basepath, Indexes.FeatureIndex, shouldPreload)

  def decodeFeature(bytes: Array[Byte]) = {
    deserializeBytes(new GeocodeServingFeature(), bytes)
  }

  def getByFeatureIds(oids: Seq[StoredFeatureId]): Map[StoredFeatureId, GeocodeServingFeature] = {
    (for {
      oid <- oids
      f <- get(oid)
    } yield {
      (oid, f)
    }).toMap
  }

  def get(id: StoredFeatureId): Option[GeocodeServingFeature] = {
    val buf = id.legacyObjectId.toByteArray()
    featureIndex.lookup(buf).map(decodeFeature)
  }
}
