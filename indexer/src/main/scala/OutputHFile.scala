package com.foursquare.twofishes

import com.foursquare.base.thrift.{LongWrapper, ObjectIdListWrapper, ObjectIdWrapper, StringWrapper}
import com.foursquare.batch.ShapefileSimplifier
import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util.{GeometryUtils, NameUtils}
import com.google.common.geometry.S2CellId
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import com.twitter.util.Duration
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import java.io._
import java.net.URI
import java.nio.ByteBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.io.{MapFile, BytesWritable}
import org.apache.hadoop.hbase.io.hfile.{Compression, HFile}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.{TCompactProtocol, TProtocolFactory}
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scalaj.collection.Implicits._

trait DurationUtils {
  def logDuration[T](what: String)(f: => T): T = {
    val (rv, duration) = Duration.inNanoseconds(f)
    if (duration.inMilliseconds > 200) {
      println(what + " in %s µs / %s ms".format(duration.inMicroseconds, duration.inMilliseconds))
    }
    rv
  }
}

class WrappedByteMapWriter(writer: MapFile.Writer) {
  def append(k: Array[Byte], v: Array[Byte]) {
    writer.append(new BytesWritable(k), new BytesWritable(v))
  }

  def close() { writer.close() }
}

class FidMap(preload: Boolean) extends DurationUtils {
  val fidMap = new HashMap[String, Option[ObjectId]]

  if (preload) {
    logDuration("preloading fids") {
      var i = 0
      val total = MongoGeocodeDAO.collection.count
      val geocodeCursor = MongoGeocodeDAO.find(MongoDBObject())
      geocodeCursor.foreach(geocodeRecord => {
        geocodeRecord.ids.foreach(id => {
          fidMap(id) = Some(geocodeRecord._id)
        })
        i += 1
        if (i % (100*1000) == 0) {
          println("preloaded %d/%d fids".format(i, total))
        }
      })
    }
  }

  def get(fid: String): Option[ObjectId] = {
    if (preload) {
      fidMap.getOrElse(fid, None)
    } else {
      if (!fidMap.contains(fid)) {
        val oidOpt = MongoGeocodeDAO.primitiveProjection[ObjectId](
          MongoDBObject("ids" -> fid), "_id")
        fidMap(fid) = oidOpt
        if (oidOpt.isEmpty) {
          //println("missing fid: %s".format(fid))
        }
      }

      fidMap.getOrElseUpdate(fid, None)
    }
  }
}

abstract class Indexer extends DurationUtils {
  def basepath: String
  def fidMap: FidMap

  val ThriftClassValue: String = "value.thrift.class"
  val ThriftClassKey: String = "key.thrift.class"
  val ThriftEncodingKey: String = "thrift.protocol.factory.class"

  val factory = new TCompactProtocol.Factory()
  val serializer = new TSerializer(factory)

  // this is all weirdly 4sq specific logic :-(
  def fixThriftClassName(n: String) = {
    if (n.contains("com.foursquare.twofishes.gen")) {
      n
    } else {
      n.replace("com.foursquare.twofishes", "com.foursquare.twofishes.gen")
        .replace("com.foursquare.base.thrift", "com.foursquare.base.gen")

    }
  }

  def buildHFileV1Writer(filename: String,
                         info: Map[String, String] = Map.empty): HFile.Writer = {
    val conf = new Configuration()
    val blockSizeKey = "hbase.mapreduce.hfileoutputformat.blocksize"
    val compressionKey = "hfile.compression"

    val blockSize = 16384
    val compressionAlgo = Compression.Algorithm.NONE.getName

    val fs = new LocalFileSystem()
    val path = new Path(new File(basepath, filename).toString)
    fs.initialize(URI.create("file:///"), conf)
    val hadoopConfiguration: Configuration = new Configuration()

    val compressionAlgorithm: Compression.Algorithm =
      Compression.getCompressionAlgorithmByName("none")

    val writer = HFile.getWriterFactory(hadoopConfiguration).createWriter(fs,
      path,
      blockSize, compressionAlgorithm,
      null)

    info.foreach({case (k, v) => writer.appendFileInfo(k.getBytes("UTF-8"), v.getBytes("UTF-8")) })

    writer
  }

  def buildMapFileWriter[K: Manifest, V: Manifest](
      filename: String,
      info: Map[String, String] = Map.empty) = {

    val keyClassName = fixThriftClassName(manifest[K].erasure.getName)
    val valueClassName = fixThriftClassName(manifest[V].erasure.getName)

    val finalInfoMap = info ++ Map(
      (ThriftClassKey, keyClassName),
      (ThriftClassValue, valueClassName),
      (ThriftEncodingKey, factory.getClass.getName)
    )

    new WrappedByteMapWriter(
      MapFileUtils.writerToLocalPath((new File(basepath, filename)).toString, finalInfoMap)
    )
  }

  val comp = new ByteArrayComparator()
  def byteBufferSort(a: ByteBuffer, b: ByteBuffer) = {
    comp.compare(a.array(), b.array()) < 0
  }
  def byteSort(a: Array[Byte], b: Array[Byte]) = {
    comp.compare(a, b) < 0
  }
  def bytePairSort(a: (Array[Byte], Array[Byte]),
      b: (Array[Byte], Array[Byte])) = {
    comp.compare(a._1, b._1) < 0
  }
  def lexicalSort(a: String, b: String) = {
    comp.compare(a.getBytes(), b.getBytes()) < 0
  }
  def objectIdSort(a: ObjectId, b: ObjectId) = {
    comp.compare(a.toByteArray(), b.toByteArray()) < 0
  }

  def fidStringsToByteArray(fids: List[String]): Array[Byte] = {
    val oids: Set[ObjectId] = fids.flatMap(fid => fidMap.get(fid)).toSet
    oidsToByteArray(oids)
  }

  def oidsToByteArray(oids: Iterable[ObjectId]): Array[Byte] = {
    val os = new ByteArrayOutputStream(12 * oids.size)
    oids.foreach(oid =>
      os.write(oid.toByteArray)
    )
    os.toByteArray()
  }
}

object PrefixIndexer {
  val MaxPrefixLength = 5
}

class PrefixIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  def hasFlag(record: NameIndex, flag: FeatureNameFlags) =
    (record.flags & flag.getValue) > 0

  def joinLists(lists: List[NameIndex]*): List[NameIndex] = {
    lists.toList.flatMap(l => {
      l.sortBy(_.pop * -1)
    })
  }

  def sortRecordsByNames(records: List[NameIndex]) = {
    // val (pureNames, unpureNames) = records.partition(r => {
    //   !hasFlag(r, FeatureNameFlags.ALIAS)
    //   !hasFlag(r, FeatureNameFlags.DEACCENT)
    // })

    val (prefPureNames, nonPrefPureNames) =
      records.partition(r =>
        (hasFlag(r, FeatureNameFlags.PREFERRED) || hasFlag(r, FeatureNameFlags.ALT_NAME)) &&
        (r.lang == "en" || hasFlag(r, FeatureNameFlags.LOCAL_LANG))
      )

    val (secondBestNames, worstNames) =
      nonPrefPureNames.partition(r =>
        r.lang == "en"
        || hasFlag(r, FeatureNameFlags.LOCAL_LANG)
      )

    (joinLists(prefPureNames), joinLists(secondBestNames, worstNames))
  }

  def getRecordsByPrefix(prefix: String, limit: Int) = {
    NameIndexDAO.find(
      MongoDBObject(
        "name" -> MongoDBObject("$regex" -> "^%s".format(prefix)))
    ).sort(orderBy = MongoDBObject("pop" -> -1)).limit(limit)
  }

  def doOutputPrefixIndex(prefixSet: HashSet[String]) {
    println("sorting prefix set")
    val sortedPrefixes = prefixSet.toList.sort(lexicalSort)
    println("done sorting")

    val bestWoeTypes = List(
      YahooWoeType.POSTAL_CODE,
      YahooWoeType.TOWN,
      YahooWoeType.SUBURB,
      YahooWoeType.ADMIN3,
      YahooWoeType.AIRPORT,
      YahooWoeType.COUNTRY
    ).map(_.getValue)

    val prefixWriter = buildMapFileWriter("prefix_index",
      Map(
        ("MAX_PREFIX_LENGTH", PrefixIndexer.MaxPrefixLength.toString)
      )
    )

    val numPrefixes = sortedPrefixes.size
    for {
      (prefix, index) <- sortedPrefixes.zipWithIndex
    } {
      if (index % 1000 == 0) {
        println("done with %d of %d prefixes".format(index, numPrefixes))
      }
      val records = getRecordsByPrefix(prefix, 1000)

      val (woeMatches, woeMismatches) = records.partition(r =>
        bestWoeTypes.contains(r.woeType))

      val (prefSortedRecords, unprefSortedRecords) =
        sortRecordsByNames(woeMatches.toList)

      var fids = new HashSet[String]
      prefSortedRecords.foreach(f => {
        if (fids.size < 50) {
          fids.add(f.fid)
        }
      })

      if (fids.size < 3) {
        unprefSortedRecords.foreach(f => {
          if (fids.size < 50) {
            fids.add(f.fid)
          }
        })
      }

      prefixWriter.append(prefix.getBytes(), fidStringsToByteArray(fids.toList))
    }

    prefixWriter.close()
    println("done")
  }
}

class NameIndexer(override val basepath: String, override val fidMap: FidMap, outputPrefixIndex: Boolean) extends Indexer {
  val prefixIndexer = new PrefixIndexer(basepath, fidMap)

  def writeNames() {
    var nameCount = 0
    val nameSize = NameIndexDAO.collection.count
    val nameCursor = NameIndexDAO.find(MongoDBObject())
      .sort(orderBy = MongoDBObject("name" -> 1)) // sort by nameBytes asc

    var prefixSet = new HashSet[String]

    var lastName = ""
    var nameFids = new HashSet[String]

    val writer = buildHFileV1Writer("name_index.hfile")

    def writeFidsForLastName() {
      writer.append(lastName.getBytes(), fidStringsToByteArray(nameFids.toList))
      if (outputPrefixIndex) {
        1.to(List(PrefixIndexer.MaxPrefixLength, lastName.size).min).foreach(length =>
          prefixSet.add(lastName.substring(0, length))
        )
      }
    }

    nameCursor.filterNot(_.name.isEmpty).foreach(n => {
      if (lastName != n.name) {
        if (lastName != "") {
          writeFidsForLastName()
        }
        nameFids.clear()
        lastName = n.name
      }

      nameFids.add(n.fid)

      nameCount += 1
      if (nameCount % 100000 == 0) {
        println("processed %d of %d names".format(nameCount, nameSize))
      }
    })
    writeFidsForLastName()
    writer.close()

    if (outputPrefixIndex) {
      prefixIndexer.doOutputPrefixIndex(prefixSet)
    }
  }
}

class FeatureIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  type IdFixer = (String) => Option[String]

  def serializeGeocodeRecordWithoutGeometry(g: GeocodeRecord, fixParentId: IdFixer) = {
    val f = g.toGeocodeServingFeature()
    f.feature.geometry.unsetWkbGeometry()
    serializeGeocodeServingFeature(f, fixParentId)
  }

  def serializeGeocodeRecord(g: GeocodeRecord, fixParentId: IdFixer) = {
    serializeGeocodeServingFeature(g.toGeocodeServingFeature(), fixParentId)
  }

  def serializeGeocodeServingFeature(f: GeocodeServingFeature, fixParentId: IdFixer) = {
    val parents = for {
      parent <- f.scoringFeatures.parents.asScala
      parentId <- fixParentId(parent)
    } yield {
      parentId
    }

    f.scoringFeatures.setParents(parents.asJava)
    serializer.serialize(f)
  }

  def writeFeatures() {
    def fixParentId(fid: String) = fidMap.get(fid).map(_.toString)

    // these types are a lie
    val writer = buildMapFileWriter[ObjectIdWrapper, GeocodeServingFeature]("features")
    var fidCount = 0
    val fidSize = MongoGeocodeDAO.collection.count
    val fidCursor = MongoGeocodeDAO.find(MongoDBObject())
      .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc
    fidCursor.foreach(f => {
      writer.append(
        f._id.toByteArray(), serializeGeocodeRecordWithoutGeometry(f, fixParentId))
      fidCount += 1
      if (fidCount % 100000 == 0) {
        println("processed %d of %d features".format(fidCount, fidSize))
      }
    })
    writer.close()
  }
}

class PolygonIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  def buildPolygonIndex() {
    val polygons =
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
        .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc

    var index: Int = 0
    // these types are a lie
    var writer = buildMapFileWriter[StringWrapper, GeocodeServingFeature]("geometry")

    for {
      (featureRecord, index) <- polygons.zipWithIndex
      polygon <- featureRecord.polygon
    } {
      if (index % 1000 == 0) {
        println("outputted %d polys so far".format(index))
      }
      writer.append(
        featureRecord._id.toByteArray(),
        polygon)
    }
    writer.close()

    println("done")
  }
}

class RevGeoIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  def buildRevGeoIndex() {
    val minS2Level = 8
    val maxS2Level = 12
    val maxCells = 1000
    val levelMod = 2

    val writer = buildMapFileWriter(
      "s2_index",
      Map(
        "minS2Level" -> minS2Level.toString,
        "maxS2Level" -> maxS2Level.toString,
        "levelMod" -> levelMod.toString
      )
    )

    val ids = MongoGeocodeDAO.primitiveProjections[ObjectId](MongoDBObject("hasPoly" -> true), "_id").toList
    val numThreads = 5
    val subMaps = 0.until(numThreads).toList.map(offset => {
      val s2map = new HashMap[Long, ListBuffer[CellGeometry]]
      val s2shapes = new HashMap[Long, Geometry]
      val thread = new Thread(new Runnable {
        val wkbReader = new WKBReader()
        val wkbWriter = new WKBWriter()

        def calculateCoverForRecord(record: GeocodeRecord) {
          for {
            polygon <- record.polygon
          } {
            //println("reading poly %s".format(index))
            val geom = wkbReader.read(polygon)

            val cells = logDuration("generated cover ") {
              GeometryUtils.s2PolygonCovering(
                geom,
                minS2Level,
                maxS2Level,
                levelMod = Some(levelMod),
                maxCellsHintWhichMightBeIgnored = Some(1000)
              )
            }
            logDuration("clipped and outputted cover for %d cells".format(cells.size)) {
              cells.foreach(
                (cellid: S2CellId) => {
                  val bucket = s2map.getOrElseUpdate(cellid.id, new ListBuffer[CellGeometry]())
                  val s2shape = s2shapes.getOrElseUpdate(cellid.id, ShapefileS2Util.fullGeometryForCell(cellid))
                  val cellGeometry = new CellGeometry()
                  val recordShape = geom.buffer(0)
                  if (recordShape.contains(s2shape)) {
                    cellGeometry.setFull(true)
                  } else {
                    cellGeometry.setWkbGeometry(wkbWriter.write(s2shape.intersection(recordShape)))
                  }
                  cellGeometry.setWoeType(record.woeType)
                  cellGeometry.setOid(record._id.toByteArray())
                  bucket += cellGeometry
                }
              )
            }
          }
        }

        def run() {
          println("thread: %d".format(offset))
          println("seeing %d ids".format(ids.size))
          println("filtering to %d ids on %d".format(ids.zipWithIndex.filter(i => (i._2 % numThreads) == offset).size, offset))

          var doneCount = 0

          ids.zipWithIndex.filter(i => (i._2 % numThreads) == offset).grouped(200).foreach(chunk => {
            val records = MongoGeocodeDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> chunk.map(_._1)))).toList
            records.foreach(calculateCoverForRecord)

            doneCount += chunk.size
            if (doneCount % 1000 == 0) {
              println("Thread %d finished %d of %d %.2f".format(offset, doneCount, ids.size, doneCount * 100.0 / ids.size))
            }
          })
        }
      })
      thread.start
      (s2map, thread)
    })

    // wait until everything finishes
    subMaps.foreach(_._2.join)

    val sortedMapKeys =
      subMaps.flatMap(_._1.keys)
             .toList.distinct.map(longCellId => GeometryUtils.getBytes(longCellId))
             .sorted(Ordering.comparatorToOrdering(comp))

    sortedMapKeys.foreach(k => {
      val longKey = GeometryUtils.getLongFromBytes(k)
      val cells: List[CellGeometry] = subMaps.flatMap(_._1.get(longKey)).flatMap(_.toList)
      val cellGeometries = new CellGeometries().setCells(cells.asJava)
      writer.append(k, serializer.serialize(cellGeometries))
    })

    scala.util.Random.shuffle(ids).take(100).foreach(id => println("new ObjectId(\"%s\")".format(id)))

    writer.close()
  }
}

class IdIndexer(override val basepath: String, override val fidMap: FidMap, slugEntryMap: SlugEntryMap) extends Indexer {
  def writeSlugsAndIds() {
    val slugEntries: List[(Array[Byte], Array[Byte])]  = for {
      (slug, entry) <- slugEntryMap.toList
      oid <- fidMap.get(entry.id)
    } yield {
      (slug.getBytes("UTF-8"), oid.toByteArray)
    }

    val oidEntries: List[(Array[Byte], Array[Byte])] = (for {
      geocodeRecord <- MongoGeocodeDAO.find(MongoDBObject())
      id <- geocodeRecord.ids
    } yield {
      (id.getBytes("UTF-8"), geocodeRecord._id.toByteArray)
    }).toList

    // these types are a lie
    val writer = buildMapFileWriter[StringWrapper, ObjectIdListWrapper]("id-mapping")

    val sortedEntries = (slugEntries ++ oidEntries).sortWith(bytePairSort).foreach({case (k, v) => {
      writer.append(k, v)
    }})

    writer.close()
  }
}

class OutputIndexes(basepath: String, outputPrefixIndex: Boolean, slugEntryMap: SlugEntryMap, outputRevgeo: Boolean) {
  def buildIndexes() {
    val fidMap = new FidMap(preload = true)

    (new NameIndexer(basepath, fidMap, outputPrefixIndex)).writeNames()
    (new IdIndexer(basepath, fidMap, slugEntryMap)).writeSlugsAndIds()
    (new FeatureIndexer(basepath, fidMap)).writeFeatures()
    (new PolygonIndexer(basepath, fidMap)).buildPolygonIndex()
    if (outputRevgeo) {
      (new RevGeoIndexer(basepath, fidMap)).buildRevGeoIndex()
    }
  }
}
