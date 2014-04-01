package com.foursquare.twofishes

import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util.{GeometryUtils, StoredFeatureId}
import com.google.common.geometry.S2CellId
import com.mongodb.casbah.Imports._
import com.mongodb.Bytes
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.util.concurrent.CountDownLatch
import com.twitter.util.Duration
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import java.io._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.{TwofishesFoursquareCacheConfig, Compression, HFile}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.hadoop.io.{BytesWritable, MapFile}
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TCompactProtocol
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scalaj.collection.Implicits._
import com.weiglewilczek.slf4s.Logging
import akka.actor.ActorSystem
import akka.actor.Props
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

class WrappedByteMapWriter[K, V](writer: MapFile.Writer, index: Index[K, V]) {
  def append(k: K, v: V) {
    writer.append(new BytesWritable(index.keySerde.toBytes(k)), new BytesWritable(index.valueSerde.toBytes(v)))
  }

  def close() { writer.close() }
}

class WrappedHFileWriter[K, V](writer: HFile.Writer, index: Index[K, V]) {
  def append(k: K, v: V) {
    writer.append(index.keySerde.toBytes(k), index.valueSerde.toBytes(v))
  }

  def close() { writer.close() }
}

class FidMap(preload: Boolean) extends DurationUtils {
  val fidMap = new HashMap[StoredFeatureId, Option[StoredFeatureId]]

  if (preload) {
    logDuration("preloading fids") {
      var i = 0
      val total = MongoGeocodeDAO.collection.count()
      val geocodeCursor = MongoGeocodeDAO.find(MongoDBObject())
      geocodeCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
      geocodeCursor.foreach(geocodeRecord => {
        geocodeRecord.featureIds.foreach(id => {
          fidMap(id) = Some(geocodeRecord.featureId)
        })
        i += 1
        if (i % (100*1000) == 0) {
          logger.info("preloaded %d/%d fids".format(i, total))
        }
      })
    }
  }

  def get(fid: StoredFeatureId): Option[StoredFeatureId] = {
    if (preload) {
      fidMap.getOrElse(fid, None)
    } else {
      if (!fidMap.contains(fid)) {
        val longidOpt = MongoGeocodeDAO.primitiveProjection[Long](
          MongoDBObject("ids" -> fid.longId), "_id")
        fidMap(fid) = longidOpt.flatMap(StoredFeatureId.fromLong _)
        if (longidOpt.isEmpty) {
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
    fids.flatMap(fid => fidMap.get(fid)).toSet.toSeq
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
    val nameCursor = NameIndexDAO.find(
      MongoDBObject(
        "name" -> MongoDBObject("$regex" -> "^%s".format(prefix)))
    ).sort(orderBy = MongoDBObject("pop" -> -1)).limit(limit)
    nameCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
    nameCursor
  }

  def doOutputPrefixIndex(prefixSet: HashSet[String]) {
    logger.info("sorting prefix set")
    val sortedPrefixes = prefixSet.toList.sortWith(lexicalSort)
    logger.info("done sorting")

    val bestWoeTypes = List(
      YahooWoeType.POSTAL_CODE,
      YahooWoeType.TOWN,
      YahooWoeType.SUBURB,
      YahooWoeType.ADMIN3,
      YahooWoeType.AIRPORT,
      YahooWoeType.COUNTRY
    ).map(_.getValue)

    val prefixWriter = buildMapFileWriter(Indexes.PrefixIndex,
      Map(
        ("MAX_PREFIX_LENGTH", PrefixIndexer.MaxPrefixLength.toString)
      )
    )

    val numPrefixes = sortedPrefixes.size
    for {
      (prefix, index) <- sortedPrefixes.zipWithIndex
    } {
      if (index % 1000 == 0) {
        logger.info("done with %d of %d prefixes".format(index, numPrefixes))
      }
      val records = getRecordsByPrefix(prefix, 1000)

      val (woeMatches, woeMismatches) = records.partition(r =>
        bestWoeTypes.contains(r.woeType))

      val (prefSortedRecords, unprefSortedRecords) =
        sortRecordsByNames(woeMatches.toList)

      var fids = new HashSet[StoredFeatureId]
      prefSortedRecords.foreach(f => {
        if (fids.size < 50) {
          fids.add(f.fidAsFeatureId)
        }
      })

      if (fids.size < 3) {
        unprefSortedRecords.foreach(f => {
          if (fids.size < 50) {
            fids.add(f.fidAsFeatureId)
          }
        })
      }

      prefixWriter.append(prefix, fidsToCanonicalFids(fids.toList))
    }

    prefixWriter.close()
    logger.info("done")
  }
}

class NameIndexer(override val basepath: String, override val fidMap: FidMap, outputPrefixIndex: Boolean) extends Indexer {
  val prefixIndexer = new PrefixIndexer(basepath, fidMap)

  def writeNames() {
    var nameCount = 0
    val nameSize = NameIndexDAO.collection.count()
    val nameCursor = NameIndexDAO.find(MongoDBObject())
      .sort(orderBy = MongoDBObject("name" -> 1)) // sort by nameBytes asc
    nameCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    var prefixSet = new HashSet[String]

    var lastName = ""
    var nameFids = new HashSet[StoredFeatureId]

    val writer = buildHFileV1Writer(Indexes.NameIndex)

    def writeFidsForLastName() {
      writer.append(lastName, fidsToCanonicalFids(nameFids.toList))
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

      nameFids.add(n.fidAsFeatureId)

      nameCount += 1
      if (nameCount % 100000 == 0) {
        logger.info("processed %d of %d names".format(nameCount, nameSize))
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
  def canonicalizeParentId(fid: StoredFeatureId) = fidMap.get(fid)

  def makeGeocodeRecordWithoutGeometry(g: GeocodeRecord, poly: Option[PolygonIndex]): GeocodeServingFeature = {
    val fullFeature = poly.map(p => g.copy(polygon = Some(p.polygon)))
      .getOrElse(g).toGeocodeServingFeature()

    val partialFeature = fullFeature.copy(
      feature = fullFeature.feature.copy(
        geometry = fullFeature.feature.geometry.copy(wkbGeometry = null)
      )
    )

    makeGeocodeServingFeature(partialFeature)
  }

  def makeGeocodeRecord(g: GeocodeRecord) = {
    makeGeocodeServingFeature(g.toGeocodeServingFeature())
  }

  def makeGeocodeServingFeature(f: GeocodeServingFeature) = {
    val parents = for {
      parentLongId <- f.scoringFeatures.parentIds
      parentFid <- StoredFeatureId.fromLong(parentLongId)
      parentId <- canonicalizeParentId(parentFid)
    } yield {
      parentFid
    }

    f.copy(
      scoringFeatures = f.scoringFeatures.copy(parentIds = parents.map(_.longId))
    )
  }

  def writeFeatures() {
    val writer = buildMapFileWriter(Indexes.FeatureIndex, indexInterval = Some(2))
    var fidCount = 0
    val fidSize = MongoGeocodeDAO.collection.count()
    val fidCursor = MongoGeocodeDAO.find(MongoDBObject())
      .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc
    fidCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    for {
      gCursor <- fidCursor.grouped(1000)
      group = gCursor.toList
      toFindPolys: Map[Long, ObjectId] = group.filter(f => f.hasPoly).map(r => (r._id, r.polyId)).toMap
      polyMap: Map[ObjectId, PolygonIndex] = PolygonIndexDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> toFindPolys.values)))
        .toList
        .groupBy(_._id).map({case (k, v) => (k, v(0))})
      f <- group
      polyOpt = polyMap.get(f.polyId)
    } {
      writer.append(
        f.featureId, makeGeocodeRecordWithoutGeometry(f, polyOpt))
      fidCount += 1
      if (fidCount % 100000 == 0) {
        logger.info("processed %d of %d features".format(fidCount, fidSize))
      }
    }
    writer.close()
  }
}

class PolygonIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  def buildPolygonIndex() {
    val hasPolyCursor = 
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
        .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc
    hasPolyCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    val writer = buildMapFileWriter(Indexes.GeometryIndex)

    val wkbReader = new WKBReader()

    var index = 0
    // would be great to unify this with featuresIndex
    for {
      g <- hasPolyCursor.grouped(1000)
      group = g.toList
      toFindPolys: Map[Long, ObjectId] = group.filter(f => f.hasPoly).map(r => (r._id, r.polyId)).toMap
      polyMap: Map[ObjectId, PolygonIndex] = PolygonIndexDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> toFindPolys.values)))
        .toList
        .groupBy(_._id).map({case (k, v) => (k, v(0))})
      f <- group
      poly <- polyMap.get(f.polyId)
    } {
      if (index % 1000 == 0) {
        logger.info("outputted %d polys so far".format(index))
      }
      index += 1
      writer.append(StoredFeatureId.fromLong(f._id).get, wkbReader.read(poly.polygon))
    }
    writer.close()

    logger.info("done")
  }
}

class RevGeoIndexer(override val basepath: String, override val fidMap: FidMap) extends Indexer {
  val minS2Level = 8
  val maxS2Level = 12
  val maxCells = 10000
  val levelMod = 2

  def buildRevGeoIndex() {
    val writer = buildMapFileWriter(
      Indexes.S2Index,
      Map(
        "minS2Level" -> minS2Level.toString,
        "maxS2Level" -> maxS2Level.toString,
        "levelMod" -> levelMod.toString
      )
    )

    val hasPolyCursor = 
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
    hasPolyCursor.option = Bytes.QUERYOPTION_NOTIMEOUT
    val idMap: Map[ObjectId, (Long, YahooWoeType)] = hasPolyCursor.map(r => {
      (r.polyId, (r._id, r.woeType))
    }).toMap

    println("did all the s2 indexing")

    val revGeoCursor = RevGeoIndexDAO.find(MongoDBObject()).sort(orderBy = MongoDBObject("cellid" -> -1))
    revGeoCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    var currentKey = 0L
    var currentCells = new ListBuffer[CellGeometry]
    for {
      revgeoIndexRecord <- revGeoCursor
      (geoid, woeType) <- idMap.get(revgeoIndexRecord.polyId)
    } {
      if (currentKey != revgeoIndexRecord.cellid) {
        if (currentKey != 0L) {
          writer.append(currentKey, CellGeometries(currentCells))
        }
        currentKey = revgeoIndexRecord.cellid
        currentCells.clear
      }
      val builder = CellGeometry.newBuilder
        .woeType(woeType)
        .longId(geoid)

      if (revgeoIndexRecord.full) {
        builder.full(true)
      } else {
        builder.wkbGeometry(revgeoIndexRecord.geom.map(ByteBuffer.wrap))
      }
      currentCells.append(builder.result)
    }

    writer.append(currentKey, CellGeometries(currentCells))

    writer.close()
  }
}

class IdIndexer(override val basepath: String, override val fidMap: FidMap, slugEntryMap: SlugEntryMap) extends Indexer {
  def writeSlugsAndIds() {
    val slugEntries: List[(String, StoredFeatureId)] = for {
      (slug, entry) <- slugEntryMap.toList
      fid <- StoredFeatureId.fromHumanReadableString(entry.id)
      canonicalFid <- fidMap.get(fid)
    } yield {
      slug -> canonicalFid
    }

    val writer = buildMapFileWriter(Indexes.IdMappingIndex)

    val sortedEntries = slugEntries.sortWith((a, b) => lexicalSort(a._1, b._1)).foreach({case (k, v) => {
      writer.append(k, v)
    }})

    writer.close()
  }
}

class OutputIndexes(basepath: String, outputPrefixIndex: Boolean, slugEntryMap: SlugEntryMap, outputRevgeo: Boolean) {
  def buildIndexes(revgeoLatch: CountDownLatch) {
    val fidMap = new FidMap(preload = false)

    (new NameIndexer(basepath, fidMap, outputPrefixIndex)).writeNames()
    (new IdIndexer(basepath, fidMap, slugEntryMap)).writeSlugsAndIds()
    (new FeatureIndexer(basepath, fidMap)).writeFeatures()
    (new PolygonIndexer(basepath, fidMap)).buildPolygonIndex()
    if (outputRevgeo) {
      revgeoLatch.await()
      (new RevGeoIndexer(basepath, fidMap)).buildRevGeoIndex()
    }
  }
}
