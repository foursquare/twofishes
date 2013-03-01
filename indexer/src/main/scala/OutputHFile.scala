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
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import java.io._
import java.net.URI
import java.nio.ByteBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.io.hfile.{Compression, HFile}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.{TCompactProtocol, TProtocolFactory}
import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet}
import scalaj.collection.Implicits._

object HFileUtil {
  val ThriftClassValueBytes: Array[Byte] = "value.thrift.class".getBytes("UTF-8")
  val ThriftClassKeyBytes: Array[Byte] = "key.thrift.class".getBytes("UTF-8")
  val ThriftEncodingKeyBytes: Array[Byte] = "thrift.protocol.factory.class".getBytes("UTF-8")
}

class OutputHFile(basepath: String, outputPrefixIndex: Boolean, slugEntryMap: SlugEntryMap) {
  val blockSizeKey = "hbase.mapreduce.hfileoutputformat.blocksize"
  val compressionKey = "hfile.compression"

  // val blockSize = HFile.DEFAULT_BLOCKSIZE
  val blockSize = 16384
  val compressionAlgo = Compression.Algorithm.NONE.getName

  val conf = new Configuration()
  
  val maxPrefixLength = 5

  def hasFlag(record: NameIndex, flag: FeatureNameFlags) =
    (record.flags & flag.getValue) > 0

  def joinLists(lists: List[NameIndex]*): List[NameIndex] = {
    lists.toList.flatMap(l => {
      l.sortBy(_.pop * -1)
    })
  }

  type IdFixer = (String) => Option[String]

  val factory = new TCompactProtocol.Factory()
  val serializer = new TSerializer(factory)

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
      parent <- f.scoringFeatures.parents
      parentId <- fixParentId(parent)
    } yield {
      parentId
    }

    f.scoringFeatures.setParents(parents)
    serializer.serialize(f)
  }

  // please clean me up, sorry
  def buildStandaloneRevGeoIndex() {
    val wkbReader = new WKBReader()

    val records = 
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
    val ids = new HashSet[ObjectId]
    val total = MongoGeocodeDAO.count(MongoDBObject("hasPoly" -> true))

    val s2map = new HashMap[Long, HashSet[ObjectId]]

    val minS2Level = 9
    val maxS2Level = 13

    var index = 0
    for {
      record <- records
      polygon <- record.polygon
    } {
      val geom = wkbReader.read(polygon)

      GeometryUtils.s2PolygonCovering(geom, minS2Level, maxS2Level).foreach(
        (cellid: S2CellId) => {
          val bucket = s2map.getOrElseUpdate(cellid.id, new HashSet[ObjectId]())
          bucket.add(record._id)
        } 
      )
      if (index % 1000 == 0) {
        println("computed cover for %d of %d (%.2f%%) polys".format(index, total, index*100.0/total))
      }
      index += 1
      ids.add(record._id)
    }

    println("sorting s2 map of %s keys".format(s2map.size))
    val listWrapper = new ObjectIdListWrapper
    val listMap: List[(Array[Byte], Array[Byte])] = s2map.toList.map({case (k, v) => {
      (
        serializer.serialize(new LongWrapper().setValue(k)),
        serializer.serialize(listWrapper.setValues(s2map(k).toList.map(v => ByteBuffer.wrap(v.toByteArray()))))
      )
    }})

    print("serializing s2map")

    val writer = buildV1Writer[LongWrapper, ObjectIdListWrapper](
      new File(basepath, "standalone_s2_geo_index.hfile").toString, factory)
    listMap.toList.sortWith(bytePairSort).foreach({case (k, v) => {
      writer.append(k, v)
    }})

    writer.appendFileInfo("minS2Level".getBytes("UTF-8"), GeometryUtils.getBytes(minS2Level))
    writer.appendFileInfo("maxS2Level".getBytes("UTF-8"), GeometryUtils.getBytes(maxS2Level))
    writer.close()

    println("outputting feature map")
    def nullFixer(s: String) = Some(s)
    writeGeocodeRecords("standalone_s2_feature_index.hfile", ids, nullFixer)
  }

  // please clean me up, sorry
  def buildStandaloneRevGeoShapeIndex() {
    val wkbReader = new WKBReader()

    val tmpFileName = "polygons-full.shp"
    BuildPolygonShapefile.buildAndWriteCollection(tmpFileName)
    ShapefileSimplifier.doSimplification(
      new File("polygons-full.shp"),
      new File("polygons-simplified.shp"),
      "id",
      ShapefileSimplifier.defaultLevels,
      None,
      None
    )
  }

  def buildPolygonIndex() {
    val polygons = 
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
        .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc

    var index: Int = 0
    // these types are a lie
    var writer = buildV1Writer[StringWrapper, GeocodeServingFeature]("geometry.hfile", factory)

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

  def logDuration[T](what: String)(f: => T): T = {
    val (rv, duration) = Duration.inNanoseconds(f)
    if (duration.inMilliseconds > 200) {
      println(what + " in %s µs / %s ms".format(duration.inMicroseconds, duration.inMilliseconds))
    }
    rv
  }

  def buildRevGeoIndex() { 
    val minS2Level = 8
    val maxS2Level = 12
    val maxCells = 1000
    val levelMod = 2

    val ids = MongoGeocodeDAO.primitiveProjections[ObjectId](MongoDBObject("hasPoly" -> true), "_id").toList
    val numThreads = 5
    val subMaps = 0.until(numThreads).toList.map(offset => {
      val s2map = new HashMap[ByteBuffer, HashSet[CellGeometry]]    
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
                  val s2Bytes: Array[Byte] = GeometryUtils.getBytes(cellid)
                  val bucket = s2map.getOrElseUpdate(ByteBuffer.wrap(s2Bytes), new HashSet[CellGeometry]())
                  val cellGeometry = new CellGeometry()
                  val (clippedGeom, isContained) = ShapefileS2Util.clipGeometryToCell(geom.buffer(0), cellid)
                  if (isContained) {
                    cellGeometry.setFull(true)
                  } else {
                    cellGeometry.setWkbGeometry(wkbWriter.write(clippedGeom))
                  }
                  cellGeometry.setWoeType(record.woeType)
                  cellGeometry.setOid(record._id.toByteArray())
                  bucket.add(cellGeometry)
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

    val sortedMapKeys = subMaps.flatMap(_._1.keys).toList.distinct.sort(byteBufferSort)
    val writer = buildBasicV1Writer("s2_index.hfile", factory)
    sortedMapKeys.foreach(k => {
      val cells = subMaps.flatMap(_._1.get(k).map(_.toList).getOrElse(Nil))
      val cellGeometries = new CellGeometries().setCells(cells)
      writer.append(k.array(), serializer.serialize(cellGeometries))
    })

    writer.appendFileInfo("minS2Level".getBytes("UTF-8"), GeometryUtils.getBytes(minS2Level))
    writer.appendFileInfo("maxS2Level".getBytes("UTF-8"), GeometryUtils.getBytes(maxS2Level))
    writer.appendFileInfo("levelMod".getBytes("UTF-8"), GeometryUtils.getBytes(levelMod))
    writer.close()

    buildPolygonIndex()
  }

  def buildPolygonFeatureIndex(groupSize: Int, includeParents: Boolean) {
    class ParentMap {
      val fidMap = new HashMap[String, Option[GeocodeFeature]]

      def get(fid: String): Option[GeocodeFeature] = {
        if (!fidMap.contains(fid)) {
          val featureOpt = MongoGeocodeDAO.find(MongoDBObject("ids" -> fid)).toList.headOption
          fidMap(fid) = featureOpt.map(f => {
            val feature = f.toGeocodeServingFeature.feature
            feature.geometry.unsetWkbGeometry()
            feature
          })
        }

        fidMap.getOrElseUpdate(fid, None)
      }
    }

    val parentMap = new ParentMap()

    def findAndFixParents(servingFeature: GeocodeServingFeature) {
      servingFeature.setParents(
        servingFeature.scoringFeatures.parents.flatMap(p => parentMap.get(p))
      )
    }

    val polygons = 
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
        .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc

    var index: Int = 0
    var writer = buildV1Writer[StringWrapper, GeocodeServingFeature]("polygon-features-%d.hfile".format(index / groupSize), factory)

    polygons.foreach(p => {
      if (index % groupSize == 0) {
        writer.close()
        writer = buildV1Writer[StringWrapper, GeocodeServingFeature]("polygon-features-%d.hfile".format(index / groupSize), factory)
        println("written %d files of %d features, total: %d".format(index / groupSize, groupSize, index))
      }

      val servingFeature = p.toGeocodeServingFeature

      if (includeParents) {
        findAndFixParents(servingFeature)
      }
      writer.append(
        serializer.serialize(new StringWrapper().setValue(p._id.toString)),
        serializer.serialize(servingFeature)
      )
      
      index += 1
    })
    writer.close()

    println("done")
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

  def buildBasicV1Writer(filename: String,
                         thriftProtocol: TProtocolFactory): HFile.Writer = {
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
    writer.appendFileInfo(HFileUtil.ThriftEncodingKeyBytes, thriftProtocol.getClass.getName.getBytes("UTF-8"))
    writer
  }

 def buildV1Writer[K: Manifest, V: Manifest](
      filename: String,
      thriftProtocol: TProtocolFactory): HFile.Writer = {

    // this is all weirdly 4sq specific logic :-(
    def fixName(n: String) = {
      if (n.contains("com.foursquare.twofishes.gen")) {
        n
      } else {
        n.replace("com.foursquare.twofishes", "com.foursquare.twofishes.gen")
          .replace("com.foursquare.base.thrift", "com.foursquare.base.gen")

      }
    }

    val writer = buildBasicV1Writer(filename, thriftProtocol)
    val keyClassName = fixName(manifest[K].erasure.getName).getBytes("UTF-8")
    val valueClassName = fixName(manifest[V].erasure.getName).getBytes("UTF-8")

    writer.appendFileInfo(HFileUtil.ThriftClassKeyBytes, keyClassName)
    writer.appendFileInfo(HFileUtil.ThriftClassValueBytes, valueClassName)
    writer
  }
  
  def writeCollection[T <: AnyRef, K <: Any](
    filename: String,
    callback: (T) => (Array[Byte], Array[Byte]),
    dao: SalatDAO[T, K],
    sortField: String
  ) {
    val writer = buildBasicV1Writer(filename, factory)
    var fidCount = 0
    val fidSize = dao.collection.count
    val fidCursor = dao.find(MongoDBObject())
      .sort(orderBy = MongoDBObject(sortField -> 1)) // sort by _id asc
    fidCursor.foreach(f => {
      val (k, v) = callback(f)
      writer.append(k, v)
      fidCount += 1
      if (fidCount % 100000 == 0) {
        println("processed %d of %d %s".format(fidCount, fidSize, filename))
      }
    })
    writer.close()
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

  def writeNames() {
    var nameCount = 0
    val nameSize = NameIndexDAO.collection.count
    val nameCursor = NameIndexDAO.find(MongoDBObject())
      .sort(orderBy = MongoDBObject("name" -> 1)) // sort by nameBytes asc

    var prefixSet = new HashSet[String]

    var lastName = ""
    var nameFids = new HashSet[String]

    val writer = buildBasicV1Writer("name_index.hfile", factory)
    nameCursor.filterNot(_.name.isEmpty).foreach(n => {
      if (lastName != n.name) {
        if (lastName != "") {
          writer.append(lastName.getBytes(), fidStringsToByteArray(nameFids.toList))
          if (outputPrefixIndex) {
            1.to(List(maxPrefixLength, lastName.size).min).foreach(length => 
              prefixSet.add(lastName.substring(0, length))
            )
          }
        }
        nameFids = new HashSet[String]
        lastName = n.name
      }

      nameFids.add(n.fid)

      nameCount += 1
      if (nameCount % 100000 == 0) {
        println("processed %d of %d names".format(nameCount, nameSize))
      }
    })
    writer.close()

    if (outputPrefixIndex) {
      doOutputPrefixIndex(prefixSet)
    }
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

    val prefixWriter = buildBasicV1Writer("prefix_index.hfile", factory)
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

    prefixWriter.appendFileInfo("MAX_PREFIX_LENGTH".getBytes(), toBytes(maxPrefixLength))
    prefixWriter.close()
    println("done")
  }

  class FidMap {
    val fidMap = new HashMap[String, Option[ObjectId]]

    def get(fid: String): Option[ObjectId] = {
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

  val fidMap = new FidMap()

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
    val writer = buildV1Writer[StringWrapper, ObjectIdListWrapper]("id-mapping.hfile", factory)

    val sortedEntries = (slugEntries ++ oidEntries).sortWith(bytePairSort).foreach({case (k, v) => {
      writer.append(k, v)
    }})
    
    writer.close()
  }

  def writeFeatures() {
    def fixParentId(fid: String) = fidMap.get(fid).map(_.toString)

    writeCollection("features.hfile",
      (g: GeocodeRecord) => 
        (g._id.toByteArray(), serializeGeocodeRecordWithoutGeometry(g, fixParentId)),
      MongoGeocodeDAO, "_id")
  }

  def process() {
    writeNames()
    writeSlugsAndIds()
    writeFeatures()
  }

  val ThriftClassValueBytes: Array[Byte] = "value.thrift.class".getBytes("UTF-8")
  val ThriftClassKeyBytes: Array[Byte] = "key.thrift.class".getBytes("UTF-8")
  val ThriftEncodingKeyBytes: Array[Byte] = "thrift.protocol.factory.class".getBytes("UTF-8")

  // def processForGeoId() {
  //   val geoCursor = MongoGeocodeDAO.find(MongoDBObject())

  //   def pickBestId(g: GeocodeRecord): String = {
  //     g.ids.find(_.startsWith("geonameid")).getOrElse(g.ids(0))
  //   }
    
  //   val gidMap = new HashMap[String, String]

  //   val ids = MongoGeocodeDAO.find(MongoDBObject()).map(pickBestId)
  //     .toList.sort(lexicalSort)

  //   geoCursor.foreach(g => {
  //     if (g.ids.size > 1) {
  //       val bestId = pickBestId(g)
  //       g.ids.foreach(id => {
  //         if (id != bestId) {
  //           gidMap(id) = bestId
  //         }
  //       })
  //     }
  //   })

  //   def fixParentId(fid: String) = Some(gidMap.getOrElse(fid, fid))
  //   val filename = "gid-features.hfile"
  //   writeGeocodeRecords(filename, ids, fixParentId)
  // }

  def writeGeocodeRecords(
    filename: String,
    unsortedIds: Iterable[ObjectId],
    fixParentId: IdFixer
  ) {
    val ids = unsortedIds.toList.sort(objectIdSort)
    val writer = buildV1Writer[ObjectIdWrapper, GeocodeServingFeature](filename, factory)

    var fidCount = 0
    val fidSize = ids.size
    ids.grouped(2000).foreach(chunk => {
      val records = MongoGeocodeDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> chunk)))
      val idToRecord = records.map(r => (r._id, r)).toMap
      idToRecord.keys.toList.sort(objectIdSort).foreach(oid => {
        val g = idToRecord(oid)
        val (k, v) =
          (serializer.serialize(new ObjectIdWrapper().setValue(oid.toByteArray)),
           serializeGeocodeRecord(g, fixParentId))
        writer.append(k, v)
        fidCount += 1
        if (fidCount % 1000 == 0) {
          println("processed %d of %d %s".format(fidCount, fidSize, filename))
        }
      })
    })
    writer.close()
  }
}
