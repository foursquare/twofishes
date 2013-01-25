package com.foursquare.twofishes

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

import com.foursquare.twofishes.util.{GeometryUtils, Hacks, NameUtils}
import com.foursquare.base.gen.{LongWrapper, ObjectIdWrapper, ObjectIdListWrapper, StringWrapper}
import com.foursquare.batch.ShapefileSimplifier
import com.foursquare.geo.shapes.ShapefileS2Util

import com.google.common.geometry.S2CellId

import com.vividsolutions.jts.io.{WKBReader, WKBWriter}

import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.util.Arrays

import org.apache.hadoop.conf.Configuration 
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hbase.KeyValue.KeyComparator
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, Compression, HFile, HFileScanner, HFileWriterV1, HFileWriterV2}
import org.apache.hadoop.hbase.util.Bytes._

import org.apache.thrift.TBase
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.{TProtocolFactory, TBinaryProtocol, TCompactProtocol}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scalaj.collection.Implicits._

import java.io._

import com.twitter.util.Duration

object HFileUtil {
  val ThriftClassValueBytes: Array[Byte] = "value.thrift.class".getBytes("UTF-8")
  val ThriftClassKeyBytes: Array[Byte] = "key.thrift.class".getBytes("UTF-8")
  val ThriftEncodingKeyBytes: Array[Byte] = "thrift.protocol.factory.class".getBytes("UTF-8")
}

class OutputHFile(basepath: String, outputPrefixIndex: Boolean, slugEntryMap: SlugEntryMap) {
  val blockSizeKey = "hbase.mapreduce.hfileoutputformat.blocksize"
  val compressionKey = "hfile.compression"

  val blockSize = HFile.DEFAULT_BLOCKSIZE
  val compressionAlgo = Compression.Algorithm.NONE.getName

  val conf = new Configuration()
  val cconf = new CacheConfig(conf)
  
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

  def serializeGeocodeRecord(g: GeocodeRecord, fixParentId: IdFixer) = {
    val f = g.toGeocodeServingFeature()

    val parents = for {
      parent <- f.scoringFeatures.parents
      parentId <- fixParentId(parent)
    } yield {
      parentId
    }

    f.scoringFeatures.setParents(parents)
    serializer.serialize(f)
  }

  val stateCache = new HashMap[List[String], Option[String]]
  def findStateName(parents: List[String]) = {
    stateCache.getOrElseUpdate(parents, {
      val features = MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> parents.toList),
        "_woeType" -> YahooWoeType.ADMIN1.getValue))
      features.map(_.toGeocodeServingFeature).flatMap(sf => 
        NameUtils.bestName(sf.feature, None, preferAbbrev = true)
      ).map(_.name).toList.headOption
    })
  }

  def buildChildEntries(children: Iterator[GeocodeRecord]): List[ChildEntry] = {
    (for {
      child <- children
      val feature = child.toGeocodeServingFeature.feature
      name <- NameUtils.bestName(feature, Some("en"), false)
      slug <- child.slug
      // hack because we're setting the boost of poorly-known neighborhoods to < 0
      if (child._woeType != YahooWoeType.SUBURB.getValue || child.boost.getOrElse(0) >= 0)
    } yield {
      var finalName = name.name
      if (child.cc == "US" && child._woeType == YahooWoeType.TOWN.getValue) {
        // awful hack to yank out state
        findStateName(child.parents) match {
          case Some(stateCode) => finalName = "%s, %s".format(name.name, stateCode)
          case None => {
            println("failed to find state parents for: " + feature.id)
            println(child.parents)
          }
        }
      }
      new ChildEntry().setName(finalName).setSlug(slug).setWoeType(child.woeType)
    }).toList
  }

  def buildChildMap(
    parentType: YahooWoeType,
    childType: YahooWoeType,
    limit: Int,
    minPopulation: Int,
    minPopulationPerCounty: Map[String, Int] = Map.empty
  ): Map[String, List[ChildEntry]] = {
    // find all parents of parentType
    val parents = MongoGeocodeDAO.find(MongoDBObject("_woeType" -> parentType.getValue))

    parents.flatMap(parent => {
      val servingFeature = parent.toGeocodeServingFeature
      val children = MongoGeocodeDAO.find(MongoDBObject("parents" -> servingFeature.feature.id,
        "hasPoly" -> true,
        "_woeType" -> childType.getValue
        ))
        .sort(orderBy = MongoDBObject("population" -> -1)) // sort by population descending
        .limit(limit)

      val childEntries = buildChildEntries(children.filter(child => {
        val population = child.population.getOrElse(0)
        (population > minPopulation ||  minPopulationPerCounty.get(child.cc).exists(_ < population))
      }))
      if (servingFeature.feature.id == null) {
        println("null id: " + servingFeature)
      }
      if (childEntries.size > 0) {
        println("found %d children for %s".format(childEntries.size, servingFeature.feature.id))
        Some(servingFeature.feature.id -> childEntries.toList)
      } else {
        None
      }
    }).toMap

    // for each parent, find all children of childType, sorted by descending popularity
    // trim down those records super aggressively
  }

  def buildRootMap(countryIds: Iterable[String]):  Map[String, List[ChildEntry]] = {
    val countries = MongoGeocodeDAO.find(MongoDBObject("ids" -> MongoDBObject("$in" -> countryIds.toList)))
      .sort(orderBy = MongoDBObject("population" -> -1)) // sort by population descending

    Map(
      "" -> buildChildEntries(countries)
    )
  }

  def buildChildMaps() {
    val countryToTownMap =
      buildChildMap(YahooWoeType.COUNTRY, YahooWoeType.TOWN, 1000, 300000,
        Map(("US" -> 150000)))

    val childMaps = 
      buildRootMap(countryToTownMap.keys) ++
      countryToTownMap ++
      buildChildMap(YahooWoeType.ADMIN1, YahooWoeType.TOWN, 1000, 300000) ++
      buildChildMap(YahooWoeType.TOWN, YahooWoeType.SUBURB, 1000, 0) ++
      buildChildMap(YahooWoeType.ADMIN2, YahooWoeType.SUBURB, 1000, 0)

    val writer = buildV1Writer[StringWrapper, ChildEntries]("child_map.hfile", factory)

    println("sorting")

    val sortedMapKeys = childMaps.keys.toList.sort(lexicalSort)

    println("sorted")
   val comp = new ByteArrayComparator()

    sortedMapKeys.map(k => {
      (serializer.serialize(new StringWrapper().setValue(k.toString)),
       serializer.serialize(new ChildEntries().setEntries(childMaps(k))))
   }).toList.sortWith((a, b) => {
     comp.compare(a._1, b._1) < 0
   }).foreach({case (k, v) => {
     writer.append(k, v)
   }})
    writer.close()
    println("done")
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
    println(what + " in %s µs / %s ms".format(duration.inMicroseconds, duration.inMilliseconds))
    rv
  }

  def buildRevGeoIndex() {
    val wkbReader = new WKBReader()
    val wkbWriter = new WKBWriter()

    val records = MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))

    val s2map = new HashMap[ByteBuffer, HashSet[CellGeometry]]

    val minS2Level = 9
    val maxS2Level = 15
    val levelMod = 2

    for {
      (record, index) <- records.zipWithIndex
      polygon <- record.polygon
    } {
      println("reading poly %s".format(index))
      val geom = wkbReader.read(polygon)

      var numCells = 0

      for {
        level <- minS2Level.to(maxS2Level)
        if (level - minS2Level) % levelMod == 0
      } {
        logDuration("generating covering at level %s".format(level)) {
          GeometryUtils.s2PolygonCovering(geom, level, level).foreach(
            (cellid: S2CellId) => {
              if (cellid.level() != level) {
                println("generated wrong level: %d SHOULD NOT HAPPEN at %d".format(cellid.level, level))
              } else {
                val s2Bytes: Array[Byte] = GeometryUtils.getBytes(cellid)
                val bucket = s2map.getOrElseUpdate(ByteBuffer.wrap(s2Bytes), new HashSet[CellGeometry]())
                val cellGeometry = new CellGeometry()
                cellGeometry.setWkbGeometry(wkbWriter.write(ShapefileS2Util.clipGeometryToCell(geom, cellid)))
                cellGeometry.setWoeType(record.woeType)
                cellGeometry.setOid(record._id.toByteArray())
                bucket.add(cellGeometry)
                numCells += 1
              }
            } 
          )
        }
      }
      println("outputted %d cells".format(numCells))
    }

    val sortedMapKeys = s2map.keys.toList.sort(byteBufferSort)
    val writer = buildV2Writer(new File(basepath, "s2_index.hfile").toString)
    sortedMapKeys.foreach(k => {
      val cellGeometries = new CellGeometries().setCells(s2map(k).toList)
      writer.append(k.array(), serializer.serialize(cellGeometries))
    })

    writer.appendFileInfo("minS2Level".getBytes("UTF-8"), GeometryUtils.getBytes(minS2Level))
    writer.appendFileInfo("maxS2Level".getBytes("UTF-8"), GeometryUtils.getBytes(maxS2Level))
    writer.close()

    buildPolygonIndex()
  }

  def buildPolygonFeatureIndex(groupSize: Int) {
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

      writer.append(
        serializer.serialize(new StringWrapper().setValue(p._id.toString)),
        serializer.serialize(p.toGeocodeServingFeature)
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

  def buildV2Writer(filename: String) = {
    val fs = new LocalFileSystem() 
    val path = new Path(new File(basepath, filename).toString)
    fs.initialize(URI.create("file:///"), conf)
    new HFileWriterV2(conf, cconf, fs, path, blockSize, compressionAlgo, null)
  }

 def buildV1Writer[K: Manifest, V: Manifest](
      filename: String,
      thriftProtocol: TProtocolFactory): HFileWriterV1 = {
    val fs = new LocalFileSystem() 
    val path = new Path(new File(basepath, filename).toString)
    fs.initialize(URI.create("file:///"), conf)
    val writer = new HFileWriterV1(conf, cconf, fs, path, blockSize, compressionAlgo, null)
    writer.appendFileInfo(HFileUtil.ThriftClassValueBytes, manifest[V].erasure.getName.getBytes("UTF-8"))
    writer.appendFileInfo(HFileUtil.ThriftClassKeyBytes, manifest[K].erasure.getName.getBytes("UTF-8"))
    writer.appendFileInfo(HFileUtil.ThriftEncodingKeyBytes, thriftProtocol.getClass.getName.getBytes("UTF-8"))
    writer
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

    val writer = buildV2Writer("name_index.hfile")
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

    val prefixWriter = buildV2Writer("prefix_index.hfile")
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
    val p = new java.io.PrintWriter(new File(basepath, "id-mapping.txt"))
    slugEntryMap.foreach({case (slug, entry) => {
      p.println("%s\t%s".format(slug, entry.id))
    }})

    MongoGeocodeDAO.find(MongoDBObject()).foreach(geocodeRecord => {
      geocodeRecord.ids.foreach(id => {
        p.println("%s\t%s".format(id, geocodeRecord._id))
      })
    })

    p.close()
  }

  def writeFeatures() {
    def fixParentId(fid: String) = fidMap.get(fid).map(_.toString)

    writeCollection("features.hfile",
      (g: GeocodeRecord) => 
        (g._id.toByteArray(), serializeGeocodeRecord(g, fixParentId)),
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
