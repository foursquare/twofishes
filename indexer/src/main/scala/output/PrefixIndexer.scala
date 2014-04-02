package com.foursquare.twofishes.output

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