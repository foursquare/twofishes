package com.foursquare.twofishes.output

import com.foursquare.twofishes.{FeatureNameFlags, Indexes, NameIndex, NameIndexDAO, YahooWoeType}
import com.foursquare.twofishes.util.StoredFeatureId
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import java.io._
import org.apache.hadoop.hbase.util.Bytes._
import scala.collection.mutable.HashSet
import scalaj.collection.Implicits._


object PrefixIndexer {
  val MaxPrefixLength = 5
  val index = Indexes.PrefixIndex
}

class PrefixIndexer(
  override val basepath: String, 
  override val fidMap: FidMap,
  prefixSet: HashSet[String]
) extends Indexer {
  val index = PrefixIndexer.index
  override val outputs = Seq(index)

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

  def writeIndexImpl() {
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

    val prefixWriter = buildMapFileWriter(index,
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