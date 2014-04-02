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