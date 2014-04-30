package com.foursquare.twofishes

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.routing.{Broadcast, RoundRobinRouter}
import com.mongodb.Bytes
import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util.{DurationUtils, GeometryUtils, RevGeoConstants}
import com.foursquare.twofishes.mongo.{PolygonIndexDAO, RevGeoIndexDAO, RevGeoIndex}
import com.google.common.geometry.S2CellId
import com.mongodb.casbah.Imports._
import com.twitter.ostrich.stats.Stats
import com.vividsolutions.jts.geom.{Point => JTSPoint, Geometry}
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import com.weiglewilczek.slf4s.Logging
import java.util.concurrent.CountDownLatch
import org.bson.types.ObjectId
import scala.collection.JavaConverters._
import scalaj.collection.Implicits._
import java.util.concurrent.atomic.AtomicInteger

// ====================
// ===== Messages =====
// ====================
sealed trait CoverMessage
case class Done() extends CoverMessage
case class CalculateCoverFromMongo(polyIds: List[ObjectId]) extends CoverMessage
case class CalculateCover(polyId: ObjectId, geomBytes: Array[Byte]) extends CoverMessage
case class FinishedCover() extends CoverMessage

class NullActor extends Actor {
  def receive = {
    case x =>
  }
}

object TerribleCounter {
  val count = new AtomicInteger
}

class RevGeoWorker extends Actor with DurationUtils with RevGeoConstants with Logging {
  val wkbReader = new WKBReader()
  val wkbWriter = new WKBWriter()

  def calculateCoverFromMongo(msg: CalculateCoverFromMongo) {
    val records = PolygonIndexDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> msg.polyIds)))
    records.option = Bytes.QUERYOPTION_NOTIMEOUT
    records.foreach(p =>
      calculateCover(p._id, p.polygon)
    )
  }

  def calculateCover(msg: CalculateCover) {
    calculateCover(msg.polyId, msg.geomBytes)
  }

  def calculateCover(polyId: ObjectId, geomBytes: Array[Byte]) {
    logDuration("totalCovering", "generated cover for %s".format(polyId)) {
      val currentCount = TerribleCounter.count.getAndIncrement()

      if (currentCount % 1000 == 0) {
        logger.info("processed about %s polygons for revgeo coverage".format(currentCount))
      }

      val geom = wkbReader.read(geomBytes)

     	// println("generating cover for %s".format(polyId))
      val cells = logDuration("s2Covering", "generated cover for %s".format(polyId)) {
        GeometryUtils.s2PolygonCovering(
          geom, minS2Level, maxS2Level,
          levelMod = Some(defaultLevelMod),
          maxCellsHintWhichMightBeIgnored = Some(1000)
        )
      }

      logDuration("coverClipping", "clipped and outputted cover for %d cells (%s)".format(cells.size, polyId)) {
        val records = cells.map((cellid: S2CellId) => {
          if (geom.isInstanceOf[JTSPoint]) {
            RevGeoIndex(
              cellid.id(), polyId,
              full = false,
              geom = Some(wkbWriter.write(geom))
            )
          } else {
            val recordShape = geom.buffer(0)
            val preparedRecordShape = PreparedGeometryFactory.prepare(recordShape)
            val s2shape = ShapefileS2Util.fullGeometryForCell(cellid)
            if (preparedRecordShape.contains(s2shape)) {
      	      RevGeoIndex(cellid.id(), polyId, full = true, geom = None)
            } else {
              val intersection = s2shape.intersection(recordShape)
              val geomToIndex = if (intersection.getGeometryType == "GeometryCollection") {
                cleanupGeometryCollection(intersection)
              } else {
                intersection
              }
              RevGeoIndex(
                cellid.id(), polyId,
                full = false,
                geom = Some(wkbWriter.write(geomToIndex))
              )
            }
          }
        })
        RevGeoIndexDAO.insert(records)
      }
    }
  }

  private def cleanupGeometryCollection(geom: Geometry): Geometry = {
    val geometryCount = geom.getNumGeometries
    val polygons = for {
      i <- 0 to geometryCount - 1
      geometry = geom.getGeometryN(i)
      if (geometry.getGeometryType == "Polygon" ||
          geometry.getGeometryType == "MultiPolygon")
    } yield geometry
    geom.getFactory.buildGeometry(polygons.asJavaCollection)
  }

  def receive = {
    case msg: CalculateCover =>
      calculateCover(msg)
      sender ! FinishedCover()
    case msg: CalculateCoverFromMongo =>
      calculateCoverFromMongo(msg)
      sender ! FinishedCover()
  }
}


// ==================
// ===== Master =====
// ==================
class RevGeoMaster(val latch: CountDownLatch) extends Actor with Logging {
  var start: Long = 0

  val _system = ActorSystem("RoundRobinRouterExample")
  val router = _system.actorOf(Props[RevGeoWorker].withRouter(RoundRobinRouter(8)), name = "myRoundRobinRouterActor")
  var inFlight = 0
  var seenDone = false

  // message handler
  def receive = {
    case msg: FinishedCover =>
      inFlight -= 1
      if (inFlight == 0 && seenDone) {
        logger.info("finished all revgeo covers, shutting down system")
        latch.countDown()
        self ! PoisonPill
      }
      if (inFlight < 0) {
        logger.error("inFlight < 0 ... we're bad at a counting")
      }
    case msg: CalculateCover =>
      Stats.incr("revgeo.akkaWorkers.CalculateCover")
      inFlight += 1
	    router ! msg
    case msg: CalculateCoverFromMongo =>
      inFlight += 1
      router ! msg
    case msg: Done =>
      logger.info("all done with revgeo coverage indexing, sending poison pills")
      // send a PoisonPill to all workers telling them to shut down themselves
      router ! Broadcast(PoisonPill)
      seenDone = true
      if (inFlight == 0) {
        logger.info("had already finished all revgeo covers, shutting down system")
        latch.countDown()
        self ! PoisonPill
      }
  }

  override def preStart() {
    start = System.currentTimeMillis
  }

  override def postStop() {
    // tell the world that the calculation is complete
    logger.info(
      "revgeo covering calculation time: \t%s millis"
        .format((System.currentTimeMillis - start))
    )
  }
}
