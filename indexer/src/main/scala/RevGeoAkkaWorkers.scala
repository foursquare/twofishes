package com.foursquare.twofishes

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.routing.{Broadcast, RoundRobinRouter}
import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util.{DurationUtils, GeometryUtils, RevGeoConstants}
import com.foursquare.twofishes.mongo.{PolygonIndexDAO, RevGeoIndexDAO, RevGeoIndex}
import com.google.common.geometry.S2CellId
import com.mongodb.casbah.Imports._
import com.vividsolutions.jts.geom.{Point => JTSPoint}
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import com.weiglewilczek.slf4s.Logging
import java.util.concurrent.CountDownLatch
import org.bson.types.ObjectId
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
    PolygonIndexDAO.find(MongoDBObject("_id" -> msg.polyIds)).foreach(p =>
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
              RevGeoIndex(
                cellid.id(), polyId,
                full = false,
                geom = Some(wkbWriter.write(s2shape.intersection(recordShape)))
              )
            }
          }
        })
        RevGeoIndexDAO.insert(records)
      }
    }
  }

  def receive = {
    case msg: CalculateCover =>
      calculateCover(msg)
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
  var totalSeen = 0
  var seenDone = false

  // message handler
  def receive = {
    case msg: FinishedCover =>
      totalSeen -= 1
      if (totalSeen == 0 && seenDone) {
        logger.info("finished all revgeo covers, shutting down system")
        self ! PoisonPill
      }
      if (totalSeen < 0) {
        logger.error("totalSeen < 0 ... we're bad at a counting")
      }
    case msg: CalculateCover =>
      totalSeen += 1
	    router ! msg
    case msg: CalculateCoverFromMongo =>
      totalSeen += msg.polyIds.size
      router ! msg
    case msg: Done =>
      logger.info("all done with revgeo coverage indexing, sending poison pills")
      // send a PoisonPill to all workers telling them to shut down themselves
      router ! Broadcast(PoisonPill)
      latch.countDown()
      seenDone = true
      if (totalSeen == 0) {
        logger.info("had already finished all revgeo covers, shutting down system")
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
