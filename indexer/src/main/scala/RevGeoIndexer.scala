package com.foursquare.twofishes

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util.{GeometryUtils, StoredFeatureId}
import com.google.common.geometry.S2CellId
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import akka.actor.{Actor, PoisonPill}
import akka.routing.RoundRobinRoutingLogic
import akka.routing.Router
import akka.actor.ActorSystem
import akka.actor.Props
import akka.routing.RoundRobinRouter
import java.util.concurrent.CountDownLatch
import scalaj.collection.Implicits._
import akka.routing.Broadcast
import java.nio.ByteBuffer

// ====================
// ===== Messages =====
// ====================
sealed trait CoverMessage
case class Done extends CoverMessage
case class CalculateCover(geom: Array[Byte], geoid: Long, woeTypeInt: Int) extends CoverMessage {
  def woeType = YahooWoeType.findByIdOrNull(woeTypeInt)
}

class RevGeoWorker extends Actor with DurationUtils {
  val minS2Level = 8
  val maxS2Level = 12
  val maxCells = 10000
  val levelMod = 2

  val wkbReader = new WKBReader()
  val wkbWriter = new WKBWriter()
  def calculateCover(msg: CalculateCover) {
    val geom = wkbReader.read(msg.geom)
 	
 	println("generating cover for %s".format(msg.geoid))
    val cells = logDuration("generated cover for %s".format(msg.geoid)) {
      GeometryUtils.s2PolygonCovering(
        geom, minS2Level, maxS2Level,
        levelMod = Some(levelMod),
        maxCellsHintWhichMightBeIgnored = Some(1000)
      )
    }

    logDuration("clipped and outputted cover for %d cells (%s)".format(cells.size, msg.geoid)) {
      val recordShape = geom.buffer(0)
	  val preparedRecordShape = PreparedGeometryFactory.prepare(recordShape)
      cells.asScala.foreach((cellid: S2CellId) => {
        val s2shape = ShapefileS2Util.fullGeometryForCell(cellid)
        val cellGeometryBuilder = CellGeometry.newBuilder
        if (preparedRecordShape.contains(s2shape)) {
          cellGeometryBuilder.full(true)
        } else {
           cellGeometryBuilder.wkbGeometry(ByteBuffer.wrap(wkbWriter.write(s2shape.intersection(recordShape))))
        }
        cellGeometryBuilder.woeType(msg.woeType)
        cellGeometryBuilder.longId(msg.geoid)
        RevGeoIndexDAO.save(RevGeoIndex(cellid.id(), cellGeometryBuilder.result))
      })
    }
  }

  def receive = {
    case msg: CalculateCover =>
      // sender ! Result(calculatePiFor(start, nrOfElements)) // perform the work
      calculateCover(msg)
  }
}


// ==================
// ===== Master =====
// ==================
class RevGeoMaster(latch: CountDownLatch) extends Actor {
  var start: Long = 0

  val _system = ActorSystem("RoundRobinRouterExample")
  val router = _system.actorOf(Props[RevGeoWorker].withRouter(RoundRobinRouter(8)), name = "myRoundRobinRouterActor")

  // message handler
  def receive = {
    case msg: CalculateCover =>
	  router ! msg
    case msg: Done => 
    // send a PoisonPill to all workers telling them to shut down themselves
    router ! Broadcast(PoisonPill)

    // send a PoisonPill to the router, telling him to shut himself down
    router ! PoisonPill
  }

  override def preStart() {
    start = System.currentTimeMillis
  }

  override def postStop() {
    // tell the world that the calculation is complete
    println(
      "\n\tCalculation time: \t%s millis"
      .format((System.currentTimeMillis - start)))
    latch.countDown()
  }
}

