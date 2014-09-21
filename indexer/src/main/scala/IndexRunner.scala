// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.indexer

import akka.actor.{ActorSystem, Props}
import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes.importers.geonames.GeonamesParser
import com.foursquare.geo.quadtree.CountryRevGeo
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes._
import scala.collection.mutable.HashMap
import com.foursquare.twofishes.SlugEntry
import com.foursquare.twofishes.output._
import com.foursquare.twofishes.mongo._
import com.foursquare.twofishes.util.{DurationUtils, GeoTools, Helpers, NameNormalizer, StoredFeatureId}
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import com.weiglewilczek.slf4s.Logging
import java.io.File
import scala.util.matching.Regex
import java.util.concurrent.CountDownLatch
import org.bson.types.ObjectId
import com.foursquare.twofishes.SlugEntryMap
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable.{HashMap, HashSet}
import scala.io.Source
import scalaj.collection.Implicits._
import com.twitter.ostrich.stats.Stats
import com.twitter.ostrich.admin.{RuntimeEnvironment, ServiceTracker}
import com.twitter.ostrich.admin.config.AdminServiceConfig
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization

object IndexRunner extends DurationUtils {
  var config: IndexRunnerConfig = null

  val adminConfig = new AdminServiceConfig {
    httpPort = 7655
  }
  val runtime = RuntimeEnvironment(this, Array.empty)
  val admin = adminConfig()(runtime)

  val system = ActorSystem("S2CoveringSystem")

  val (s2CoveringMaster, s2CoveringLatch) = if (config != null && (config.outputRevgeo || config.outputS2Covering)) {
    val latch = new CountDownLatch(1)
    (Some(system.actorOf(Props(new S2CoveringMaster(latch)), name = "master")), Some(latch))
   } else {
    (None, None)
  }

  val store = new MongoGeocodeStorageService()

  def main(args: Array[String]) {
     try {
      CountryRevGeo.getNearestCountryCode(40.74, -74)
    } catch {
      case e: Exception => {
        println("caught exception in country revgeo, might need to run \ngit submodule init\n git submodule update;")
        System.exit(1)
      }
    }

    config = IndexRunnerConfigParser.parse(args)
    val parser = new GeonamesParser(args, store)

    if (config.reloadData) {
      MongoGeocodeDAO.collection.drop()
      NameIndexDAO.collection.drop()
      PolygonIndexDAO.collection.drop()
      RevGeoIndexDAO.collection.drop()
      S2CoveringIndexDAO.collection.drop()
      parser.loadIntoMongo()
      writeIndexes(s2CoveringLatch, parser.getSlugEntryMap)
    } else {
      writeIndexes(None, parser.getSlugEntryMap)
    }


    implicit val formats = Serialization.formats(NoTypeHints)
    val prettyJsonStats = Serialization.writePretty(JsonMethods.parse(Stats.get().toJson))
    logger.info(prettyJsonStats)
    logger.info("all done with parse, trying to shutdown admin server and exit")
    admin.foreach(_.shutdown())
    System.exit(0)
  }

  def makeFinalIndexes() {
    logPhase("making indexes before generating output") {
      PolygonIndexDAO.makeIndexes()
      RevGeoIndexDAO.makeIndexes()
      S2CoveringIndexDAO.makeIndexes()
    }
  }

  def writeIndex(args: Array[String]) {
    writeIndexes(None, new HashMap[String, SlugEntry])
  }

  // TODO(blackmad): if we aren't redoing mongo indexing
  // then add some code to see if the s2 index is 'done'
  // We should also add an option to skip reloading polys
  def writeIndexes(
      s2CoveringLatch: Option[CountDownLatch],
      slugEntryMap: SlugEntryMap.SlugEntryMap) {
    makeFinalIndexes()
    val outputter = new OutputIndexes(
      config.hfileBasePath,
      config.outputPrefixIndex,
      slugEntryMap,
      config.outputRevgeo,
      config.outputS2Covering
    )
    outputter.buildIndexes(s2CoveringLatch)
  }
}
