// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.twofishes._
import com.foursquare.twofishes.mongo.{GeocodeStorageWriteService, MongoGeocodeDAO}
import com.foursquare.twofishes.util.{Helpers, NameUtils, SlugBuilder, StoredFeatureId}
import java.io.File
import scala.collection.mutable.{HashMap, HashSet}
import scalaj.collection.Implicits._


// TODO
// stop using string representations of "a:b" featureids everywhere, PLEASE

class SlugIndexer {
  val idToSlugMap = new HashMap[String, String]
  val slugEntryMap = new SlugEntryMap.SlugEntryMap
  var missingSlugList = new HashSet[String]

  def getBestSlug(id: StoredFeatureId): Option[String] = {
    idToSlugMap.get(id.humanReadableString)
  }

  def addMissingId(id: StoredFeatureId) {
    missingSlugList.add(id.humanReadableString)
  }

  Helpers.duration("readSlugs") { readSlugs() }

  def readSlugs() {
    // step 1 -- load existing slugs into ... memory?
    val files = List(
      new File("data/computed/slugs.txt"),
      new File("data/private/slugs.txt")
    )
    files.foreach(file =>
      if (file.exists) {
        val fileSource = scala.io.Source.fromFile(file)
        val lines = fileSource.getLines.toList.filterNot(_.startsWith("#"))
        lines.map(l => {
          val parts = l.split("\t")
          val slug = parts(0)
          val id = parts(1)
          val score = parts(2).toInt
          val deprecated = parts(3).toBoolean
          slugEntryMap(slug) = SlugEntry(id, score, deprecated = deprecated, permanent = true)
          if (!deprecated) {
            idToSlugMap(id) = slug
          }
        })
      }
    )
    println("read %d slugs".format(slugEntryMap.size))
  }

   // TODO: not in love with this talking directly to mongo, please fix
  import com.mongodb.casbah.Imports._
  import com.novus.salat._
  import com.novus.salat.annotations._
  import com.novus.salat.dao._
  import com.novus.salat.global._
  val parentMap = new HashMap[StoredFeatureId, Option[GeocodeFeature]]

  def findFeature(fid: StoredFeatureId): Option[GeocodeServingFeature] = {
    val ret = MongoGeocodeDAO.findOne(MongoDBObject("_id" -> fid.longId)).map(_.toGeocodeServingFeature)
    if (ret.isEmpty) {
      println("couldn't find %s".format(fid))
    }
    ret
  }

  def findParent(fid: StoredFeatureId): Option[GeocodeFeature] = {
    parentMap.getOrElseUpdate(fid, findFeature(fid).map(_.feature))
  }

  def calculateSlugScore(f: GeocodeServingFeature): Int = {
    f.scoringFeatures.boost + f.scoringFeatures.population
  }

  def matchSlugs(id: String, servingFeature: GeocodeServingFeature, possibleSlugs: List[String]): Option[String] = {
    // println("trying to generate a slug for %s".format(id))
    possibleSlugs.foreach(slug => {
      // println("possible slug: %s".format(slug))
      val existingSlug = slugEntryMap.get(slug)
      val score = calculateSlugScore(servingFeature)
      existingSlug match {
        case Some(existing) => {
          if (!existing.permanent && score > existing.score) {
            val evictedId = existingSlug.get.id
            // println("evicting %s and recursing".format(evictedId))
            slugEntryMap(slug) = SlugEntry(id, score, deprecated = false, permanent = false)
            buildSlug(evictedId)
            return Some(slug)
          }
        }
        case _ => {
          // println("picking %s".format(slug))
          slugEntryMap(slug) = SlugEntry(id, score, deprecated = false, permanent = false)
          idToSlugMap(id) = slug
          return Some(slug)
        }
      }
    })
  // println("failed to find any slug")
    return None
  }

  def buildSlug(id: String) {
    val oldSlug = idToSlugMap.get(id)
    val oldEntry = oldSlug.map(slug => slugEntryMap(slug))
    var newSlug: Option[String] = None

    for {
      fid <- StoredFeatureId.fromHumanReadableString(id)
      servingFeature <- findFeature(fid)
      if (servingFeature.scoringFeatures.population > 0 ||
          servingFeature.scoringFeatures.boost > 0 ||
          servingFeature.feature.geometry.wkbGeometryOption.nonEmpty ||
          servingFeature.feature.woeTypeOption.exists(YahooWoeTypes.isAdminWoeType) ||
           (servingFeature.feature.attributesOption.exists(_.adm1capOption.exists(a => a)) ||
            servingFeature.feature.attributesOption.exists(_.adm0capOption.exists(a => a)))
        )
    } {
      val parents = servingFeature.scoringFeatures.parentIds
        .flatMap(StoredFeatureId.fromLong _)
        .flatMap(findParent _).toList
      var possibleSlugs = SlugBuilder.makePossibleSlugs(servingFeature.feature, parents)

      // if a city is bigger than 2 million people, we'll attempt to use the bare city name as the slug
      // unless it's the US, where I'd rather have consistency of always doing city-state
      if (servingFeature.scoringFeatures.population > 2000000 && servingFeature.feature.cc != "US") {
        possibleSlugs = NameUtils.bestName(servingFeature.feature, Some("en"), false).toList.map(n => SlugBuilder.normalize(n.name)) ++ possibleSlugs
      }

      newSlug = matchSlugs(id, servingFeature, possibleSlugs)
      if (newSlug.isEmpty && possibleSlugs.nonEmpty) {
        var extraDigit = 1
        var slugFound = false
        while (!newSlug.isEmpty) {
          newSlug = matchSlugs(id, servingFeature, possibleSlugs.map(s => "%s-%d".format(s, extraDigit)))
          extraDigit += 1
        }
      }
    }
    if (newSlug != oldSlug) {
      println("deprecating old slug for %s %s -> %s".format(id, oldSlug, newSlug.getOrElse("newslug")))
      oldEntry.map(_.deprecated = true)
    }
  }

  def buildMissingSlugs() {
    println("building missing slugs for %d fetures".format(missingSlugList.size))
    // step 2 -- compute slugs for records without
    for {
      (id, index) <- missingSlugList.zipWithIndex
    } {
      if (index % 10000 == 0) {
        println("built %d of %d slugs".format(index, missingSlugList.size))
      }
      buildSlug(id)
    }

    // step 3 -- write new slug file
    println("writing new slug map for %d features".format(slugEntryMap.size))
    val p = new java.io.PrintWriter(new File("data/computed/slugs.txt"))
    slugEntryMap.keys.toList.sorted.foreach(slug =>
     p.println("%s\t%s".format(slug, slugEntryMap(slug)))
    )
    p.close()
  }

  def writeMissingSlugs(store: GeocodeStorageWriteService) {
    for {
      (id, index) <- missingSlugList.zipWithIndex
      slug <- idToSlugMap.get(id)
      fid <- StoredFeatureId.fromHumanReadableString(id)
    } {
      if (index % 10000 == 0) {
        println("flushed %d of %d slug to mongo".format(index, missingSlugList.size))
      }
      store.addSlugToRecord(fid, slug)
    }
  }
}
