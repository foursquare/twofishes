// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.twofishes._
import com.foursquare.twofishes.util.NameNormalizer
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.LongWritable
import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import com.rockymadden.stringmetric.transform._

class BaseUnmatchedPolygonFeatureMatchingIntermediateJob(
  name: String,
  polygonSources: Seq[String],
  featureSources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val polygons = getJobOutputsAsTypedPipe[PolygonMatchingKeyWritable, PolygonMatchingValues](polygonSources).group
  val features = getJobOutputsAsTypedPipe[PolygonMatchingKeyWritable, PolygonMatchingValues](featureSources).group

  val customRewritesCachedFile = getCachedFileByRelativePath("custom/rewrites.txt")
  @transient lazy val nameRewritesMap = InMemoryLookupTableHelper.buildNameRewritesMap(Seq(customRewritesCachedFile))

  val customDeletesCachedFile = getCachedFileByRelativePath("custom/deletes.txt")
  @transient lazy val nameDeletesList = InMemoryLookupTableHelper.buildNameDeletesList(Seq(customDeletesCachedFile))

  val spaceRegex = " +".r
  def fixName(s: String) = spaceRegex.replaceAllIn(s, " ").trim

  def doRewrites(names: Seq[String]): Seq[String] = {
    val nameSet = new scala.collection.mutable.HashSet[String]()
    nameRewritesMap.foreach({case(from, toList) => {
      names.foreach(name => {
        toList.foreach(to => {
          nameSet += from.replaceAllIn(name, to)
        })
      })
    }})
    nameSet ++= names.map(_.replace("ß", "ss"))
    nameSet.toSeq
  }

  lazy val bigDeleteRe = {
    val re = nameDeletesList
      .map(_ + "\\b")
      .sortBy(_.length * -1)
      .mkString("|")
    ("(?i)%s".format(re)).r
  }

  def doDelete(name: String): Option[String] = {
    val newName = bigDeleteRe.replaceAllIn(name, "")
    if (newName != name) {
      Some(fixName(newName))
    } else {
      None
    }
  }

  def removeDiacritics(text: String) = {
    NameNormalizer.normalize(text)
  }

  // really, we should apply all the normalizations to shape names that we do to
  // geonames
  def applyHacks(originalText: String): Seq[String] = {
    val text = originalText
    val translit = PolygonMatchingHelper.transliterator.transform(text)

    val names = (List(text, translit) ++ doDelete(text)).toSet
    (doRewrites(names.toSeq) ++ names).toSet.toSeq.map(removeDiacritics)
  }

  def scoreMatch(polygonNames: Seq[FeatureName], candidateNames: Seq[FeatureName]): Int = {
    val polygonNamesModified = polygonNames.map(_.name).flatMap(applyHacks)
    val candidateNamesModified = candidateNames.map(_.name).flatMap(applyHacks)

    val composedTransform = (filterAlpha andThen ignoreAlphaCase)

    val scores = polygonNamesModified.flatMap(pn => {
      candidateNamesModified.map(cn => {
        if (cn.nonEmpty && pn.nonEmpty) {
          if (cn == pn) {
            return 100
          } else {
            val jaroScore = (JaroWinklerMetric withTransform composedTransform).compare(pn, cn).getOrElse(0.0)
            jaroScore * 100
          }
        } else {
          0
        }
      })
    })

    if (scores.nonEmpty) { scores.max.toInt } else { 0 }
  }

  def getBestMatchIds(polygon: PolygonMatchingValue, candidates: Seq[PolygonMatchingValue]): Seq[Long] = {
    val scores: Seq[(Int, Long)] = candidates.map(candidate => {
      (scoreMatch(polygon.names, candidate.names) -> candidate.featureIdOption.getOrElse(0L))
    }).filter({case (score: Int, featureId: Long) => score > 95}).toSeq

    val scoresMap = scores.groupBy({case (score: Int, featureId: Long) => score})

    if (scoresMap.nonEmpty) {
      scoresMap(scoresMap.keys.max).map({case (score: Int, featureId: Long) => featureId})
    } else {
      Nil
    }
  }

  val joined = polygons.join(features)
  (for {
    (k, (pValues, fValues)) <- joined
    polygon <- pValues.values
    polygonId <- polygon.polygonIdOption.toList
    preferenceLevel <- polygon.polygonWoeTypePreferenceLevelOption.toList
    candidates = fValues.values
    bestMatchId <- getBestMatchIds(polygon, candidates)
  } yield {
    // 1. first emit polygonId -> featureId + preferenceLevel
    val matchingValue = PolygonMatchingValue.newBuilder
      .featureId(bestMatchId)
      .polygonWoeTypePreferenceLevel(preferenceLevel)
      .result
    (polygonId -> matchingValue)
  }).group
    // 2. then filter down to features which match at best preferenceLevel
    .toList
    .mapValues({featureMatches: List[PolygonMatchingValue] => {
      val preferenceMap = featureMatches.groupBy(_.polygonWoeTypePreferenceLevelOption.getOrElse(0))
      val matchesAtBestLevel = preferenceMap(preferenceMap.keys.min)
      PolygonMatchingValues(matchesAtBestLevel)
    }})
    // 3. flip feature and polygonIds
    .flatMap({case (polygonId: Long, featureMatches: PolygonMatchingValues) => {
      for {
        featureMatch <- featureMatches.values
        featureId <- featureMatch.featureIdOption
      } yield {
        val matchingValue = PolygonMatchingValue.newBuilder
          .polygonId(polygonId)
          .result
        (new LongWritable(featureId) -> matchingValue)
      }
    }})
    // 4. finally group by featureId and pick highest valued polygonId
    .group
    .maxBy(_.polygonIdOption.getOrElse(0L))
    .write(TypedSink[(LongWritable, PolygonMatchingValue)](SpindleSequenceFileSource[LongWritable, PolygonMatchingValue](outputPath)))
}
