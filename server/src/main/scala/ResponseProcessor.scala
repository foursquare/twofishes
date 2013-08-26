//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.foursquare.twofishes.util.{NameUtils, StoredFeatureId}
import com.foursquare.twofishes.util.NameUtils.BestNameMatch
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, WKTWriter}
import org.bson.types.ObjectId
import scala.collection.mutable.ListBuffer
import scalaj.collection.Implicits._

// Sort a list of features, smallest to biggest
object GeocodeServingFeatureOrdering extends Ordering[GeocodeServingFeature] {
  def compare(a: GeocodeServingFeature, b: GeocodeServingFeature) = {
    YahooWoeTypes.getOrdering(a.feature.woeType) - YahooWoeTypes.getOrdering(b.feature.woeType)
  }
}

object ResponseProcessor {
  def generateResponse(debugLevel: Int, logger: MemoryLogger, interpretations: Seq[GeocodeInterpretation]): GeocodeResponse = {
    val resp = new GeocodeResponse()
    resp.setInterpretations(interpretations.asJava)
    if (debugLevel > 0) {
      resp.setDebugLines(logger.getLines.asJava)
    }
    resp
  }
}

// After generating parses, the code in this class is called to clean that up into
// GeocodeResponse/GeocodeInterpretation to return to the client
class ResponseProcessor(
  req: CommonGeocodeRequestParams,
  store: GeocodeStorageReadService,
  logger: MemoryLogger
) extends GeocoderImplTypes {
  def responseIncludes(include: ResponseIncludes): Boolean = GeocodeRequestUtils.responseIncludes(req, include)

  def dedupeParses(parses: SortedParseSeq): SortedParseSeq = {
    val parseIndexToNameMatch: Map[Int, Option[BestNameMatch]] =
      parses.zipWithIndex.map({case (parse, index) => {
        (index,
          parse.headOption.flatMap(f => {
            // en is cheating, sorry
            bestNameWithMatch(f.fmatch.feature, Some("en"), false, Some(f.phrase))
          })
        )
      }}).toMap

    val parseMap: Map[String, Seq[(Parse[Sorted], Int)]] = parses.zipWithIndex.groupBy({case (parse, index) => {
      parseIndexToNameMatch(index).map(_._1.name).getOrElse("")
    }})

    if (req.debug > 0) {
      parseMap.foreach({case(name, parseSeq) => {
        logger.ifDebug("have %d parses for %s", parseSeq.size, name)
        parseSeq.foreach(p => {
          logger.ifDebug("%s: %s", name, p._1)
        })
      }})
    }

    def isAliasName(index: Int): Boolean = {
      parseIndexToNameMatch(index).exists(_._1.flags.contains(
        FeatureNameFlags.ALIAS))
    }

    type ParsePair = (Parse[Sorted], Int)
    object DuplicateGeocodeParseOrdering extends Ordering[ParsePair] {
      def compare(a: ParsePair, b: ParsePair): Int = {
        // negative if a < b
        val isAliasA = isAliasName(a._2)
        val isAliasB = isAliasName(b._2)
        if (isAliasA != isAliasB) {
          if (isAliasA) { return -1 }
          else { return 1 }
        }

        val hasPolyA = a._1.headOption.exists(_.fmatch.scoringFeatures.hasPoly)
        val hasPolyB = b._1.headOption.exists(_.fmatch.scoringFeatures.hasPoly)

        if (hasPolyA != hasPolyB) {
          if (hasPolyA) { return 1 }
          else { return -11 }
        }

        // if a came before b, it was better
        // a = 2, b = 3 ... b - a ... 3 - 2 ... 1
        return b._2 - a._2
      }
    }



    val dedupedMap: Seq[(Parse[Sorted], Int)] = for {
      (textKey, parsePairs) <- parseMap.toSeq
      // bucket into 0.1 degree buckets (= 11km)
      val geoBuckets = parsePairs.groupBy({case (parse, index) => {
        "%s-%s".format(
          parse.headOption.map(_.fmatch.feature.geometry.center.lat / 0.1).getOrElse(0.0).toInt,
          parse.headOption.map(_.fmatch.feature.geometry.center.lng / 0.1).getOrElse(0.0).toInt)
        }})
        (geoKey, parses) <- geoBuckets
      } yield {
        logger.ifDebug("for %s, have %d parses in bucket %s".format(textKey, parses.size, geoKey))
        parses.sorted(DuplicateGeocodeParseOrdering).lastOption.get
      }
    // We have a map of [name -> List[Parse, Int]] ... extract out the parse-int pairs
    // join them, and re-sort by the int, which was their original ordering
    dedupedMap.toList.sortBy(_._2).map(_._1)
  }

   // Modifies a GeocodeFeature that is about to be returned
  // --set 'name' to the feature's best name in the request context
  // --set 'displayName' to a string that includes names of parents in the request context
  // --filter out the total set of names to a more managable number
  // --add in parent features if needed
  def fixFeature(
      f: GeocodeFeature,
      parents: Seq[GeocodeServingFeature],
      parse: Option[Parse[Sorted]],
      numExtraParentsRequired: Int = 0,
      fillHighlightedName: Boolean = false
    ) {
    // set name
    val name = NameUtils.bestName(f, Some(req.lang), false).map(_.name).getOrElse("")
    f.setName(name)

    // rules
    // if you have a city parent, use it
    // if you're in the US or CA, use state parent
    // if you need an extra parent, use it

    val parentsToUse = new ListBuffer[GeocodeServingFeature]
    parentsToUse.appendAll(
      parents.filter(p => p.feature.woeType == YahooWoeType.TOWN))

    if (f.cc == "US" || f.cc == "CA") {
      parentsToUse.appendAll(
        parents.filter(p => p.feature.woeType == YahooWoeType.ADMIN1))
    }

    val countryAbbrev: Option[String] = if (f.cc != req.cc) {
      if (f.cc == "GB") {
        Some("UK")
      } else {
        Some(f.cc)
      }
    } else {
      None
    }

    var namesToUse: Seq[(com.foursquare.twofishes.FeatureName, Option[String])] = Nil

    // set highlightedName and matchedName
    if (fillHighlightedName) {
      parse.foreach(p => {
        val partsFromParse: Seq[(Option[FeatureMatch], GeocodeServingFeature)] =
          p.map(fmatch => (Some(fmatch), fmatch.fmatch))

        val partsFromParents: Seq[(Option[FeatureMatch], GeocodeServingFeature)] =
          parentsToUse.filterNot((f: GeocodeServingFeature) => {
            partsFromParse.exists(_._2.longId =? f.longId)
          })
            .map(f => (None, f))

        val extraParents: Seq[(Option[FeatureMatch], GeocodeServingFeature)] =
          parents
            .filterNot(f => partsFromParse.exists(_._2.longId =? f.longId))
            .filterNot(f => partsFromParents.exists(_._2.longId =? f.longId))
            .filterNot(p => p.feature.woeType == YahooWoeType.COUNTRY)
            .takeRight(numExtraParentsRequired)
            .map(f => (None, f))

        val partsToUse = (partsFromParse ++ partsFromParents ++ extraParents).sortBy(_._2)(GeocodeServingFeatureOrdering)
        // logger.ifDebug("parts to use: " + partsToUse)
        var i = 0
        namesToUse = partsToUse.flatMap({case(fmatchOpt, servingFeature) => {
          // awful hack because most states outside the US don't actually
          // use their abbrev names
          val inUsOrCA = servingFeature.feature.cc == "US" ||  servingFeature.feature.cc == "CA"
          val name = bestNameWithMatch(servingFeature.feature, Some(req.lang),
            preferAbbrev = (i != 0 && inUsOrCA),
            fmatchOpt.map(_.phrase))
          i += 1
          name
        }})

        // strip dupe un-matched parts, so we don't have "Istanbul, Istanbul, TR"
        // don't strip out matched parts (that's the isempty check)
        // don't strip out the main feature name (index != 0)
        namesToUse = namesToUse.zipWithIndex.filterNot({case (nameMatch, index) => {
          index != 0 && nameMatch._2.isEmpty && nameMatch._1.name == namesToUse(0)._1.name
        }}).map(_._1)

        var (matchedNameParts, highlightedNameParts) =
          (namesToUse.map(_._1.name),
           namesToUse.map({case(fname, highlightedName) => {
            highlightedName.getOrElse(fname.name)
          }}))

        if (!partsToUse.exists(_._2.feature.woeType == YahooWoeType.COUNTRY)) {
          matchedNameParts ++= countryAbbrev.toList
          highlightedNameParts ++= countryAbbrev.toList
        }
        f.setMatchedName(matchedNameParts.mkString(", "))
        f.setHighlightedName(highlightedNameParts.mkString(", "))
      })
    }

    // possibly clear names
    val names = f.names
    f.setNames(names.asScala.filter(n =>
      Option(n.flags).exists(_.contains(FeatureNameFlags.ABBREVIATION)) ||
      n.lang == req.lang ||
      n.lang == "en" ||
      namesToUse.contains(n)
    ).asJava)

    // now pull in extra parents
    parentsToUse.appendAll(
      parents
        .filterNot(p => parentsToUse.has(p))
        .filterNot(p => p.feature.woeType == YahooWoeType.COUNTRY)
        .takeRight(numExtraParentsRequired)
    )

    val parentNames = parentsToUse
      .sorted(GeocodeServingFeatureOrdering)
      .map(p =>
        NameUtils.bestName(p.feature, Some(req.lang), true).map(_.name).getOrElse(""))
       .filterNot(parentName => {
         name == parentName
       })

    var displayNameParts = Vector(name) ++ parentNames
    if (f.woeType != YahooWoeType.COUNTRY) {
      displayNameParts ++= countryAbbrev.toList
    }
    f.setDisplayName(displayNameParts.mkString(", "))
  }

  // This function signature is gross
  // Given a set of parses, create a geocode response which has fully formed
  // versions of all the features in it (names, parents)
  def hydrateParses(
    sortedParsesIn: SortedParseSeq,
    parseParams: ParseParams,
    polygonMap: Map[StoredFeatureId, Geometry],
    fixAmbiguousNames: Boolean,
    dedupByMatchedName: Boolean = false
  ): Seq[GeocodeInterpretation] = {
    val tokens = parseParams.tokens
    val originalTokens = parseParams.originalTokens
    val connectorStart = parseParams.connectorStart
    val connectorEnd = parseParams.connectorEnd
    val hadConnector = parseParams.hadConnector

    // sortedParses.foreach(p => {
    //   logger.ifDebug(printDebugParse(p))
    // })

    val sortedParses = {
      // Order-preserving de-duplication by feature match id
      val seen = scala.collection.mutable.HashSet[Long]()
      sortedParsesIn.filter(sp => {
        if (!seen(sp(0).fmatch.longId)) {
          seen += sp(0).fmatch.longId
          true
        } else false
      })
    }

    val parentIdsAll: Seq[Long] = sortedParses.flatMap(
      _.headOption.toList.flatMap(_.fmatch.scoringFeatures.parentIds.asScala))
    val parentIds = parentIdsAll.distinct
    val parentFids: Seq[StoredFeatureId] = parentIds.flatMap(StoredFeatureId.fromLong _)
    logger.ifDebug("parent ids: %s", parentFids)

    // possible optimization here: add in features we already have in our parses and don't refetch them
    val parentMap = store.getByFeatureIds(parentFids)
    logger.ifDebug("parentMap: %s", parentMap)

    val interpretations = sortedParses.map(p => {
      val parseLength = p.tokenLength

      val what = if (hadConnector) {
        originalTokens.take(connectorStart).mkString(" ")
      } else {
        val whatTokens = tokens.take(tokens.size - parseLength)
	(if (whatTokens.lastOption.exists(_ == "in")) {
          whatTokens.dropRight(1)
        } else {
          whatTokens
        }).mkString(" ")
      }
      val where = tokens.drop(tokens.size - parseLength).mkString(" ")
      logger.ifDebug("%d sorted parses", sortedParses.size)
      logger.ifDebug("sortedParses: %s", sortedParses)

      val fmatch = p(0).fmatch
      val feature = p(0).fmatch.feature

      val shouldFetchParents =
        responseIncludes(ResponseIncludes.PARENTS) ||
        responseIncludes(ResponseIncludes.DISPLAY_NAME)

      val sortedParents = if (shouldFetchParents) {
        // we've seen dupe parents, not sure why, the toSet.toSeq fixes
        // TODO(blackmad): why dupe US parents on new york state?
        p(0).fmatch.scoringFeatures.parentIds.asScala.toSet.toSeq
          .flatMap(StoredFeatureId.fromLong _)
          .flatMap(fid => parentMap.get(fid)).sorted(GeocodeServingFeatureOrdering)
      } else {
        Nil
      }
      fixFeature(feature, sortedParents, Some(p), fillHighlightedName=parseParams.tokens.size > 0)

      val interp = new GeocodeInterpretation()
      interp.setWhat(what)
      interp.setWhere(where)
      interp.setFeature(feature)

      val scores = p.scoringFeatures
      interp.setScores(scores)

      if (req.debug > 0) {
        p.debugInfo.foreach(interp.setDebugInfo)
      }

      if (responseIncludes(ResponseIncludes.WKT_GEOMETRY) ||
          responseIncludes(ResponseIncludes.WKB_GEOMETRY)) {
        for {
          fid <- StoredFeatureId.fromLong(fmatch.longId)
          geom <- polygonMap.get(fid)
        } {
          val wkbWriter = new WKBWriter()
          feature.geometry.setWkbGeometry(wkbWriter.write(geom))
          if (responseIncludes(ResponseIncludes.WKT_GEOMETRY)) {
            val wktWriter = new WKTWriter()
            feature.geometry.setWktGeometry(wktWriter.write(geom))
          }
        }
      }

      if (responseIncludes(ResponseIncludes.PARENTS)) {
        interp.setParents(sortedParents.map(parentFeature => {
          val sortedParentParents = parentFeature.scoringFeatures.parentIds.asScala
            .flatMap(StoredFeatureId.fromLong _)
            .flatMap(parentFid => parentMap.get(parentFid)).sorted
          val feature = parentFeature.feature
          fixFeature(feature, sortedParentParents, None, fillHighlightedName=parseParams.tokens.size > 0)
          feature
        }).asJava)
      }
      interp
    })

    if (fixAmbiguousNames) {
      // Find + fix ambiguous names
      // check to see if any of our features are ambiguous, even after deduping (which
      // happens outside this function). ie, there are 3 "Cobble Hill, NY"s. Which
      // are the names we get if we only take one parent component from each.
      // Find ambiguous geocodes, tell them to take more name component
      val ambiguousInterpretationsMap: Map[String, Seq[GeocodeInterpretation]] =
        interpretations.groupBy(interp => {
          if (dedupByMatchedName) {
            interp.feature.matchedName
          } else {
            interp.feature.displayName
          }
        }).filter(_._2.size > 1)
      val ambiguousInterpretations: Iterable[GeocodeInterpretation] =
        ambiguousInterpretationsMap.flatMap(_._2).toList
      val ambiguousIdMap: Map[String, Iterable[GeocodeInterpretation]] =
        ambiguousInterpretations.groupBy(_.feature.ids.toString)

      if (ambiguousInterpretations.size > 0) {
        logger.ifDebug("had ambiguous interpretations")
        ambiguousInterpretationsMap.foreach({case (k, v) =>
          logger.ifDebug("have %d of %s", v.size, k)
        })
        sortedParses.foreach(p => {
          val fmatch = p(0).fmatch
          val feature = p(0).fmatch.feature
          ambiguousIdMap.getOrElse(feature.ids.toString, Nil).foreach(interp => {
            val sortedParents = p(0).fmatch.scoringFeatures.parentIds.asScala
              .flatMap(StoredFeatureId.fromLong _)
              .flatMap(parentFid => parentMap.get(parentFid))
              .sorted(GeocodeServingFeatureOrdering)
            fixFeature(interp.feature, sortedParents, Some(p), 1)
          })
        })
      }
    }
    interpretations
  }

  def bestNameWithMatch(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean,
    matchedStringOpt: Option[String]
  ): Option[BestNameMatch] = {
    NameUtils.bestName(f, lang, preferAbbrev, matchedStringOpt, req.debug, logger)
  }

  def filterParses(parses: SortedParseSeq, parseParams: ParseParams): SortedParseSeq = {
    if (req.debug > 0) {
      logger.ifDebug("have %d parses in filterParses", parses.size)
      parses.foreach(s => logger.ifLevelDebug(2, "examining: %s", s))
    }

    var goodParses = if (req.woeRestrict.size > 0) {
      parses.filter(p =>
        p.headOption.exists(f => req.woeRestrict.contains(f.fmatch.feature.woeType))
      )
    } else {
      parses
    }
    logger.ifDebug("have %d parses after filtering types/woes/restricts", goodParses.size)

    goodParses = goodParses.filterNot(p => {
      p.headOption.exists(f =>
        StoredFeatureId.fromLong(f.fmatch.longId).exists(fid => store.hotfixesDeletes.has(fid)))
    })

    logger.ifDebug("have %d parses after filtering from delete hotfixes", goodParses.size)

    goodParses = goodParses.filter(p => {
      val parseLength = p.tokenLength
        parseLength == parseParams.tokens.size || parseLength != 1 ||
          p.headOption.exists(m => {
            m.fmatch.scoringFeatures.population > 50000 || p.length > 1
          })
    })
    logger.ifDebug("have %d parses after removeLowRankingParses", goodParses.size)

    goodParses = goodParses.filter(p => p.headOption.exists(m => m.fmatch.scoringFeatures.canGeocode))

    if (req.isSetAllowedSources()) {
      val allowedSources = req.allowedSources.asScala
      goodParses = goodParses.filter(p =>
        p.headOption.exists(_.fmatch.feature.ids.asScala.exists(i => allowedSources.has(i.source)))
      )
    }

    goodParses
  }

  def buildFinalParses(
    parses: SortedParseSeq,
    parseParams: ParseParams,
    originalMaxInterpretations: Int,
    dedupByMatchedName: Boolean = false
  ) = {
    val tokens = parseParams.tokens
    val originalTokens = parseParams.originalTokens
    val connectorStart = parseParams.connectorStart
    val connectorEnd = parseParams.connectorEnd

    // filter out parses that are really the same feature
    // this code is gross gross gross

    // build a map from
    // primary feature id -> list of parses containing that id, sorted by
    val parsesByMainId: Map[Long, Seq[SortedParseWithPosition]] = parses.zipWithIndex.map({
      case (parse, index) => SortedParseWithPosition(parse, index)
    }).groupBy(_.parse.headOption.map(_.fmatch.longId).getOrElse(-1L)).mapValues(parses => {
      parses.sortBy(p => {
        // prefer interpretations that are shorter and don't have reused features
        val dupeWeight = if (p.parse.hasDupeFeature) { 10 } else { 0 }
        p.parse.size + dupeWeight - p.parse.tokenLength*1000
      })
    })

    val actualParses =
      parses.zipWithIndex.filter({case (p, index) => {
        (for {
          primaryFeature <- p.headOption
          parses: Seq[SortedParseWithPosition] <- parsesByMainId.get(primaryFeature.fmatch.longId)
          bestParse <- parses.headOption
        } yield {
          bestParse.position == index
        }).getOrElse(false)
      }}).map(_._1)

    val filteredParses = filterParses(actualParses, parseParams)

    val maxInterpretations = if (originalMaxInterpretations == 0) {
      filteredParses.size
    } else {
      originalMaxInterpretations
    }

    val dedupedParses = if (maxInterpretations >= 1) {
      dedupeParses(filteredParses.take(maxInterpretations * 2)).take(maxInterpretations)
    } else {
      dedupeParses(filteredParses.take(maxInterpretations))
    }

    if (req.debug > 0) {
      logger.ifDebug("%d parses after deduping", dedupedParses.size)
      dedupedParses.zipWithIndex.foreach({case (parse, index) =>
        logger.ifDebug("deduped parse ids: %s", parse.map(_.fmatch.longId))
      })
    }

    // TODO: make this configurable
    // val sortedDedupedParses: SortedParseSeq = dedupedParses.sorted(new ParseOrdering).take(3)
    val sortedDedupedParses: SortedParseSeq = dedupedParses.take(maxInterpretations)
    val polygonMap: Map[StoredFeatureId, Geometry] = if (GeocodeRequestUtils.shouldFetchPolygon(req)) {
      store.getPolygonByFeatureIds(sortedDedupedParses.flatMap(p =>
        StoredFeatureId.fromLong(p(0).fmatch.longId)))
    } else {
      Map.empty
    }
    ResponseProcessor.generateResponse(req.debug, logger, hydrateParses(
      sortedDedupedParses, parseParams, polygonMap, fixAmbiguousNames = true, dedupByMatchedName = dedupByMatchedName))
  }
}
