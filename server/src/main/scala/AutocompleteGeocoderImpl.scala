//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.geo.country.DependentCountryInfo
import com.foursquare.twofishes.AutocompleteBias.UnknownWireValue
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.{GeoTools, GeonamesId, NameNormalizer, NameUtils, StoredFeatureId}
import com.foursquare.twofishes.util.Lists.Implicits._
import com.foursquare.twofishes.util.NameUtils.BestNameMatch
import scala.collection.mutable.HashMap
import scalaj.collection.Implicits._

trait GeocoderUtils {
  def req: GeocodeRequest

  lazy val requestGeom = GeocodeRequestUtils.getRequestGeometry(req)

  def isAcceptableFeature(req: GeocodeRequest, servingFeature: GeocodeServingFeature): Boolean = {
    !req.strict ||
      (
      // bounds or ll+radius contains the center
      requestGeom.isEmptyOr(g =>
        g.contains(GeoTools.pointToGeometry(servingFeature.feature.geometry.center))) &&
      req.ccOption.isEmptyOr(_ =? servingFeature.feature.cc)
      )
  }
}

class AutocompleteGeocoderImpl(
  store: GeocodeStorageReadService,
  val req: GeocodeRequest,
  logger: MemoryLogger
) extends AbstractGeocoderImpl[GeocodeResponse] with GeocoderUtils {
  val query = req.queryOption.getOrElse("")
  val parseParams = new QueryParser(logger).parseQuery(query)

  override val implName = "autocomplete"

  val commonParams = GeocodeRequestUtils.geocodeRequestToCommonRequestParams(req)
  val responseProcessor = new ResponseProcessor(
    commonParams,
    store,
    logger,
    pickBestNamesForAutocomplete = true)

  // Another delightful hack. We don't save a pointer to the specific name we matched
  // in our inverted index, instead, if we know which tokens matched this feature,
  // we look for the name that, when normalized, matches the query substring.
  // i.e. for [New York, NY, Nueva York], if we know that we matched "neuv", we
  // look for the names that started with that.
  var nameMatchMap =
    new scala.collection.mutable.HashMap[String, Option[BestNameMatch]]

  def bestNameWithMatch(
    f: GeocodeFeature,
    lang: Option[String],
    preferAbbrev: Boolean,
    matchedStringOpt: Option[String]
  ): Option[BestNameMatch] = {
    val hashKey = "%s:%s:%s:%s".format(f.ids, lang, preferAbbrev, matchedStringOpt)
    if (!nameMatchMap.contains(hashKey)) {
      nameMatchMap(hashKey) = NameUtils.bestName(f, lang, preferAbbrev, matchedStringOpt, req.debug, logger)
    }
    nameMatchMap(hashKey)
  }

  // Yet another huge hack because I don't know what name I hit
  def filterNonPrefExactAutocompleteMatch(ids: Seq[StoredFeatureId], phrase: String): Seq[StoredFeatureId] = {
    store.getByFeatureIds(ids).filter(f => {
      f._2.feature.woeTypeOption.exists(_ =? YahooWoeType.POSTAL_CODE) ||
      {
        val nameMatch = bestNameWithMatch(f._2.feature, Some(req.lang), false, Some(phrase))
        nameMatch.exists(nm =>
          nm._1.flags.contains(FeatureNameFlags.PREFERRED) ||
          nm._1.flags.contains(FeatureNameFlags.ALT_NAME)
        )
      }
    }).toList.map(_._1)
  }

  /**** AUTOCOMPLETION LOGIC ****/
  /*
   In an effort to make autocomplete much faster, we throw out a number of the
   features and hacks of the primary matching logic. We assume that the query
   is being entered right-to-left, smallest-to-biggest. We also skip out on most
   of the zip-code hacks. Since zip-codes aren't really in the political hierarchy,
   they don't have all the parents one might expect them to. In the full scorer, we
   need to hydrate the full features for our name hits so that we can check the
   feature type and compare the distances as a hack for zip-code containment.

   In this scorer, we assume the left-most strings that we match are matched to the
   smallest feature, and then we don't need to hydrate any future matches to determine
   validity, we only need to check whether the matched ids occur in the parents of
   that smallest feature

   This means that some logic is duplicated. sorry.

   */
  def buildValidAutocompleteParses(
      parses: ParseSeq,
      matches: Seq[FeatureMatch],
      offset: Int,
      i: Int,
      matchString: String): ParseSeq = {
    if (parses.size == 0) {
      logger.ifDebug("parses == 0, so accepting everything")
      matches.map(m =>
        Parse[Unsorted](List(m))
      ).toList
    } else {
      parses.flatMap(parse => {
        if (req.debug > 0) {
          logger.ifDebug("checking %d fids against %s", matches.size, parse.map(_.fmatch.longId))
          logger.ifDebug("these are the fids of my parse: %s", matches.map(_.fmatch.longId))
        }

        // prevent matching of feature names in random combinations of languages
        // Always allow names in English and the user's language, and abbreviations
        // If the parse so far has tokens that match names in multiple languages,
        // keep only those languages which have PREFERRED names if possible
        val preferredNameLanguages = parse.headOption.toList.flatMap(
          _.possibleNameHits.filter(_.flags.contains(FeatureNameFlags.PREFERRED))
          .map(_.lang))

        val allowedLanguages =
          Set("en", "abbr") ++ Set(req.lang) ++
          (if (preferredNameLanguages.nonEmpty) {
            preferredNameLanguages
          } else {
            parse.headOption.toList.flatMap(_.possibleNameHits.map(_.lang))
          }).toSet

        matches.flatMap(featureMatch => {
          val fid = featureMatch.fmatch.longId
          val fcc = featureMatch.fmatch.feature.cc
          if (req.debug > 0) {
            logger.ifDebug("checking if %s is an unused parent of %s",
              fid, parse.map(_.fmatch.longId))
          }

          val featureIsParent = parse.exists(_.fmatch.scoringFeatures.parentIds.has(fid))
          val featureIsExtraRelation = parse.exists(_.fmatch.scoringFeatures.extraRelationIds.has(fid))
          val featureHasDependentCountryRelation =
            featureMatch.fmatch.feature.woeType == YahooWoeType.COUNTRY &&
            parse.exists(p =>
              DependentCountryInfo.isCountryDependentOnCountry(p.fmatch.feature.cc, fcc))

          val featureIsNotRepeat = !parse.exists(_.fmatch.longId.toString == fid)
          val featureNameHitsInAllowedLanguage =
            featureMatch.possibleNameHits.exists(n => allowedLanguages.has(n.lang))

          val isValid = (featureIsParent || featureIsExtraRelation || featureHasDependentCountryRelation) &&
             featureIsNotRepeat && featureNameHitsInAllowedLanguage

          if (isValid) {
            if (req.debug > 0) {
              logger.ifDebug("HAD %s as a parent", fid)
            }
            Some(parse.addFeature(featureMatch))
          } else {
           // logger.ifDebug("wasn't")
           None
          }
        })
      })
    }
  }

  def matchName(name: FeatureName, query: String, isEnd: Boolean): Boolean = {
    if (name.flags.contains(FeatureNameFlags.PREFERRED) ||
        name.flags.contains(FeatureNameFlags.ABBREVIATION) ||
        name.flags.contains(FeatureNameFlags.LOCAL_LANG) ||
        name.flags.contains(FeatureNameFlags.ALT_NAME) ||
        name.flags.contains(FeatureNameFlags.DEACCENT) ||
        name.flags.contains(FeatureNameFlags.HISTORIC) ||
        name.flags.contains(FeatureNameFlags.SHORT_NAME) ||
        name.flags.contains(FeatureNameFlags.ALIAS) ||
        // a lot of aliases tend to be names without a language
        name.lang.isEmpty)
    {
      val normalizedName = NameNormalizer.normalize(name.name)
      if (isEnd) {
        normalizedName.startsWith(query)
      } else {
        normalizedName == query
      }
    } else {
      false
    }
  }

  def generateAutoParsesHelper(tokens: List[String], offset: Int, parses: ParseSeq, spaceAtEnd: Boolean): ParseSeq = {
    if (tokens.size == 0) {
      parses
    } else {
      val validParses: Seq[ParseSeq] = 1.to(tokens.size).map(i => {
        val query = tokens.take(i).mkString(" ")
        val isEnd = (i == tokens.size)

        val possibleParents = (for {
          parse <- parses
          parseFeature <- parse
          featureParentId <- parseFeature.fmatch.scoringFeatures.parentIds
        } yield {
          StoredFeatureId.fromLong(featureParentId)
        }).flatten

        val featuresMatches: Seq[FeatureMatch] =
          if (parses.size == 0) {
            val featureIds = if (isEnd) {
              logger.ifDebug("looking at prefix: %s", query)
              if (spaceAtEnd) {
                List(
                  store.getIdsByNamePrefix(query + " "),
                  filterNonPrefExactAutocompleteMatch(store.getIdsByName(query), query)
                ).flatten
              } else {
                store.getIdsByNamePrefix(query)
              }
            } else {
              store.getIdsByName(query)
            }

            store.getByFeatureIds(featureIds)
              .filter({case (oid, servingFeature) => isAcceptableFeature(req, servingFeature)})
              .map({case (oid, servingFeature) => {
                FeatureMatch(offset, offset + i, query, servingFeature,
                  servingFeature.feature.names.filter(n => matchName(n, query, isEnd)))
              }})
            .filter(featureMatch => featureMatch.possibleNameHits.nonEmpty)
            .toSeq
          } else {
            val parents = store.getByFeatureIds(possibleParents).toSeq
            val countriesOnWhichParentsAreDependent =
              parents.map(_._2.feature.cc)
              .toList
              .distinct
              .flatMap(dcc => DependentCountryInfo.getCountryIdOnWhichCountryIsDependent(dcc).map(id => GeonamesId(id)))

            val augmentedParents = parents ++ store.getByFeatureIds(countriesOnWhichParentsAreDependent).toSeq
            logger.ifDebug("looking for %s in parents: %s", query, parents)
            for {
              (oid, servingFeature) <- augmentedParents
              names = servingFeature.feature.names.filter(n => matchName(n, query, isEnd))
              if names.nonEmpty
            } yield {
              FeatureMatch(offset, offset + i, query, servingFeature, names)
            }
          }

        if (req.debug > 0) {
          logger.ifDebug("%d-%d: looking at: %s (is end? %s)", offset, i, query, isEnd)
          logger.ifDebug("%d-%d: %d previous parses: %s", offset, i, parses.size,   parses)
          logger.ifDebug("%d-%d: examining %d featureIds against parse", offset, i, featuresMatches.size)
        }

        val nextParses: ParseSeq = buildValidAutocompleteParses(parses, featuresMatches, offset, i, query)

        if (nextParses.size == 0) {
          List(Parse[Unsorted](Nil))
        } else {
          generateAutoParsesHelper(tokens.drop(i), offset + i, nextParses, spaceAtEnd)
        }
      })
      validParses.flatten
    }
  }

  def generateAutoParses(tokens: List[String], spaceAtEnd: Boolean): SortedParseSeq = {
    // The getSorted is redundant here because the isValid function for autocomplete
    // parses enforces smallest-to-largest ordering, but we can't prove that to the compiler
    try {
      generateAutoParsesHelper(tokens, 0, Nil, spaceAtEnd).map(_.getSorted)
    } catch {
      case e: TooManyResultsException => {
        logger.ifDebug(e.getMessage)
        Vector.empty
      }
    }
  }

  val maxInterpretations = {
    // TODO: remove once clients are filling this
    if (req.maxInterpretations <= 0) {
      3
    } else {
      req.maxInterpretations
    }
  }

  def doGeocodeImpl(): GeocodeResponse = {
    val parses = generateAutoParses(parseParams.tokens, parseParams.spaceAtEnd)
    if (req.debug > 0) {
      parses.foreach(p => {
        logger.ifDebug("parse ids: %s", p.map(_.fmatch.longId))
      })
    }

    val validParses = parses
      .filterNot(p =>
        p.headOption.isEmptyOr(f =>
          (f.fmatch.feature.woeType == YahooWoeType.ADMIN1 ||
           f.fmatch.feature.woeType == YahooWoeType.CONTINENT ||
           f.fmatch.feature.woeType == YahooWoeType.COUNTRY) &&
          (!commonParams.woeRestrict.has(f.fmatch.feature.woeType))
        )
      )

    val sortedParses = (req.autocompleteBiasOrDefault match {
      case AutocompleteBias.NONE => validParses.sorted(
        new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForAutocompleteDefault, "default"))

      case AutocompleteBias.BALANCED | AutocompleteBias.UnknownWireValue(_) => {
        val globalRelevanceCutoff = 1000000
        val defaultCutoff = 0

        val worldCityRanker = new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForAutocompleteWorldCityBias, "worldCity")
        val worldCities = OrderedParseGroup(worldCityRanker, Some(defaultCutoff), Some(1))

        val strictLocalRanker = new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForAutocompleteStrictLocal, "strictLocal")
        val locallyRelevant = OrderedParseGroup(strictLocalRanker, Some(defaultCutoff), Some(1))

        val inCountryGlobalRanker = new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForAutocompleteStrictInCountryGlobal, "inCountryGlobal")
        val inCountryGloballyRelevant = OrderedParseGroup(inCountryGlobalRanker, Some(globalRelevanceCutoff), Some(1))

        val globalBiasRanker = new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForAutocompleteGlobalBias, "globalBias")
        val globallyRelevant = OrderedParseGroup(globalBiasRanker, Some(globalRelevanceCutoff), Some(1))

        val localBiasRanker = new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForAutocompleteLocalBias, "localBias")
        val inCountry = OrderedParseGroup(localBiasRanker, Some(defaultCutoff), Some(1))

        val rest = OrderedParseGroup(globalBiasRanker, None, None)

        val merger = new OrderedParseGroupMerger(
          validParses,
          Seq(
            worldCities,
            locallyRelevant,
            inCountryGloballyRelevant,
            globallyRelevant,
            inCountry,
            rest))

        merger.merge()
      }

      case AutocompleteBias.LOCAL => validParses.sorted(
        new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForAutocompleteLocalBias, "localBias"))

      case AutocompleteBias.GLOBAL => validParses.sorted(
        new GeocodeParseOrdering(commonParams, logger, GeocodeParseOrdering.scorersForAutocompleteGlobalBias, "globalBias"))
    }).toSeq


    responseProcessor.buildFinalParses(
      GeocodeParseOrdering.maybeReplaceTopResultWithRelatedCity(sortedParses),
      parseParams,
      maxInterpretations,
      requestGeom,
      dedupByMatchedName = true)
  }
}
