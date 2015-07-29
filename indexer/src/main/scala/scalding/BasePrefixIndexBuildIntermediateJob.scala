// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes._
import com.foursquare.twofishes.output.PrefixIndexer
import com.foursquare.twofishes.util.NameNormalizer
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import org.apache.hadoop.io.{LongWritable, Text}

case class PrefixEntry(
  isFull: Boolean,
  score: Int,
  id: Long,
  woeType: YahooWoeType,
  cc: String,
  name: FeatureName
) {
  val bestWoeTypes = Set(
    YahooWoeType.POSTAL_CODE,
    YahooWoeType.TOWN,
    YahooWoeType.SUBURB,
    YahooWoeType.ADMIN3,
    YahooWoeType.AIRPORT,
    YahooWoeType.COUNTRY
  )

  def isAllowedWoeType(): Boolean = bestWoeTypes.contains(woeType)
}

class BasePrefixIndexBuildIntermediateJob(
  name: String,
  sources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](sources).group

  def isAllDigits(x: String) = x forall Character.isDigit

  def shouldExcludeFromPrefixIndex(name: FeatureName, woeType: YahooWoeType): Boolean = {
    // exclude because of flags
    name.flags.exists(flag => (flag == FeatureNameFlags.NEVER_DISPLAY) || flag == FeatureNameFlags.LOW_QUALITY) ||
    // exclude purely numeric names of non-postalcode features
    woeType == YahooWoeType.POSTAL_CODE && isAllDigits(name.name)
  }



  def joinAndSortLists(lists: List[PrefixEntry]*): List[PrefixEntry] = {
    lists.toList.flatMap(l => {
      l.sortBy(_.score)
    })
  }

  def sortRecordsByNames(entries: List[PrefixEntry]) = {
    val (prefPureNames, nonPrefPureNames) = entries.partition(e =>
        e.name.flags.exists(flag => flag == FeatureNameFlags.PREFERRED || flag == FeatureNameFlags.ALT_NAME) &&
        (e.name.lang == "en" || e.name.flags.contains(FeatureNameFlags.LOCAL_LANG)))

    val (secondBestNames, worstNames) = nonPrefPureNames.partition(e =>
        e.name.lang == "en" || e.name.flags.contains(FeatureNameFlags.LOCAL_LANG))

    (joinAndSortLists(prefPureNames), joinAndSortLists(secondBestNames, worstNames))
  }

  def roundRobinByCountryCode(entries: List[PrefixEntry]): List[PrefixEntry] = {
    // to ensure global distribution of features from all countries, group by cc
    // and then pick the top from each group by turn and cycle through
    // input: a (US), b (US), c (CN), d (US), e (AU), f (AU), g (CN)
    // desired output: a (US), c (CN), e (AU), b (US), g (CN), f (AU), d (US)
    entries.groupBy(_.cc)                   // (US -> a, b, d), (CN -> c, g), (AU -> e, f)
      .values.toList                        // (a, b, d), (c, g), (e, f)
      .flatMap(_.zipWithIndex)              // (a, 0), (b, 1), (d, 2), (c, 0), (g, 1), (e, 0), (f, 1)
      .groupBy(_._2).toList                 // (0 -> a, c, e), (1 -> b, g, f), (2 -> d)
      .sortBy(_._1).flatMap(_._2.map(_._1)) // a, c, e, b, g, f, d
  }

  (for {
    (featureId, servingFeature) <- features
    population = servingFeature.scoringFeatures.populationOption.getOrElse(0)
    boost = servingFeature.scoringFeatures.boostOption.getOrElse(0)
    score = population + boost
    cc = servingFeature.feature.cc
    woeType = servingFeature.feature.woeTypeOrDefault
    // unlike with the name index, we can't just choose distinct normalized names up front because we need
    // to know which name each prefix came from
    // as a result different names might normalize to the same string, and separately generate the same prefixes
    // so we will need to dedupe longIds in the reducer instead
    name <- servingFeature.feature.names
    shouldExclude = shouldExcludeFromPrefixIndex(name, woeType)
    normalizedName = NameNormalizer.normalize(name.name)
    // filter out any name that must be excluded unless it's short enough to make it into the prefix index
    // as a full name match, in which case, let it through for now and don't generate prefixes for it
    if (!shouldExclude || normalizedName.length <= PrefixIndexer.MaxPrefixLength)
    fromLength = if (!shouldExclude) { 1 } else { normalizedName.length }
    toLength = math.min(PrefixIndexer.MaxPrefixLength, normalizedName.length)
    length <- fromLength to toLength
    prefix = normalizedName.substring(0, length)
    if prefix.nonEmpty
    isFull = (length == normalizedName.length)
    // to prevent too many results for short prefixes, use score threshold
    if (isFull || (length > 2) || (length <= 2 && score > 0))
  } yield {
    (new Text(prefix) -> PrefixEntry(isFull, score * -1, featureId.get, woeType, cc, name))
  }).group
    .withReducers(1)
    .toList
    .mapValues({values: List[PrefixEntry] => {
      val filtered = values
        .sortBy({case entry: PrefixEntry => (entry.isFull, entry.score, entry.id)})
        .take(PrefixIndexer.MaxNamesToConsider)

      val woeMatches = filtered.filter({case entry: PrefixEntry => entry.isAllowedWoeType})

      val (prefSortedRecords, unprefSortedRecords) = sortRecordsByNames(woeMatches)

      //val preferredIds = roundRobinByCountryCode(prefSortedRecords).map(_.id).distinct.take(PrefixIndexer.MaxFidsToStorePerPrefix)
      val preferredIds = prefSortedRecords.map(_.id).distinct.take(PrefixIndexer.MaxFidsToStorePerPrefix)
      val nonPreferredIds = if (preferredIds.size < PrefixIndexer.MaxFidsWithPreferredNamesBeforeConsideringNonPreferred) {
        //roundRobinByCountryCode(unprefSortedRecords).map(_.id).distinct.take(PrefixIndexer.MaxFidsToStorePerPrefix - preferredIds.size)
        unprefSortedRecords.map(_.id).distinct.take(PrefixIndexer.MaxFidsToStorePerPrefix - preferredIds.size)
      } else {
        Nil
      }

      IntermediateDataContainer.newBuilder.longList(preferredIds ++ nonPreferredIds).result
    }})
    .filter({case (k: Text, v: IntermediateDataContainer) => v.longList.nonEmpty})
    .write(TypedSink[(Text, IntermediateDataContainer)](SpindleSequenceFileSource[Text, IntermediateDataContainer](outputPath)))
}
