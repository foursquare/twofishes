// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import com.foursquare.twofishes._
import org.apache.hadoop.io.LongWritable
import com.twitter.scalding.typed.TypedSink
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.util.Lists.Implicits._
import com.foursquare.twofishes.util.NameNormalizer

class BaseAlternateNamesJoinIntermediateJob(
  name: String,
  featureSources: Seq[String],
  altNameSources: Seq[String],
  args: Args
) extends TwofishesIntermediateJob(name, args) {

  val features = getJobOutputsAsTypedPipe[LongWritable, GeocodeServingFeature](featureSources).group
  val altNames = getJobOutputsAsTypedPipe[LongWritable, FeatureNames](altNameSources).group

  val countryInfoCachedFile = getCachedFileByRelativePath("downloaded/countryInfo.txt")
  @transient lazy val countryNameMap = InMemoryLookupTableHelper.buildCountryNameMap(countryInfoCachedFile)
  @transient lazy val countryLangMap = InMemoryLookupTableHelper.buildCountryLangsMap(countryInfoCachedFile)

  val customRewritesCachedFile = getCachedFileByRelativePath("custom/rewrites.txt")
  @transient lazy val nameRewritesMap = InMemoryLookupTableHelper.buildNameRewritesMap(Seq(customRewritesCachedFile))

  val customShortensCachedFile = getCachedFileByRelativePath("custom/shortens.txt")
  @transient lazy val nameShortensMap = InMemoryLookupTableHelper.buildNameShortensMap(Seq(customShortensCachedFile))

  val customDeletesCachedFile = getCachedFileByRelativePath("custom/deletes.txt")
  @transient lazy val nameDeletesList = InMemoryLookupTableHelper.buildNameDeletesList(Seq(customDeletesCachedFile))

  def processFeatureName(
    cc: String,
    woeType: YahooWoeType,
    lang: String,
    name: String,
    flags: Seq[FeatureNameFlags]
  ): Seq[FeatureName] = {
    if (lang != "post") {

      val spaceRe = " +".r
      def fixName(s: String) = spaceRe.replaceAllIn(s, " ").trim

      // TODO(rahul): actually use flags
      def doShorten(): Seq[String] = {
        val shortens = nameShortensMap.getOrElse("*", Nil) ++
          nameShortensMap.getOrElse(cc, Nil)

        val candidates: Seq[String] = shortens.flatMap(shorten => {
          val newName = shorten.from.replaceAllIn(name, shorten.to)
          if (newName != name) {
            Some(fixName(newName))
          } else {
            None
          }
        })

        candidates.sortBy(_.length).headOption.toSeq
      }

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

      def hackName(): Seq[String] = {
        // HACK(blackmad): TODO(blackmad): move these to data files
        if (woeType == YahooWoeType.ADMIN1 && cc == "JP" && (lang == "en" || lang == "")) {
          Seq(name + " Prefecture")
        } else if (woeType == YahooWoeType.TOWN && cc == "TW" && (lang == "en" || lang == "")) {
          Seq(name + " County")
          // Region Lima -> Lima Region
        } else if (woeType == YahooWoeType.ADMIN1 && cc == "PE" && name.startsWith("Region")) {
          Seq(name.replace("Region", "").trim + " Region")
        } else {
          Nil
        }
      }

      def rewriteNames(names: Seq[String]): (Seq[String], Seq[String]) = {
        val deleteModifiedNames: Seq[String] = names.flatMap(doDelete)

        val deaccentedNames = names.map(NameNormalizer.deaccent).filterNot(n =>
          names.contains(n))

        val rewrittenNames = doRewrites(names ++ deleteModifiedNames).filterNot(n =>
          names.contains(n))

        (deaccentedNames, (deleteModifiedNames ++ rewrittenNames).distinct)
      }

      def isLocalLang() =
        countryLangMap.getOrElse(cc, Nil).contains(lang)

      def buildFeatureName(name: String, flags: Seq[FeatureNameFlags]) = {
        FeatureName.newBuilder
          .lang(lang)
          .name(name)
          .flags(flags)
          .result
      }

      def processNameList(names: Seq[String], originalFlags: Seq[FeatureNameFlags]): Seq[FeatureName] = {
        names.map(n => {
          val finalFlags = originalFlags ++
            (if (isLocalLang()) {
              Seq(FeatureNameFlags.LOCAL_LANG)
            } else {
              Nil
            })
          buildFeatureName(n, finalFlags)
        })
      }

      val originalNames = Seq(name)
      val hackedNames = hackName()
      val (deaccentedNames, allModifiedNames) = rewriteNames(originalNames)
      val shortenedNames = doShorten()

      processNameList(originalNames, flags) ++
        processNameList(shortenedNames, flags :+ FeatureNameFlags.SHORT_NAME) ++
        processNameList(deaccentedNames, flags :+ FeatureNameFlags.DEACCENT) ++
        processNameList(allModifiedNames, flags :+ FeatureNameFlags.ALIAS) ++
        processNameList(hackedNames, flags :+ FeatureNameFlags.ALIAS)
    } else {
      Nil
    }
  }

  def processFeatureName(
    cc: String,
    woeType: YahooWoeType,
    featureName: FeatureName
  ): Seq[FeatureName] = {
    val fixedFlags = if (featureName.lang == "abbr") {
      Seq(FeatureNameFlags.ABBREVIATION) ++ featureName.flags.filterNot(_ == FeatureNameFlags.ABBREVIATION)
    } else {
      featureName.flags
    }

    processFeatureName(cc, woeType, featureName.lang, featureName.name, fixedFlags)
  }

  def applyOneOffFixes(featureNames: Seq[FeatureName], cc: String): Seq[FeatureName] = {
    def fixRomanianName(s: String) = {
      val romanianTranslationTable = List(
        // cedilla -> comma
        "Ţ" -> "Ț",
        "Ş" -> "Ș",
        // tilde and caron to breve
        "Ã" -> "Ă",
        "Ǎ" -> "Ă"
      ).flatMap({case (from, to) => {
        List(
          from.toLowerCase -> to.toLowerCase,
          from.toUpperCase -> to.toUpperCase
        )
      }})

      var newS = s
      romanianTranslationTable.foreach({case (from, to) => {
        newS = newS.replace(from, to)
      }})
      newS
    }

    var fixedNames = featureNames.map(n => {
      if (n.lang == "ro") {
        n.copy(name = fixRomanianName(n.name))
      } else {
        n
      }
    })

    // Lately geonames has these stupid JP aliases, like "Katsuura Gun" for "Katsuura-gun"
    if (cc == "JP" || cc == "TH") {
      def isPossiblyBad(s: String): Boolean = {
        s.contains(" ") && s.split(" ").forall(_.headOption.exists(Character.isUpperCase))
      }

      def makeBetterName(s: String): String = {
        val parts = s.split(" ")
        val head = parts.headOption
        val rest = parts.drop(1)
        (head.toList ++ rest.map(_.toLowerCase)).mkString("-")
      }

      val enNames = fixedNames.filter(_.lang == "en")
      enNames.foreach(n => {
        if (isPossiblyBad(n.name)) {
          fixedNames = fixedNames.filterNot(_.name == makeBetterName(n.name))
        }
      })
    }

    fixedNames
  }

  val joined = features.leftJoin(altNames)
    .map({case (k: LongWritable, (f: GeocodeServingFeature, altNamesOpt: Option[FeatureNames])) => {
      altNamesOpt match {
        case Some(altNamesContainer) => {
          val woeType = f.feature.woeTypeOption.getOrElse(YahooWoeType.UNKNOWN)
          val cc = f.feature.cc

          val altNames = altNamesContainer.names
          val featureNames = f.feature.names

          // primary name on feature was added as english preferred
          val primaryName = featureNames.find(featureName =>
            featureName.lang == "en" && featureName.flags.contains(FeatureNameFlags.PREFERRED)
          ).get

          // start by unconditionally adding non-primary names from feature
          var finalNames = featureNames.filterNot(_ == primaryName)

          // next, if this is a country, add its english name as preferred colloquial
          if (woeType == YahooWoeType.COUNTRY) {
            countryNameMap.get(cc).foreach(name =>
              finalNames ++= processFeatureName(
                cc,
                woeType,
                lang = "en",
                name = name,
                flags = Seq(FeatureNameFlags.PREFERRED, FeatureNameFlags.COLLOQUIAL))
            )
          }

          val preferredEnglishAltName = altNames.find(altName =>
            altName.lang == "en" && altName.flags.contains(FeatureNameFlags.PREFERRED)
          )
          val hasEnglishAltName = altNames.exists(_.lang == "en")
          val hasPreferredEnglishAltName = preferredEnglishAltName.isDefined
          val hasNonPreferredEnglishAltNameIdenticalToFeatureName = altNames.exists(altName =>
            altName.lang == "en" && !altName.flags.contains(FeatureNameFlags.PREFERRED) && altName.name == primaryName.name
          )

          // consider using the primary feature name from geonames as an english name:
          // skip: if an identical preferred english alt name exists
          // add as preferred:
          //    if no english alt name exists OR
          //    no preferred english alt name exists BUT an identical non-preferred english name exists
          // add as non-preferred otherwise
          if (!preferredEnglishAltName.exists(_.name == primaryName.name)) {
            finalNames ++= processFeatureName(
              cc,
              woeType,
              lang = "en",
              name = primaryName.name,
              flags = if (!hasEnglishAltName || (!hasPreferredEnglishAltName && hasNonPreferredEnglishAltNameIdenticalToFeatureName)) {
                Seq(FeatureNameFlags.PREFERRED)
              } else {
                Nil
              })
          }

          // process and add alternate names
          val processedAltNames = altNames.flatMap(altName => {
            processFeatureName(cc, woeType, altName)
          })
          val (deaccentedFeatureNames, nonDeaccentedFeatureNames) = processedAltNames.partition(n => n.flags.contains(FeatureNameFlags.DEACCENT))
          val nonDeaccentedNames: Set[String] = nonDeaccentedFeatureNames.map(_.name).toSet
          finalNames ++= nonDeaccentedFeatureNames
          finalNames ++= deaccentedFeatureNames.filterNot(n => nonDeaccentedNames.contains(n.name))

          // apply all one-off language/country specific fixes
          finalNames = applyOneOffFixes(finalNames, cc)

          // merge flags of dupes in same language
          finalNames  = finalNames.groupBy(n => (n.lang, n.name)).toSeq
            .map({case ((lang, name), featureNames) => {
              val combinedFlags = featureNames.map(_.flags).flatten.distinct
              // If we collapsed multiple names, and not all of them had ALIAS,
              // then we should strip off that flag because some other entry told
              // us it didn't deserve to be ranked down
              val finalFlags =
                if (featureNames.size > 1 &&
                    featureNames.exists(n => !n.flags.has(FeatureNameFlags.ALIAS))) {
                  combinedFlags.filterNot(_ == FeatureNameFlags.ALIAS)
                } else {
                  combinedFlags
                }
              FeatureName.newBuilder
                .lang(lang)
                .name(name)
                .flags(finalFlags)
                .result
            }})

          (k -> f.copy(feature = f.feature.copy(names = finalNames)))
        }
        case None =>
          (k -> f)
      }}
  })

  joined.write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](outputPath)))
}
