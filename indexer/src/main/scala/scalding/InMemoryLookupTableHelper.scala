// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding.filecache.CachedFile
import scala.io.Source
import com.foursquare.twofishes.importers.geonames.ShortenInfo
import com.foursquare.twofishes.FeatureNameFlags
import scala.util.matching.Regex

object InMemoryLookupTableHelper {

  def buildAdminCodeMap(countryInfoFile: CachedFile, adminCodesFile: CachedFile): Map[String, String] = {

    val countryInfoSource = Source.fromFile(countryInfoFile.file)
    val countries = countryInfoSource.getLines.filterNot(_.startsWith("#")).map(l => {
      val parts = l.split("\t")
      val cc = parts(0)
      val geonameid = parts(16)
      (cc -> geonameid)
    })

    val adminCodesSource = Source.fromFile(adminCodesFile.file)
    val admins = adminCodesSource.getLines.filterNot(_.startsWith("#")).map(l => {
      val parts = l.split("\t")
      val admCode = parts(0)
      val geonameid = parts(3)
      (admCode -> geonameid)
    })

    (countries ++ admins).toMap
  }

  def buildCountryNameMap(countryInfoFile: CachedFile): Map[String, String] = {

    Source.fromFile(countryInfoFile.file).getLines.filterNot(_.startsWith("#")).map(l => {
      val parts = l.split("\t")
      val cc = parts(0)
      val englishName = parts(4)
      (cc -> englishName)
    }).toMap
  }

  def buildCountryLangsMap(countryInfoFile: CachedFile): Map[String, Seq[String]] = {

    Source.fromFile(countryInfoFile.file).getLines.filterNot(_.startsWith("#")).map(l => {
      val parts = l.split("\t")
      val cc = parts(0)
      val langs = parts(15).split(",").map(l => l.split("-")(0)).toSeq
      (cc -> langs)
    }).toMap
  }

  def buildNameRewritesMap(cachedFiles: Seq[CachedFile]): Map[Regex, Seq[String]] = {

    (for {
      cachedFile <- cachedFiles
      source = Source.fromFile(cachedFile.file)
      line <- source.getLines.filterNot(_.startsWith("#"))
    } yield {
      val parts = line.split("\\|")
      val key = parts(0)
      val values = parts(1).split(",").toSeq
      (key.r -> values)
    }).toMap
  }

  def buildNameShortensMap(cachedFiles: Seq[CachedFile]): Map[String, Seq[ShortenInfo]] = {

    def buildFlagsMask(flagsOpt: Option[String]): Int = {
      val flagsList = if (flagsOpt.isEmpty) {
        Nil
      } else {
        flagsOpt.getOrElse("").split(",").flatMap(f => FeatureNameFlags.unapply(f)).toSeq
      }

      var flagsMask = 0
      flagsList.foreach(f => flagsMask = flagsMask | f.getValue())
      flagsMask
    }

    (for {
      cachedFile <- cachedFiles
      source = Source.fromFile(cachedFile.file)
      line <- source.getLines.filterNot(_.startsWith("#"))
      parts = line.split("[\\|\t]").toList
      countries = parts(0).split(",").toList
      shortenParts = parts.drop(1)
      toShortenFrom = shortenParts(0) + "\\b"
      toShortenTo = shortenParts.lift(1).getOrElse("")
      flagsOpt = shortenParts.lift(2)
      flagsMask = buildFlagsMask(flagsOpt)
      cc <- countries
    } yield {
      (cc -> ShortenInfo(toShortenFrom.r, toShortenTo, flagsMask))
    }).groupBy(_._1).mapValues(_.map(_._2)).toList.toMap
  }

  def buildNameDeletesList(cachedFiles: Seq[CachedFile]): Seq[String] = {
    (for {
      cachedFile <- cachedFiles
      source = Source.fromFile(cachedFile.file)
      line <- source.getLines.filterNot(_.startsWith("#"))
    } yield {
      line
    }).toSeq
  }
}