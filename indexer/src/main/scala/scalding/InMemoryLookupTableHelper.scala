// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding.filecache.CachedFile
import scala.io.Source

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
}