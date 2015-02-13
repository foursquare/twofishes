// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.geo.country

import scala.io.Source
import scala.io.BufferedSource
import scala.util.matching.Regex


object TimeZoneInfoFields extends Enumeration {
  val COUNTRY_CODE, TZID, GMT_OFFSET, DST_OFFSET, RAW_OFFSET = Value
}

case class TimeZoneInfo(
  cc: String,
  tzid: String,
  gmtOffset: Double,
  dstOffset: Double,
  rawOffset: Double
)

object TimeZoneInfo {
  import TimeZoneInfoFields._

  val tzLines = new BufferedSource(getClass.getResourceAsStream("/timeZones.txt")).getLines.drop(1)
  val tzInfos: Seq[TimeZoneInfo] = tzLines.filterNot(l => l.startsWith("#") || l.isEmpty).toList.map(l => {
    val parts = l.split("\t")
    TimeZoneInfo(
      cc = parts(COUNTRY_CODE.id), 
      tzid = parts(TZID.id), 
      gmtOffset = parts(GMT_OFFSET.id).toDouble,
      dstOffset = parts(DST_OFFSET.id).toDouble,
      rawOffset = parts(RAW_OFFSET.id).toDouble
    )
  })

  val ccToTZIDs: Map[String, Seq[String]] = 
    tzInfos
      .groupBy(_.cc)
      .map({case (cc, infos) => (cc -> infos.map(_.tzid))})
      .toMap
}