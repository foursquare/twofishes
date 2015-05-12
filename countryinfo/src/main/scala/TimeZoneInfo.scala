// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.geo.country

import scala.io.BufferedSource


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

  private val tzLines = new BufferedSource(getClass.getResourceAsStream("/timeZones.txt")).getLines.drop(1)
  private val timeZoneInfos: Seq[TimeZoneInfo] = tzLines.filterNot(l => l.startsWith("#") || l.isEmpty).toList.map(l => {
    val parts = l.split("\t")
    TimeZoneInfo(
      cc = parts(COUNTRY_CODE.id),
      tzid = parts(TZID.id),
      gmtOffset = parts(GMT_OFFSET.id).toDouble,
      dstOffset = parts(DST_OFFSET.id).toDouble,
      rawOffset = parts(RAW_OFFSET.id).toDouble
    )
  })

  private val ccToTimeZoneInfos: Map[String, Seq[TimeZoneInfo]] =
    timeZoneInfos.groupBy(_.cc).toMap

  private val tzIdToTimeZoneInfo: Map[String, TimeZoneInfo] =
    timeZoneInfos.map(tz => (tz.tzid, tz)).toMap

  def lookupTzID(tzid: String): Option[TimeZoneInfo] =
    tzIdToTimeZoneInfo.get(tzid)

  def tzIdsFromIso2(cc: String): Seq[String] =
    timeZoneInfosFromIso2(cc).map(_.tzid)

  def timeZoneInfosFromIso2(cc: String): Seq[TimeZoneInfo] = {
    if (cc.length != 2) {
      throw new Exception("two letter country code required")
    } else {
      ccToTimeZoneInfos.getOrElse(cc, Seq.empty)
    }
  }
}
