// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.foursquare.common.thrift.ThriftConverter
import com.foursquare.twofishes._
import com.ibm.icu.text.Transliterator

object PolygonMatchingHelper {

  val transliterator = Transliterator.getInstance("Any-Latin; NFD;")

  def getS2LevelForWoeType(woeType: YahooWoeType): Int = {
    woeType match {
      case YahooWoeType.COUNTRY => 4
      case YahooWoeType.ADMIN1 => 6
      case YahooWoeType.ADMIN2 => 6
      case YahooWoeType.ADMIN3 => 8
      case YahooWoeType.TOWN => 8
      case YahooWoeType.SUBURB => 10
      case _ => 10
    }
  }
}
