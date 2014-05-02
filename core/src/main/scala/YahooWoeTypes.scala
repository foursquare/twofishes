// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.YahooWoeType._

object YahooWoeTypes {
  val order = List(POSTAL_CODE, AIRPORT, SUBURB, TOWN, ADMIN3, ADMIN2, ADMIN1, COUNTRY)
  val orderMap = order.zipWithIndex.toList.toMap

  def getOrdering(woetype: YahooWoeType): Int = {
    orderMap.get(woetype).getOrElse(-1)
  }

  val isAdminWoeTypes = Set(ADMIN3, ADMIN2, ADMIN1, COUNTRY)
  def isAdminWoeType(woeType: YahooWoeType): Boolean =
    isAdminWoeTypes.contains(woeType)
}
