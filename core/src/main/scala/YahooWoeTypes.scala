// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.twofishes.YahooWoeType._

object YahooWoeTypes {
  val order = List(POSTAL_CODE, AIRPORT, SUBURB, TOWN, ADMIN3, ADMIN2, ADMIN1, COUNTRY)

  def getOrdering(woetype: YahooWoeType): Int = {
    order.indexOf(woetype)
  }
}
