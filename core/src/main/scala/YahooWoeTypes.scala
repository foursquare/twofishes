// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofish

import com.foursquare.twofish.YahooWoeType._

object YahooWoeTypes {
  val order = List(POSTAL_CODE, AIRPORT,  TOWN, SUBURB, ADMIN3, ADMIN2, ADMIN1, COUNTRY)

  def getOrdering(woetype: YahooWoeType): Int = {
    order.indexOf(woetype)
  }
}