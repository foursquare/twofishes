package com.foursquare.twofishes.country

import org.specs2.mutable.Specification

class CountryUtilsSpec extends Specification {
  "US should exist" in {
    CountryUtils.getNameByCode("US") mustEqual Some("United States")
    CountryUtils.getNameByCode("USA") mustEqual Some("United States")
    CountryUtils.getNameByCode("ABC") mustEqual None
  }
}
