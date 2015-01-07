package com.foursquare.geo.quadtree

import org.specs2.mutable.Specification

class CountryRevGeoTest extends Specification {
  "basic test" in {
    CountryRevGeo.getNearestCountryCode(40.74, -74) mustEqual Some("US")
    CountryRevGeo.getNearestCountryCode(-19.937205332238577,-55.8489990234375) mustEqual Some("BR")
  }
}
