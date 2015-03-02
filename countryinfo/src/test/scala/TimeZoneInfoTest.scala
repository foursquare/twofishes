package com.foursquare.geo.country

import org.specs2.mutable.Specification

class TimeZoneInfoSpec extends Specification {
  "timezone lookup works" in {
    TimeZoneInfo.lookupTzID("America/Anchorage").map(_.cc) mustEqual Some("US")
  }

  "timezone lookup gracefully fails on missing" in {
    TimeZoneInfo.lookupTzID("America/Adsfdsgdfgsdg") mustEqual None
  }

  "timezone lookup throws on invalid CC" in {
    TimeZoneInfo.tzIdsFromIso2("USA") must throwAn[Exception]
  }

  "timezones lookup from country should work" in {
    TimeZoneInfo.tzIdsFromIso2("US") must haveTheSameElementsAs(
      List("America/Adak", "America/Anchorage", "America/Boise", "America/Chicago", "America/Denver", "America/Detroit", "America/Indiana/Indianapolis", "America/Indiana/Knox", "America/Indiana/Marengo", "America/Indiana/Petersburg", "America/Indiana/Tell_City", "America/Indiana/Vevay", "America/Indiana/Vincennes", "America/Indiana/Winamac", "America/Juneau", "America/Kentucky/Louisville", "America/Kentucky/Monticello", "America/Los_Angeles", "America/Menominee", "America/Metlakatla", "America/New_York", "America/Nome", "America/North_Dakota/Beulah", "America/North_Dakota/Center", "America/North_Dakota/New_Salem", "America/Phoenix", "America/Sitka", "America/Yakutat", "Pacific/Honolulu")
    )

    TimeZoneInfo.tzIdsFromIso2("DE") must haveTheSameElementsAs(
      List("Europe/Berlin", "Europe/Busingen")
    )
  }
}
