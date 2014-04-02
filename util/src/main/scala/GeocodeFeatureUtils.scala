// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import com.foursquare.twofishes.{GeocodeFeature, GeocodeInterpretation, YahooWoeType}
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.Lists.Implicits._

object GeoIdConstants {
  def makeGeonameIds(ids: Int*): Seq[GeonamesId] = {
    ids.map(i => GeonamesId(i.toLong))
  }

  val WashingtonDcIds = makeGeonameIds(4140963, 4138106) // city, state
  val QueensNyIds = makeGeonameIds(5133273, 5133268)
  val BrooklynNyIds = makeGeonameIds(5110302, 6941775) // city, county
  val ManhattanNyIds = makeGeonameIds(5128581, 5128594) // city, county

  // in greece, these are the admin2s of the attica admin1
  val atticaDepartments = makeGeonameIds(445408, 445406, 445407, 406101)
}

object GeocodeFeatureUtils {
  def findFeaturesByWoeType(
    interpretation: GeocodeInterpretation,
    woeType: YahooWoeType
  ): Iterable[GeocodeFeature] = {
    (if (interpretation.feature.woeTypeOption.has(woeType)) Some(interpretation.feature) else None) ++
      interpretation.parents.filter(_.woeTypeOption.has(woeType))
  }

  def findFeaturesByWoeType(
    interpretations: Iterable[GeocodeInterpretation],
    woeType: YahooWoeType
  ): Iterable[GeocodeFeature] = {
    interpretations.flatMap(findFeaturesByWoeType(_, woeType))
  }

  def getStateLikeFeature(interpretation: GeocodeInterpretation): Option[GeocodeFeature] = {
    val cc = interpretation.feature.ccOption.getOrElse("XX")
    // long discussion from greek SU wanted this formatting
    if (cc =? "GR") {
      val department = GeocodeFeatureUtils.findFeaturesByWoeType(interpretation, YahooWoeType.ADMIN2).headOption
      if (department.exists(f => GeoIdConstants.atticaDepartments.exists(_.longId == f.longId))) {
        GeocodeFeatureUtils.findFeaturesByWoeType(interpretation, YahooWoeType.ADMIN1).headOption
      } else {
        department
      }
    } else if (NameUtils.countryUsesCountyAsState(cc)) {
      GeocodeFeatureUtils.findFeaturesByWoeType(interpretation, YahooWoeType.ADMIN2).headOption
    } else {
      GeocodeFeatureUtils.findFeaturesByWoeType(interpretation, YahooWoeType.ADMIN1).headOption
    }
  }
}
