//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.util.Lists.Implicits._

case class OrderedParseGroup(ranker: GeocodeParseOrdering, scoreCutoff: Option[Int], limit: Option[Int] = None)

class OrderedParseGroupMerger(parses: Seq[Parse[Sorted]], groups: Seq[OrderedParseGroup]) {

  def merge() = {
    var uniqueFeatureIds: Set[Long] = Set()
    val finalParsesFromGroups = groups.flatMap(group => {
      val finalParses = parses.sorted(group.ranker).filter(r => {
        (r.finalScore >= group.scoreCutoff.getOrElse(Int.MinValue) &&
         !uniqueFeatureIds.has(r.primaryFeature.fmatch.longId))
      }).take(group.limit.getOrElse(Int.MaxValue))

      uniqueFeatureIds = uniqueFeatureIds ++ finalParses.map(_.primaryFeature.fmatch.longId)
      finalParses
    })

    finalParsesFromGroups
  }
}