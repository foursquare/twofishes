//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.twitter.ostrich.stats.{Stats => OstrichStats}
import java.util.concurrent.ConcurrentHashMap

trait GeocoderTypes {
  case class SortedParseWithPosition(parse: Parse[Sorted], position: Int)

  val NullParse = Parse[Sorted](Vector.empty)
  // ParseSeq = multiple interpretations
  type ParseSeq = Seq[Parse[Unsorted]]
  type SortedParseSeq = Seq[Parse[Sorted]]

  // ParseCache = a table to save our previous parses from the left-most
  // side of the string.
  type ParseCache = ConcurrentHashMap[Int, SortedParseSeq]
}

trait StatsInterface[T] {
  def incr(k: String, num: Int)
  def incr(k: String)
  def addMetric(name: String, value: Int)
  def time[T](name: String)(f: => T): T
}

abstract class AbstractGeocoderImpl[T] extends GeocoderTypes {
  def implName: String

  def doGeocode(): T = {
    Stats.incr("requests", 1)
    Stats.time("response_time") {
      doGeocodeImpl()
    }
  }

  def doGeocodeImpl(): T

  object Stats extends StatsInterface[T] {
    def incr(k: String, num: Int) { OstrichStats.incr(implName + "." + k, num) }
    def incr(k: String) { OstrichStats.incr(implName + "." + k, 1) }
    def addMetric(name: String, value: Int)  { OstrichStats.addMetric(implName + "." + name, value) }
    def time[T](name: String)(f: => T): T = { OstrichStats.time(implName + "." + name) { f } }
  }
}
