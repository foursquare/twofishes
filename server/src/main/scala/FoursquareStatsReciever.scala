// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.twofishes

import com.twitter.finagle.stats.{Counter, Stat, StatsReceiverWithCumulativeGauges}
import com.twitter.ostrich.stats.{Stats, StatsProvider}

/**
 * An adapter from our stats-gathering interface to finagle's StatsReceiver interface.
 *
 * Note that our stats-gathering interface is just an ostrich StatsProvider, and Finagle already comes with an
 * OstrichStatsReceiver. Unfortunately, however, that class talks directly to the global Stats object, and
 * we may want to override with a custom StatsProvider.
 */
class FoursquareStatsReceiver(prefix: List[String] = Nil)
        extends StatsReceiverWithCumulativeGauges {

  val statsProvider = Stats
  val repr = statsProvider

  protected[this] def registerGauge(name: Seq[String], f: => Float) {
    statsProvider.addGauge(variableName(name)) {
      f.toDouble
    }
  }

  protected[this] def deregisterGauge(name: Seq[String]) {
    statsProvider.clearGauge(variableName(name))
  }

  def counter(name: String*) = new Counter {
    def incr(delta: Int) {
      statsProvider.incr(variableName(name), delta)
    }
  }

  def stat(name: String*) = new Stat {
    def add(value: Float) {
      statsProvider.addMetric(variableName(name), value.toInt)
    }
  }

  private[this] def variableName(name: Seq[String]): String = (prefix ++ name).map(_.replace('.', '_')).mkString(".")
}