//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.{NameNormalizer, TwofishesLogger}
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.ostrich.stats.Stats
import scalaj.collection.Implicits._

case class ParseParams(
  tokens: List[String] = Nil,
  originalTokens: List[String] = Nil,
  connectorStart: Int = 0,
  connectorEnd: Int = 0,
  hadConnector: Boolean = false,
  spaceAtEnd: Boolean = false
)

class QueryParser(logger: TwofishesLogger) {
  def parseQuery(query: String): ParseParams = {
    val normalizedQuery = NameNormalizer.normalize(query)
    logger.ifDebug("%s --> %s", query, normalizedQuery)

    var originalTokens = NameNormalizer.tokenize(normalizedQuery)
    parseQueryTokens(
      originalTokens,
      spaceAtEnd = query.takeRight(1) == " "
    )
  }

  def parseQueryTokens(originalTokens: List[String], spaceAtEnd: Boolean = false): ParseParams = {
    logger.ifDebug("--> %s", originalTokens.mkString("_|_"))

    // This is awful connector parsing
    val connectorStart = originalTokens.findIndexOf(_ == "near")
    val connectorEnd = connectorStart
    val hadConnector = connectorStart != -1

    val tokens = if (hadConnector) {
      originalTokens.drop(connectorEnd + 1)
    } else { originalTokens }

    // Need to tune the algorithm to not explode on > 10 tokens
    // in the meantime, reject.
    Stats.addMetric("query_length", originalTokens.size)
    if (originalTokens.size > 10) {
      Stats.incr("too_many_tokens", 1)
      throw new Exception("too many tokens")
    }

    ParseParams(
      tokens = tokens,
      originalTokens = originalTokens,
      connectorStart = connectorStart,
      connectorEnd = connectorEnd,
      hadConnector = hadConnector,
      spaceAtEnd = spaceAtEnd
    )
  }
}
