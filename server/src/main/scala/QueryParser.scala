//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.util.{NameNormalizer, TwofishesLogger}
import com.twitter.ostrich.stats.Stats

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
      spaceAtEnd = query.endsWith(" ")
    )
  }

  def parseQueryTokens(originalTokens: List[String], spaceAtEnd: Boolean = false): ParseParams = {
    logger.ifDebug("--> %s", originalTokens.mkString("_|_"))

    // This is awful connector parsing
    val connectorStart = originalTokens.indexOf("near")
    val connectorEnd = connectorStart
    val hadConnector = connectorStart != -1

    val tokens = if (hadConnector) {
      originalTokens.drop(connectorEnd + 1)
    } else { originalTokens }

    // if there're too many tokens, take as many as are allowed and continue
    Stats.addMetric("query_length", originalTokens.size)
    if (hadConnector) {
      Stats.addMetric("query_length_after_connector_parsing", tokens.size)
    }
    val finalTokens = if (tokens.size > GeocodeServerConfigSingleton.config.maxTokens) {
      Stats.incr("too_many_tokens", 1)
      tokens.take(GeocodeServerConfigSingleton.config.maxTokens)
    } else {
      tokens
    }
    if (originalTokens.exists(_ == "http")) {
      throw new Exception("don't support url queries")
    }

    ParseParams(
      tokens = finalTokens,
      originalTokens = originalTokens,
      connectorStart = connectorStart,
      connectorEnd = connectorEnd,
      hadConnector = hadConnector,
      spaceAtEnd = spaceAtEnd
    )
  }
}
