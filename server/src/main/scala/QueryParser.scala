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
    val originalTokens = NameNormalizer.tokenize(normalizedQuery)

    logger.ifDebug("%s --> %s", query, normalizedQuery)

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
    val tokensStart = connectorEnd + 1

    val tokens = originalTokens.drop(tokensStart).map(NameNormalizer.depunctuate)

    // Need to tune the algorithm to not explode on > 10 tokens
    // in the meantime, reject.
    Stats.addMetric("query_length", tokens.size)
    if (tokens.size > GeocodeServerConfigSingleton.config.maxTokens) {
      Stats.incr("too_many_tokens", 1)
      throw new Exception("too many tokens")
    }
    if (tokens.exists(_ == "http")) {
      throw new Exception("don't support url queries")
    }

    ParseParams(
      tokens = tokens,
      originalTokens = originalTokens,
      connectorStart = connectorStart,
      connectorEnd = connectorEnd,
      hadConnector = connectorStart != -1,
      spaceAtEnd = spaceAtEnd
    )
  }
}
