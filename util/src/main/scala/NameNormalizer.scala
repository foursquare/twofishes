// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import java.text.Normalizer
import java.util.regex.Pattern

object NameNormalizer {
  def tokenize(s: String): List[String] = {
    s.split(" ").filterNot(_.isEmpty).toList
  }

  def normalize(s: String): String = {
    var n = s.toLowerCase
    // remove periods and quotes
    // \u2013 = en-dash
    n = n.replaceAll("['\u2018\u2019\\.\u2013]", "")
    // change all other punctuation to spaces
    n = n.replaceAll("\\p{Punct}", " ")
    // replace multiple spaces with one
    n = " +".r.replaceAllIn(n, " ")
    n
  }

  def deaccent(s: String): String = {
    val temp = Normalizer.normalize(s, Normalizer.Form.NFD);
    val pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    pattern.matcher(temp).replaceAll("");
  }
} 
