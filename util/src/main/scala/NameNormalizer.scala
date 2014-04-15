// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import java.text.Normalizer
import java.util.regex.Pattern

object NameNormalizer {
  def tokenize(s: String): List[String] = {
    s.split(" ").filterNot(_.isEmpty).toList
  }

  val whitespaceRegexp = "[ \t]+".r
  val allPunctuationRegexp = "[\\p{Punct}\\u2018\\u2019\\u2013]+".r
  val loosePunctuationRegexp = ("\\s+" + allPunctuationRegexp).r

  def normalize(s: String): String = {
    var n: String = null

    if (System.getProperty("javaSevenDottedIHack") != null) {
      // change uppercase dotted I to uppercase I
      // java7 does something weird when it lowercases this that isn't
      // backwards compatible with an index generated in 6
      // https://github.com/alexholmes/blog/blob/master/_posts/2013-02-14-java-7-and-the-dotted--and-dotless-i.markdown
      n = s.replace('\u0130', 'I').toLowerCase
    } else {
      n = s.toLowerCase
    }

    // Remove loose punctuation and collapse redundant whitespace.
    n = loosePunctuationRegexp.replaceAllIn(n, "")
    n = whitespaceRegexp.replaceAllIn(n, " ")

    n
  }

  def depunctuate(s: String) = {
    allPunctuationRegexp.replaceAllIn(s, "")
  }

  def deaccent(s: String): String = {
    val temp = Normalizer.normalize(s, Normalizer.Form.NFD);
    val pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    pattern.matcher(temp).replaceAll("");
  }
}
