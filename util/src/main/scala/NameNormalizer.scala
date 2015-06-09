// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.util

import java.text.Normalizer
import java.util.regex.Pattern

object NameNormalizer {
  val splitTokenRegex = "[ ,]"
  def tokenize(s: String): List[String] = {
    s.split(splitTokenRegex).filterNot(_.isEmpty).toList
  }

  val spaceRegexp = " +".r
  val punctRegexp = "\\p{Punct}".r
  val dotsAndOtherRegexp = "['\u2018\u2019\\.\u2013]".r

  val useJavaSevenDottedIHack = System.getProperty("javaSevenDottedIHack") != null

  def normalize(s: String): String = {
    var n: String = null

    if (useJavaSevenDottedIHack) {
      // change uppercase dotted I to uppercase I
      // java7 does something weird when it lowercases this that isn't
      // backwards compatible with an index generated in 6
      // https://github.com/alexholmes/blog/blob/master/_posts/2013-02-14-java-7-and-the-dotted--and-dotless-i.markdown
      n = s.replace('\u0130', 'I').toLowerCase
    } else {
      n = s.toLowerCase
    }

    // remove periods and quotes
    // \u2013 = en-dash
    n = dotsAndOtherRegexp.replaceAllIn(n, "")
    // change all other punctuation to spaces
    n = punctRegexp.replaceAllIn(n, " ")
    // replace multiple spaces with one
    n = spaceRegexp.replaceAllIn(n, " ")
    n = n.replace("\t", " ")
    
    n
  }

  def deaccent(s: String): String = {
    val temp = Normalizer.normalize(s, Normalizer.Form.NFD);
    val pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    pattern.matcher(temp).replaceAll("");
  }
}
