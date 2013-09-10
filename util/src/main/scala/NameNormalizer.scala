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

    if (System.getProperty("javaSevenDottedIHack") != null) {
      // change uppercase dotted I to uppercase I
      // java7 does somethign weird when it lowercases this that isn't
      // backwards comaptibel with an index generated in 6
      // https://github.com/alexholmes/blog/blob/master/_posts/2013-02-14-java-7-and-the-dotted--and-dotless-i.markdown
      n = n.replace('\u0130', 'I')
    }
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
