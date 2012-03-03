// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.geocoder

import java.text.Normalizer
import java.util.regex.Pattern

trait Helpers {
  def tryo[T](f: => T): Option[T] = {
    try {
      Some(f)
    } catch {
      case _ => None
    }
  }

  def flattryo[T](f: => Option[T]): Option[T] = {
    try {
      f
    } catch {
      case _ => None
    }
  }
}

class NameNormalizer {
  def normalize(s: String): String = {
    var n = s
    // remove periods and quotes
    n = n.replaceAll("['\u2018\u2019\\.]", "")
    // change all other punctuation to spaces
    n = n.replaceAll("\\p{Punct}", " ")
    n
  }

  def deaccent(s: String): String = {
    val temp = Normalizer.normalize(s, Normalizer.Form.NFD);
    val pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    pattern.matcher(temp).replaceAll("");
  }
}