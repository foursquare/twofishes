// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

trait HotfixSource {
  def getEdits(): Seq[GeocodeServingFeatureEdit]
}
