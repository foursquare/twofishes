// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.indexer

import com.foursquare.twofishes.SlugEntryMap

trait FeatureLoader {
  def loadIntoMongo()	
  def getSlugEntryMap: SlugEntryMap.SlugEntryMap
}