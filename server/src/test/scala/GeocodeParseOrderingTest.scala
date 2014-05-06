// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes

import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.twofishes.util.{GeometryUtils, GeonamesId, StoredFeatureId}
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import java.nio.ByteBuffer
import org.specs2.mutable._
import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

class GeocodeParseOrderingTest {
}