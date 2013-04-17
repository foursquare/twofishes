//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.twofishes.Implicits._
import com.foursquare.twofishes.util.GeoTools
import com.foursquare.twofishes.util.Lists.Implicits._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scalaj.collection.Implicits._

// Represents a match from a run of tokens to one particular feature
case class FeatureMatch(
  tokenStart: Int,
  tokenEnd: Int,
  phrase: String,
  fmatch: GeocodeServingFeature,
  possibleNameHits: Seq[FeatureName] = Nil
)

// Sort a list of feature matches, smallest to biggest
object FeatureMatchOrdering extends Ordering[FeatureMatch] {
  def compare(a: FeatureMatch, b: FeatureMatch) = {
    GeocodeServingFeatureOrdering.compare(a.fmatch, b.fmatch)
  }
}

trait MaybeSorted
trait Sorted extends MaybeSorted
trait Unsorted extends MaybeSorted

// Parse = one particular interpretation of the query
case class Parse[T <: MaybeSorted](
  fmatches: Seq[FeatureMatch],
  scoringFeatures: InterpretationScoringFeatures = new InterpretationScoringFeatures(),
  debugInfo: Option[InterpretationDebugInfo] = Some(new InterpretationDebugInfo())
) extends Seq[FeatureMatch] {
  def apply(i: Int) = fmatches(i)
  def iterator = fmatches.iterator
  def length = fmatches.length

  override def toString: String = {
    val name = this.flatMap(_.fmatch.feature.names.headOption).map(_.name).mkString(", ")
    // god forgive this line of code
    val id = this.headOption.toList.flatMap(f => Option(f.fmatch.feature.ids)).flatten.headOption.map(fid =>
      "%s:%s".format(fid.source, fid.id)).getOrElse("no:id")
    "%s %s".format(id, name)
  }

  def tokenLength = fmatches.map(pp => pp.tokenEnd - pp.tokenStart).sum

  def getSorted: Parse[Sorted] =
    Parse[Sorted](fmatches.sorted(FeatureMatchOrdering), scoringFeatures)

  def addFeature(f: FeatureMatch) = Parse[Unsorted](fmatches ++ List(f))

  def countryCode = fmatches.headOption.map(_.fmatch.feature.cc).getOrElse("XX")

  def hasDupeFeature: Boolean = {
    this.headOption.exists(primaryFeature => {
      val rest = this.drop(1)
      rest.exists(_.fmatch.feature.ids == primaryFeature.fmatch.feature.ids)
    })
  }

}

trait GeocoderImplTypes {
  case class SortedParseWithPosition(parse: Parse[Sorted], position: Int)

  val NullParse = Parse[Sorted](Nil)
  // ParseSeq = multiple interpretations
  type ParseSeq = Seq[Parse[Unsorted]]
  type SortedParseSeq = Seq[Parse[Sorted]]

  // ParseCache = a table to save our previous parses from the left-most
  // side of the string.
  type ParseCache = ConcurrentHashMap[Int, SortedParseSeq]
}

object ParseUtils {
  def featureDistance(f1: GeocodeFeature, f2: GeocodeFeature) = {
    GeoTools.getDistance(
      f1.geometry.center.lat,
      f1.geometry.center.lng,
      f2.geometry.center.lat,
      f2.geometry.center.lng)
  }

  def boundsContains(f1: GeocodeFeature, f2: GeocodeFeature) = {
    Option(f1.geometry.bounds).exists(bb =>
      GeoTools.boundsContains(bb, f2.geometry.center)) ||
    Option(f2.geometry.bounds).exists(bb =>
      GeoTools.boundsContains(bb, f1.geometry.center))
  }

  def parsesNear(p1: Parse[Sorted], p2: Parse[Sorted]): Boolean = {
    (p1.headOption, p2.headOption) match {
      case (Some(sf1), Some(sf2)) => {
        (featureDistance(sf1.fmatch.feature, sf2.fmatch.feature) < 15000) ||
        boundsContains(sf1.fmatch.feature, sf2.fmatch.feature)
      }
      case _ => false
    }
  }
}