package com.foursquare.twofishes

import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.StoredFeatureId
import scalaj.collection.Implicits._

trait BulkImplHelpers {
  def makeBulkReply[T](
      inputs: Seq[T],
      inputFids: Map[T, Seq[StoredFeatureId]],
      interps: Seq[GeocodeInterpretation],
      isolateParents: Boolean = false):
      (Seq[Seq[Int]], Seq[GeocodeInterpretation], Seq[GeocodeFeature]) = {
    val inputToInputIdxes: Map[T, Seq[Int]] = inputs.zipWithIndex.groupBy(_._1).mapValues(_.map(_._2))

    val inputIdxToLongFids: Map[Int, Seq[Long]] = inputFids.flatMap({
      case (input, fids) => inputToInputIdxes.getOrElse(input, Nil).map(inputIdx => (inputIdx -> fids.map(_.longId)))
    })

    val featureIdToInterpIdxes: Map[Long, Seq[Int]] = interps.zipWithIndex
      .groupBy(_._1.feature.longId)
      .mapValues(_.map(_._2))

    val inputIdxToInterpIdxs: Seq[Seq[Int]] = (for {
      inputIdx <- (0 to inputs.size - 1)
      val longFids = inputIdxToLongFids.getOrElse(inputIdx, Nil)
    } yield {
      longFids.flatMap(longFid => featureIdToInterpIdxes(longFid))
    })

    if (isolateParents) {
      val parents = interps.flatMap(interp => if (interp.isSetParents) interp.parents.asScala else Nil)
      val allParentFeatures = {
        val seen = scala.collection.mutable.HashSet[Long]()
        parents.filter(p => {
          if (!seen(p.longId)) {
            seen += p.longId
            true
          } else false
        })
      }

      interps.foreach(interp => {
        if (interp.isSetParents) {
          val parentIds = interp.parents.asScala.map(_.longId)
          interp.unsetParents()
          interp.setParentLongIds(parentIds.asJava)
        }
      })

      (inputIdxToInterpIdxs, interps, allParentFeatures)
    } else {
      (inputIdxToInterpIdxs, interps, Nil)
    }
  }
}
