
package com.foursquare.twofishes.output

import com.foursquare.twofishes.Indexes
import com.foursquare.twofishes.mongo.{MongoGeocodeDAO, S2CoveringIndex, S2CoveringIndexDAO}
import com.foursquare.twofishes.util.S2CoveringConstants
import com.mongodb.Bytes
import com.mongodb.casbah.Imports._

class S2CoveringIndexer(
  override val basepath: String,
  override val fidMap: FidMap
) extends Indexer with S2CoveringConstants {
  val index = Indexes.S2CoveringIndex
  override val outputs = Seq(index)

  def writeIndexImpl() {
    val polygonSize = S2CoveringIndexDAO.collection.count()
    val usedPolygonSize = MongoGeocodeDAO.count(MongoDBObject("hasPoly" -> true))

    val hasPolyCursor =
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))
        .sort(orderBy = MongoDBObject("_id" -> 1)) // sort by _id asc
    hasPolyCursor.option = Bytes.QUERYOPTION_NOTIMEOUT

    val writer = buildMapFileWriter(
      index,
      Map(
        "minS2Level" -> minS2LevelForS2Covering.toString,
        "maxS2Level" -> maxS2LevelForS2Covering.toString,
        "levelMod" -> defaultLevelModForS2Covering.toString
      )
    )

    var numUsedPolygon = 0
    val groupSize = 1000
    // would be great to unify this with featuresIndex
    for {
      (g, groupIndex) <- hasPolyCursor.grouped(groupSize).zipWithIndex
      group = g.toList
      toFindCovers: Map[Long, ObjectId] = group.filter(f => f.hasPoly).map(r => (r._id, r.polyId)).toMap
      coverMap: Map[ObjectId, S2CoveringIndex] = S2CoveringIndexDAO.find(MongoDBObject("_id" -> MongoDBObject("$in" -> toFindCovers.values.toList)))
        .toList
        .groupBy(_._id).map({case (k, v) => (k, v(0))})
      (f, coverIndex) <- group.zipWithIndex
      covering <- coverMap.get(f.polyId)
    } {
      if (coverIndex == 0) {
        logger.info("S2CoveringIndexer: outputted %d of %d used polys, %d of %d total polys seen".format(
          numUsedPolygon, usedPolygonSize, polygonSize, groupIndex*groupSize))
      }
      numUsedPolygon += 1
      writer.append(f.featureId, covering.cellIds)
    }
    writer.close()

    logger.info("done")
  }
}
