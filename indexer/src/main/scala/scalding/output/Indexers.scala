// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding.output

import com.foursquare.twofishes._
import com.foursquare.twofishes.util.{RevGeoConstants, S2CoveringConstants, StoredFeatureId}
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBReader
import org.apache.hadoop.io.{Text, LongWritable}

// copy scalding index build intermediate job output sequence files locally
// run any indexer as follows:
// java -cp indexer-assembly-[version].jar com.foursquare.twofishes.scalding.output.[indexer class name] [path to scalding intermediate sequence files] [output path]
// TODO(rahul): make it so mapfiles/hfile can be directly produced by scalding jobs

object NameIndexer {
  def processor(key: Text, value: IntermediateDataContainer): Option[(String, Seq[StoredFeatureId])] = {
    val name = key.toString
    val featureIds = value.longList.flatMap(StoredFeatureId.fromLong)
    Some((name, featureIds))
  }

  def main(args: Array[String]): Unit = {
    new BaseIndexer(
      inputBaseDir = args(0),
      outputBaseDir = args(1),
      index = Indexes.NameIndex,
      scaldingIntermediateJobName = "name_index_build_intermediate",
      options = IndexerOptions(IndexOutputType.HFILE_OUTPUT),
      processor = processor
    ).writeIndex()
  }
}

object PrefixIndexer {
  def main(args: Array[String]): Unit = {
    new BaseIndexer(
      inputBaseDir = args(0),
      outputBaseDir = args(1),
      index = Indexes.PrefixIndex,
      scaldingIntermediateJobName = "prefix_index_build_intermediate",
      options = IndexerOptions(
        IndexOutputType.MAPFILE_OUTPUT,
        Map(
          "MAX_PREFIX_LENGTH" -> com.foursquare.twofishes.output.PrefixIndexer.MaxPrefixLength.toString,
          "MAX_FIDS_PER_PREFIX" -> com.foursquare.twofishes.output.PrefixIndexer.MaxFidsToStorePerPrefix.toString
        )),
      processor = NameIndexer.processor
    ).writeIndex()
  }
}

object IdIndexer {
  def processor(key: Text, value: IntermediateDataContainer): Option[(String, StoredFeatureId)] = {
    val idString = key.toString
    for {
      longId <- value.longValueOption
      featureId <- StoredFeatureId.fromLong(longId)
    } yield {
      (idString, featureId)
    }
  }

  def main(args: Array[String]): Unit = {
    new BaseIndexer(
      inputBaseDir = args(0),
      outputBaseDir = args(1),
      index = Indexes.IdMappingIndex,
      scaldingIntermediateJobName = "id_index_build_intermediate",
      options = IndexerOptions(IndexOutputType.MAPFILE_OUTPUT),
      processor = processor
    ).writeIndex()
  }
}

object PolygonIndexer {
  val wkbReader = new WKBReader()

  def processor(key: LongWritable, value: IntermediateDataContainer): Option[(StoredFeatureId, Geometry)] = {
    val featureIdOpt = StoredFeatureId.fromLong(key.get)
    featureIdOpt.map(featureId => {
      val geometry = wkbReader.read(value.bytesByteArray)
      (featureId, geometry)
    })
  }

  def main(args: Array[String]): Unit = {
    new BaseIndexer(
      inputBaseDir = args(0),
      outputBaseDir = args(1),
      index = Indexes.GeometryIndex,
      scaldingIntermediateJobName = "polygon_index_build_intermediate",
      options = IndexerOptions(IndexOutputType.MAPFILE_OUTPUT),
      processor = processor
    ).writeIndex()
  }
}

object S2CoveringIndexer {
  def processor(key: LongWritable, value: IntermediateDataContainer): Option[(StoredFeatureId, Seq[Long])] = {
    val featureIdOpt = StoredFeatureId.fromLong(key.get)
    featureIdOpt.map(featureId => {
      val cellIds = value.longList
      (featureId, cellIds)
    })
  }

  def main(args: Array[String]): Unit = {
    new BaseIndexer(
      inputBaseDir = args(0),
      outputBaseDir = args(1),
      index = Indexes.S2CoveringIndex,
      scaldingIntermediateJobName = "s2_covering_index_build_intermediate",
      options = IndexerOptions(
        IndexOutputType.MAPFILE_OUTPUT,
        Map(
          "minS2Level" -> S2CoveringConstants.minS2LevelForS2Covering.toString,
          "maxS2Level" -> S2CoveringConstants.maxS2LevelForS2Covering.toString,
          "levelMod" -> S2CoveringConstants.defaultLevelModForS2Covering.toString
        )),
      processor = processor
    ).writeIndex()
  }
}

object S2InteriorIndexer {
  def processor(key: LongWritable, value: IntermediateDataContainer): Option[(StoredFeatureId, Seq[Long])] = {
    val featureIdOpt = StoredFeatureId.fromLong(key.get)
    featureIdOpt.map(featureId => {
      val cellIds = value.longList
      (featureId, cellIds)
    })
  }

  def main(args: Array[String]): Unit = {
    new BaseIndexer(
      inputBaseDir = args(0),
      outputBaseDir = args(1),
      index = Indexes.S2InteriorIndex,
      scaldingIntermediateJobName = "s2_interior_index_build_intermediate",
      options = IndexerOptions(
        IndexOutputType.MAPFILE_OUTPUT,
        Map(
          "minS2Level" -> S2CoveringConstants.minS2LevelForS2Covering.toString,
          "maxS2Level" -> S2CoveringConstants.maxS2LevelForS2Covering.toString,
          "levelMod" -> S2CoveringConstants.defaultLevelModForS2Covering.toString
        )),
      processor = processor
    ).writeIndex()
  }
}

object RevGeoIndexer {
  def processor(key: LongWritable, value: CellGeometries): Option[(Long, CellGeometries)] = {
    val cellId = key.get
    Some((cellId, value))
  }

  def main(args: Array[String]): Unit = {
    new BaseIndexer(
      inputBaseDir = args(0),
      outputBaseDir = args(1),
      index = Indexes.S2Index,
      scaldingIntermediateJobName = "rev_geo_index_build_intermediate",
      options = IndexerOptions(
        IndexOutputType.MAPFILE_OUTPUT,
        Map(
          "minS2Level" -> RevGeoConstants.minS2LevelForRevGeo.toString,
          "maxS2Level" -> RevGeoConstants.maxS2LevelForRevGeo.toString,
          "levelMod" -> RevGeoConstants.defaultLevelModForRevGeo.toString
        )),
      processor = processor
    ).writeIndex()
  }
}

object FeatureIndexer {
  def processor(key: LongWritable, value: GeocodeServingFeature): Option[(StoredFeatureId, GeocodeServingFeature)] = {
    val featureIdOpt = StoredFeatureId.fromLong(key.get)
    featureIdOpt.map(featureId => {
      (featureId, value)
    })
  }

  def main(args: Array[String]): Unit = {
    new BaseIndexer(
      inputBaseDir = args(0),
      outputBaseDir = args(1),
      index = Indexes.FeatureIndex,
      scaldingIntermediateJobName = "feature_index_build_intermediate",
      options = IndexerOptions(IndexOutputType.MAPFILE_OUTPUT, mapFileIndexInterval = Some(2)),
      processor = processor
    ).writeIndex()
  }
}
