
package com.foursquare.geo

import com.foursquare.geo.ShapefileGeo.{GeoBounds, ShapeLeafNode, ShapeTrieNode}
import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier
import java.io.{File, Serializable}
import org.geotools.data._
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.geotools.feature.simple.SimpleFeatureTypeImpl
import org.opengis.feature.`type`.{AttributeDescriptor, AttributeType}
import scalaj.collection.Imports._

/** Options for simplification.
  *
  * @param outputSingleFeatureCellsWithGeometry - If this is true, then cells
  *   with just one feature keep their potentially complex shape. If it's false
  *   (the default), then we expand those cells to be the full "square" and
  *   then roll that expansion up to the parents too. This can lead to very
  *   blocky coasts, but since those are often the most complex polygons, it
  *   may be worthwhile.
  * @param thresholdForPolygonSimplification - When we get to the lowest-level
  *   cell in the grid and stop gridifying, there may be complex polygons
  *   (borders, coasts, etc.). For any polygons with more than this number of
  *   vertices, attempt to simplify them.
  * @param toleranceForPolygonSimplification - The tolerance as measured in
  *   degrees for the polygon simplification at the bottom layer. The default
  *   of .0005 is about 50 meters.
  */
case class SimplifierOptions(outputSingleFeatureCellsWithGeometry: Boolean = false,
                             thresholdForPolygonSimplification: Int = 50,
                             toleranceForPolygonSimplification: Double = .0005)

object SimplifierOptions {
  val Default = SimplifierOptions()
}

object ShapefileSimplifier {
  val defaultLevels = Array(40, 2, 2, 2)

  object Coords extends Enumeration {
    type Coords = Value
    val BottomLeft, BottomRight, TopRight, TopLeft = Value
  }
}

/** Takes an input shapefile with complicated shapes, then outputs a shapefile
  * with more shapes that are each simpler.
  *
  * The major steps are:
  * 1. Break the world into a grid as specified by the input `levels` array.
  *   For the default of (40, 2, 2, 2), we break the world into a 40x40 grid,
  *   then each of those cells into a 2x2, then each of those into a 2x2, then
  *   each of those into a 2x2.
  * 2. For each grid cell, see which of the original complex shapes it overlaps
  *   with. Intersect with each and save a sub-shape for each intersection, along
  *   with the attributes from that intersection.
  * 3. If a grid cell at one level is comprised of child cells that completely
  *   fill it, and all those child cells share the same attribute, then throw
  *   away the child cell and just use the parent cell.
  */
class ShapefileSimplifier(options: SimplifierOptions = SimplifierOptions.Default) {
  import ShapefileSimplifier._

  def simplify(node: ShapeTrieNode, levels: Array[Int]) = {
    gridifyList(node)

    def gridifyList(node: ShapeTrieNode): Unit = {
      val nLongs = levels(node.nodeLevel)
      val nLats = levels(node.nodeLevel)
      val longChunk = node.nodeBounds.width / nLongs
      val latChunk = node.nodeBounds.height / nLats

      def toIndex(coord: Coordinate): (Int, Int) = {
        (((coord.x - node.nodeBounds.minLong) / longChunk).toInt,
         ((coord.y - node.nodeBounds.minLat)  / latChunk ).toInt)
      }

      def foreachOverNodeCells(shapeOpt: Option[Geometry] = None)(f: ShapeTrieNode => Unit) = {
        val (minLg, maxLg, minLt, maxLt) = shapeOpt match {
          case Some(shape) =>
            // optimization to only consider intersecting cells within the shape's bounding box.
            val boundingBox = shape.getEnvelope
            assert(boundingBox.isRectangle)  // as opposed to a point.
            val (minLongIdx, minLatIdx) = toIndex(boundingBox.getCoordinates()(Coords.BottomLeft.id))
            val (maxLongIdx, maxLatIdx) = toIndex(boundingBox.getCoordinates()(Coords.TopRight.id))

            // Note: We can get indexes past the edge because of floating point
            // precision-loss or exact boundaries, so we correct here.
            (minLongIdx, math.min(maxLongIdx, nLongs - 1),
             minLatIdx, math.min(maxLatIdx, nLats - 1))
         case None =>
            (0, nLongs - 1,
             0, nLats - 1)
        }

        for {
          longIdx <- minLg to maxLg
          latIdx <- minLt to maxLt
        } {
          f(node.subGrid.get(longIdx)(latIdx))
        }
      }

      // Split this node into an NxN grid. The size of the grid is the
      // nodeLevel'th element in the `levels` array.
      node.makeSubGrid(levels)

      // Process shapes into sub-shapes by intersecting the shapes with the grid.
      // This greatly reduces the # of points per shape, and contains(lat,lng)
      // is roughly linear to the number of points in the shape, so it speeds the
      // contains() operation sigificantly.
      node.subList.foreach(keyShape => {
        // optimization to only consider intersecting cells within the shape's bounding box.
        val boundingBox = keyShape.shape.getEnvelope
        foreachOverNodeCells(shapeOpt = Some(keyShape.shape))(cell => {
          // Note: The cell may not actually intersect with the shape because
          // the shape's envelope is an over approximation of the shape.  We
          // may have found a nook in the shape.
          if (cell.shape.intersects(keyShape.shape)) {
            cell.subList ::= new ShapeLeafNode(keyShape.keyValue.get, cell.shape.intersection(keyShape.shape))
          }
        })
      })

      // If the cell is half-covered by shapes, but all those shapes share an
      // attribute and we're okay with throwing away our leaf-node geometries,
      // just expand the cell out to be one square with the attribute.
      foreachOverNodeCells()(cell => {
        cell.subList.headOption.flatMap(_.keyValue).foreach(kv => {
          if (!options.outputSingleFeatureCellsWithGeometry &&
              cell.subList.forall(_.keyValue == Some(kv))) {
            cell.subList = List(new ShapeLeafNode(kv, cell.shape))
          }
        })
      })

      val oneSubCell = node.subGrid.get.apply(0).apply(0)
      // Compress shapes in the case where there is only one active attribute
      // in the subgrid. The rules are different depending on whether we want
      // to preserve the geometries of these shapes. If we do, then we can only
      // collapse when the cell shape and subshapes are the same, and the same
      // is true of the children. (Basically, when we're in a big landmass and
      // we just want to collapse 4 sub-squares into one big square.) If we're
      // okay with throwing away the leaf node's geometries, then we can
      // aggressively collapse up as long as all the attributes are the same.
      if (options.outputSingleFeatureCellsWithGeometry &&
          oneSubCell.keyValue.isDefined &&
          node.subList.forall(_.shape.covers(node.shape)) &&
          node.subGrid.get.forall(_.forall(cell =>
            cell.keyValue == oneSubCell.keyValue &&
            cell.subList.forall(_.shape.covers(cell.shape))))) {
        // This is the interior-landmass case, where we want to collapse 4
        // sub-squares into one big square.
        node.subGrid = None
      } else if (!options.outputSingleFeatureCellsWithGeometry &&
                 oneSubCell.keyValue.isDefined &&
                 node.subGrid.get.forall(_.forall(cell =>
                   cell.keyValue == oneSubCell.keyValue))) {
        node.subGrid = None
      } else {
        node.subList = Nil
        // Gridify each of the children.
        foreachOverNodeCells()(cell => {
          cell.subList match {
            case Nil =>
            case x :: Nil if x == cell.shape =>
            case _ =>
              if (cell.nodeLevel < levels.length) {
                // RECURSE
                // Note: I have tried several different additional condtions,
                // e.g.  only if listSize > 5. Doesn't seem to matter much. Maybe
                // a good tweak would be recurse if the aggregate number of
                // points in the list is greater than some threshold
                gridifyList(cell)
              } else {
                cell.subList = cell.subList.map(shape =>
                  if (shape.shape.getNumPoints > options.thresholdForPolygonSimplification) {
                    val newShape =
                      TopologyPreservingSimplifier.simplify(shape.shape, options.toleranceForPolygonSimplification)
                    new ShapeLeafNode(shape.keyValue.get, newShape)
                  } else {
                    shape
                  }
                )
              }
          }
        })
      }
    }
  }

  def saveSimplifiedFeatures(featureStore: AbstractDataStore,  world: ShapeTrieNode) = {
    val transaction = new DefaultTransaction("addShapes")
    val schema = featureStore.getSchema(featureStore.getNames.get(0))
    val writer = featureStore.getFeatureWriterAppend(schema.getTypeName.asInstanceOf[String], transaction)

    def addFeature(poly: Geometry, tz: String, path: String) = {
      val feat = writer.next()
      feat.setAttributes(Array[Object](poly,tz,path))
      writer.write()
    }

    def enumerateFeatures(cell: ShapeTrieNode, path: String): Unit = cell.keyValue match {
      // if leaf (no subgrid or sublist), use shape
      case Some(tz) => addFeature(cell.shape, tz, path)
      case None =>
        cell.subGrid match {
          case Some(grid) => {
             for{ longIdx <- 0 until cell.subGridSize._1
                  latIdx  <- 0 until cell.subGridSize._2 }
                    enumerateFeatures(grid(longIdx)(latIdx), (path + longIdx + "," + latIdx + ";"))
          }
          case None => cell.subList.foreach(keyShape => addFeature(keyShape.shape, keyShape.keyValue.get, path))
        }
    }

    enumerateFeatures(world, "")
    writer.close()
    transaction.commit()
    transaction.close()

  }

  def createSimplifiedFeatureStore( simplified: File,
                                    originalSource: SimpleFeatureSource,
                                    keyAttribute: String,
                                    simplifiedKeyAttribute: String,
                                    levels: Array[Int]): AbstractDataStore = {
    // Building Datasources:
    // http://docs.geotools.org/stable/userguide/examples/crslab.html
    // Attrs/Features:
    // http://docs.geotools.org/latest/userguide/library/main/feature.html
    val storeFactory: DataStoreFactorySpi = new ShapefileDataStoreFactory()
    val create = Map( "url" -> simplified.toURI.toURL)
    val saveStore = storeFactory.createNewDataStore(create.asJava)
    val oldSchema = originalSource.getSchema
    val descriptorList:java.util.List[AttributeDescriptor] = new java.util.ArrayList[AttributeDescriptor]()
    var index: Int = 0
    descriptorList.add(oldSchema.getGeometryDescriptor)

    // Rename the key attribute, if applicable
    val originalKeyAttribute = oldSchema.getDescriptor(keyAttribute)
    val keyTB = new AttributeTypeBuilder()
    keyTB.setName(simplifiedKeyAttribute)
    keyTB.setBinding(classOf[String])
    keyTB.setNillable(originalKeyAttribute.isNillable)
    descriptorList.add(keyTB.buildDescriptor(simplifiedKeyAttribute))

    val indexAttribute = ShapefileGeo.indexAttributePrefix + levels.mkString("_")
    if (indexAttribute.length > 10) {
      throw new IllegalArgumentException("Stringification of levels is too long"+
        " for DBF format. Index "+indexAttribute+
        " must be less than or equal to length 10")
    }


    // Finally, add the new index parameter
    val indexTB = new AttributeTypeBuilder()
    indexTB.setName(indexAttribute)
    indexTB.setBinding(classOf[String])
    indexTB.setNillable(false)
    descriptorList.add(indexTB.buildDescriptor(indexAttribute))

    val newSchema = new SimpleFeatureTypeImpl(new NameImpl(simplified.getName),
                                              descriptorList,
                                              oldSchema.getGeometryDescriptor,
                                              oldSchema.isAbstract,
                                              oldSchema.getRestrictions,
                                              oldSchema.getSuper,
                                              null)
    saveStore.createSchema(newSchema)
    saveStore.asInstanceOf[AbstractDataStore]
  }

  def getFeatureSource(file: File): SimpleFeatureSource = {
    val url = file.toURI.toURL
    val dataStoreParams: java.util.Map[String, Serializable] = new java.util.HashMap[String, Serializable]()
    dataStoreParams.put(ShapefileDataStoreFactory.URLP.key, url)
    dataStoreParams.put(ShapefileDataStoreFactory.MEMORY_MAPPED.key, java.lang.Boolean.TRUE)
    dataStoreParams.put(ShapefileDataStoreFactory.CACHE_MEMORY_MAPS.key, java.lang.Boolean.FALSE)
    val dataStoreFactory: DataStoreFactorySpi = new ShapefileDataStoreFactory()
    val dataStore = dataStoreFactory.createDataStore(dataStoreParams).asInstanceOf[FileDataStore]
    dataStore.getFeatureSource()
  }


  def loadOriginal( featureSource: SimpleFeatureSource,
                    keyAttribute: String,
                    keyMap: Option[Map[String, String]]) = {
    // determine the key, index, attribute names, and the number and size of the index levels
    if (featureSource.getSchema.getDescriptor(keyAttribute) == null)
      throw new IllegalArgumentException("Schema has no attribute named \"%s\"".format(keyAttribute))

    // build the world
    val bounds = featureSource.getInfo.getBounds
    val world = new ShapeTrieNode( 0, GeoBounds(bounds.getMinX,
                                                bounds.getMinY,
                                                bounds.getWidth,
                                                bounds.getHeight))
    val iterator = featureSource.getFeatures.features

    try {
      while (iterator.hasNext) {
        val feature = iterator.next()
        val sourceGeometry = feature.getDefaultGeometry().asInstanceOf[Geometry]
        val origKeyValue = feature.getAttribute(keyAttribute).toString
        val keyValue = keyMap match {
          case Some(map) => {
            map.getOrElse(origKeyValue,
                          throw new IllegalArgumentException("Can't find %s in map".format(origKeyValue)))
          }
          case None => origKeyValue
        }
        world.subList ::= new ShapeLeafNode(keyValue, sourceGeometry)
      }
    } finally {
      iterator.close()
    }

    world
  }

  def doSimplification( original: File,
                        simplified: File,
                        keyAttribute: String,
                        levels: Array[Int],
                        keyMap: Option[Map[String, String]],
                        newKeyAttribute: Option[String]){
    val simplifiedKeyAttribute = newKeyAttribute.getOrElse(keyAttribute)
    val originalSource = getFeatureSource(original)

    val world = loadOriginal(originalSource, keyAttribute, keyMap)
    simplify(world, levels)

    val featureStore = createSimplifiedFeatureStore(simplified,
                                                    originalSource,
                                                    keyAttribute,
                                                    simplifiedKeyAttribute,
                                                    levels)
    saveSimplifiedFeatures(featureStore, world)
  }

  def main(args: Array[String]) = {
    if (args.length < 3){
      println("Error: you didn't specify the correct number of parmeters!")
      println("run-class com.foursquare.batch.ShapefileSimplifier \n"+
        "\t<original-shapefile.shp>\n"+
        "\t<simplified-shapefile.shp>\n"+
        "\t<key-name>\n"+
        "\t[underscore-separated-levels]")
      println("Example:")
      println("run-class com.foursquare.batch.ShapefileSimplifier tz_world.shp new/4sq_tz.shp TZID 40_2_2_2")
      System.exit(1)
    }

    val original = new File(args(0))
    val simplified = new File(args(1))
    val keyAttribute = args(2)

    if (!original.exists) {
      println(args(0)+" not found!")
      System.exit(1)
    }

    if (simplified.exists) {
      println(args(1)+" already exists! Too scared to overwrite.")
      System.exit(1)
    }

    val levels = if (args.length > 3) {
      args(3).split("_").map(_.toInt)
    } else {
      defaultLevels
    }

    doSimplification(original, simplified, keyAttribute, levels, None, None)
  }
}
