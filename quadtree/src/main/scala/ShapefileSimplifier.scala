// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.geo.quadtree

import com.foursquare.geo.quadtree.ShapefileGeo.{ShapeTrieNode, GeoBounds, ShapeLeafNode}
import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import java.io.File
import org.geotools.data._
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.geotools.feature.simple.SimpleFeatureTypeImpl
import org.opengis.feature.`type`.{AttributeDescriptor, AttributeType}
import scalaj.collection.Imports._

object ShapefileSimplifier{
  val defaultLevels = Array(1000,10,4,4)

  object Coords extends Enumeration {
      type Coords = Value
      val BottomLeft, BottomRight, TopRight, TopLeft = Value
  }


  def simplify(node: ShapeTrieNode, levels: Array[Int]) = {
    gridifyList(node)

    def gridifyList(node: ShapeTrieNode): Unit = {


      val nLongs = levels(node.nodeLevel)
      val nLats = levels(node.nodeLevel)
      val longChunk = node.nodeBounds.width/nLongs
      val latChunk = node.nodeBounds.height/nLats

      def toIndex(coord: Coordinate): (Int, Int) = {
        ( ((coord.x - node.nodeBounds.minLong) / longChunk).toInt,
          ((coord.y - node.nodeBounds.minLat)  / latChunk ).toInt )
      }


      // initialize subGrid
      node.makeSubGrid(levels)


      // process shapes into sub-shapes by intersecting the shapes with the grid
      // this greatly reduces the # of points per shape, and contains(lat,lng)
      // is roughly linear to the number of points in the shape, so it speeds the
      // contains() operation sigificantly
      node.subList.foreach(keyShape => {

        // optimization to only consider intersecting Rects within the shape's envelope.
        val env = keyShape.shape.getEnvelope
        // can be point
        assert(env.isRectangle)
        val (minLongIdx, minLatIdx) = toIndex(env.getCoordinates()(Coords.BottomLeft.id))
        val (maxLongIdx, maxLatIdx) = toIndex(env.getCoordinates()(Coords.TopRight.id))

        // Note: this can happen when
        // floating point precision loss/exact boundary point
        val mxLg = if (maxLongIdx >= nLongs) nLongs - 1 else maxLongIdx
        val mxLt = if (maxLatIdx >= nLats) nLats - 1 else maxLatIdx

        for{
          longIdx <- minLongIdx to mxLg
          latIdx <- minLatIdx to mxLt}
        {
          val cell = node.subGrid.get(longIdx)(latIdx)
          // Note: may not actually intersect because the shape's envelope is an
          // over approximation of the shape.  We may have found a nook

          val validKeyShape = if (keyShape.shape.isValid()) {
            keyShape.shape
          } else {
            keyShape.shape.buffer(0)
          }

          try {
            if (cell.shape.intersects(validKeyShape)) {
              cell.subList ::= new ShapeLeafNode(keyShape.keyValue.get, cell.shape.intersection(validKeyShape))
            }
          } catch {
            case e: Throwable => {
              println(cell.shape)
              println(cell.shape.isValid())
              println(keyShape)
              println(keyShape.shape)
              println(keyShape.shape.isValid())
              println(keyShape.shape.buffer(0))
              println(keyShape.shape.buffer(0).isValid())
              throw e
            }
          }
        }
      })

      for {
        longIdx <- 0 until nLongs
        latIdx <- 0 until nLats}
      {
        val cell = node.subGrid.get(longIdx)(latIdx)
        gridifyList(cell)
      }

      node.subList = Nil
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
    val dataStore: FileDataStore = FileDataStoreFinder.getDataStore(file)
    dataStore.getFeatureSource()
  }


  def loadOriginal( featureSource: SimpleFeatureSource,
                    keyAttribute: String,
                    keyMap: Option[Map[String, String]]) = {
    // determine the key, index, attribute names, and the number and size of the index levels
    if (featureSource.getSchema.getDescriptor(keyAttribute) == null)
      throw new IllegalArgumentException("Schema has no attribute named \""+keyAttribute+"\"")

    // build the world
    val bounds = featureSource.getInfo.getBounds
    val world = new ShapeTrieNode( 0, GeoBounds(bounds.getMinX,
                                                bounds.getMinY,
                                                bounds.getWidth,
                                                bounds.getHeight), true)
    val iterator = featureSource.getFeatures.features

    try{
      while (iterator.hasNext) {
        val feature = iterator.next()
        val sourceGeometry = feature.getDefaultGeometry().asInstanceOf[Geometry]
        val origKeyValue = feature.getAttribute(keyAttribute).toString
        val keyValue = keyMap match {
          case Some(map) => {
            map.getOrElse(origKeyValue,
                          throw new IllegalArgumentException("Can't find "+origKeyValue+" in map"))
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

    if (!original.exists){
      println(args(0)+" not found!")
      System.exit(1)
    }

    if (simplified.exists){
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
