package com.foursquare.twofishes

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

import com.foursquare.geo.{SimplifierOptions, ShapefileSimplifier, ShapefileGeo}

import org.geotools.data.DataUtilities
import org.geotools.data.DefaultTransaction
import org.geotools.data.Transaction
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature.FeatureCollections
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Geometry}
import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.geom.MultiPolygon
import com.vividsolutions.jts.io.{WKTWriter, WKBReader}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.simple.SimpleFeatureType

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.GeometryFactory

import scalaj.collection.Imports._

import java.io.File
class BuildPolygonShapefile(basepath: String) {
  val TYPE: SimpleFeatureType = DataUtilities.createType("Location",
    "location:MultiPolygon:srid=4326," + // <- the geometry attribute: Point type
    "id:String" // <- a String attribute
  )

  val tmpOutputFilename = "polys.shp"
  val simplifiedOutputFilename = "polys_simplified.shp"

  def testLoadRevgeo(filename: String) {
    ShapefileGeo.load(new File(simplifiedOutputFilename), "id", None, "")
  }

  def outputSimplified() {
    val coll = buildCollection()
    println("writing to " + tmpOutputFilename)
    writeCollection(coll, tmpOutputFilename)
    println("simplifying to " + simplifiedOutputFilename)
    new ShapefileSimplifier(SimplifierOptions(
      outputSingleFeatureCellsWithGeometry=true)).doSimplification(
      new File(tmpOutputFilename),
      new File(simplifiedOutputFilename),
      "id",
      ShapefileSimplifier.defaultLevels,
      None,
      None)
  }

  def writeCollection(collection: SimpleFeatureCollection, filename: String) {
      val newFile = new File(filename);
      
      val storeFactory = new ShapefileDataStoreFactory()
      val create = Map( "url" -> newFile.toURI.toURL)
      val newDataStore = storeFactory.createNewDataStore(create.asJava).asInstanceOf[ShapefileDataStore]

      newDataStore.createSchema(TYPE)

      /*
       * You can comment out this line if you are using the createFeatureType method (at end of
       * class file) rather than DataUtilities.createType
       */
      newDataStore.forceSchemaCRS(DefaultGeographicCRS.WGS84);

      val transaction = new DefaultTransaction("create");

      val typeName = newDataStore.getTypeNames()(0)
      val featureSource = newDataStore.getFeatureSource(typeName)

      if (featureSource.isInstanceOf[SimpleFeatureStore]) {
        val featureStore = featureSource.asInstanceOf[SimpleFeatureStore]
        featureStore.setTransaction(transaction);
        try {
            featureStore.addFeatures(collection);
            transaction.commit();

        } catch {
          case problem => {
            problem.printStackTrace();
            transaction.rollback();
          }
        } finally {
            transaction.close();
        }
      } else {
        System.out.println(typeName + " does not support read/write access");
      }
    }

  def buildCollection(): SimpleFeatureCollection = {
    val featureBuilder = new SimpleFeatureBuilder(TYPE)
    val collection = FeatureCollections.newCollection()

    val records = 
      MongoGeocodeDAO.find(MongoDBObject("polygon" -> MongoDBObject("$exists" -> true)))
      .limit(100)

    val geomFactory = new GeometryFactory()
    val wkbReader = new WKBReader()

    for {
      (record, index) <- records.zipWithIndex
      polygon <- record.polygon
    } {
      val geom = wkbReader.read(polygon)

      var multiPolygon = geom
      if (geom.isInstanceOf[Polygon]) {
        println("converting polygon to multipolygon")
        multiPolygon = new MultiPolygon(Array(geom.asInstanceOf[Polygon]), geomFactory)
      }

      featureBuilder.add(multiPolygon)
      featureBuilder.add(record.toGeocodeServingFeature.id)
      val feature = featureBuilder.buildFeature(null)
      collection.add(feature)
    }
    collection
  }
}