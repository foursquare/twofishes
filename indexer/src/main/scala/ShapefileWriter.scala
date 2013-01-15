package com.foursquare.twofishes

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

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
object BuildPolygonShapefile {
  val TYPE: SimpleFeatureType = DataUtilities.createType("Location",
    "location:MultiPolygon:srid=4326," + // <- the geometry attribute: Point type
    "id:String" // <- a String attribute
  )

  def buildAndWriteCollection(filename: String) {
    writeCollection(buildCollection(), filename)
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
        System.exit(0); // success!
      } else {
        System.out.println(typeName + " does not support read/write access");
        System.exit(1);
      }
    }

  def buildCollection(): SimpleFeatureCollection = {
    val featureBuilder = new SimpleFeatureBuilder(TYPE)
    val collection = FeatureCollections.newCollection()

    val total = MongoGeocodeDAO.count(MongoDBObject("hasPoly" -> true))


    val records = 
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))

    val geomFactory = new GeometryFactory()
    val wkbReader = new WKBReader()

    for {
      (record, index) <- records.zipWithIndex
      polygon <- record.polygon
    } {
      val geom = wkbReader.read(polygon)

      var multiPolygon = geom
      if (geom.isInstanceOf[Polygon]) {
        multiPolygon = new MultiPolygon(Array(geom.asInstanceOf[Polygon]), geomFactory)
      }

      if (index % 10000 == 0) {
        println("outputted %d of %d (%.2f%%)".format(index, total, index*100.0/total))
      }

      featureBuilder.add(multiPolygon)
      featureBuilder.add(record.toGeocodeServingFeature.feature.id + "," + record._woeType.toString)
      val feature = featureBuilder.buildFeature(null)
      collection.add(feature)
    }
    collection
  }
}