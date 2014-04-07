package com.foursquare.twofishes

import com.foursquare.twofishes.mongo.MongoGeocodeDAO
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._
import com.vividsolutions.jts.geom.{GeometryFactory, MultiPolygon, Polygon}
import com.vividsolutions.jts.io.WKBReader
import java.io.File
import org.geotools.data.{DataUtilities, DefaultTransaction}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureStore}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.feature.simple.SimpleFeatureType
import scalaj.collection.Imports._


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
          case problem: Throwable => {
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
    val collection = new ListFeatureCollection(TYPE)

    val total = MongoGeocodeDAO.count(MongoDBObject("hasPoly" -> true))

    val records =
      MongoGeocodeDAO.find(MongoDBObject("hasPoly" -> true))

    val geomFactory = new GeometryFactory()
    val wkbReader = new WKBReader()

    for {
      (record, index) <- records.zipWithIndex
      if (record.woeType != YahooWoeType.ADMIN2)
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
      featureBuilder.add(record.toGeocodeServingFeature.feature.longId + "," + record._woeType.toString)
      val feature = featureBuilder.buildFeature(null)
      collection.add(feature)
    }
    collection
  }
}
