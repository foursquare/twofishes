package com.foursquare.twofishes

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Service
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol
import org.opengis.feature.simple.SimpleFeature
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import com.twitter.util.{Duration, TimeLike}
import com.twitter.conversions.time._
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.geotools.data.shapefile.ShapefileDataStore
import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import scala.collection.JavaConverters._
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import org.geotools.geometry.jts.JTSFactoryFinder
import java.io.File
import org.geotools.data.Transaction

class GeocoderVanillaThriftClient {
  val transport = new TSocket("datamining-ml-1", 20000);
  val protocol = new TBinaryProtocol(transport);
  val client = new Geocoder.Client(protocol);

  try {
    transport.open();
    val req = new GeocodeRequest()
    req.setQuery("nyc")
    val time = client.geocode(req)
    println(time)
    transport.close();
  } catch {
    case e => println(e)
  }
}

abstract class GeocoderFinagleThiftClient(host: String, port: Int) {
  // Create a raw Thrift client service. This implements the
  // ThriftClientRequest => Future[Array[Byte]] interface.
  lazy val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()               // 1
    //.hosts(new InetSocketAddress(8080))
    .hosts(new InetSocketAddress(host, port))
    .codec(ThriftClientFramedCodec())
    .hostConnectionLimit(10)
    .logger(java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME))
    .connectionTimeout(10.seconds)        // max time to spend establishing a TCP connection
    .build()

  lazy val client: Geocoder.ServiceToClient = new Geocoder.ServiceToClient(service, new TBinaryProtocol.Factory())         // 2
}

class ZillowShapeMatcher(client: Geocoder.ServiceToClient, inputFilename: String, outputFilename: String)
    extends BasicGeocodeMatcher(client, inputFilename, outputFilename) {
  override val getQueryColumnsNames = List("NAME", "CITY", "COUNTY", "STATE")
  override def getWoeRestrict(f: SimpleFeature) = Some(List(YahooWoeType.SUBURB))
}

abstract class BasicGeocodeMatcher(client: Geocoder.ServiceToClient, inputFilename: String, outputFilename: String)
    extends GeocodeMatcher(client, inputFilename, outputFilename) {
  def getQueryColumnsNames: List[String]

  def getQuery(f: SimpleFeature): Option[String] = {
    Some(getQueryColumnsNames.map(colName => f.propMap(colName)).mkString(", "))
  }
  def getLatLngHint(f: SimpleFeature): Option[GeocodePoint] = {
    for {
      geom <- f.geometry
    } yield {
      val center = geom.getEnvelopeInternal().centre()
      new GeocodePoint(center.y, center.x)
    }
  }
}


abstract class GeocodeMatcher(client: Geocoder.ServiceToClient, inputFilename: String, outputFilename: String) {
  def getQuery(f: SimpleFeature): Option[String]
  def getWoeRestrict(f: SimpleFeature): Option[List[YahooWoeType]]
  def getLatLngHint(f: SimpleFeature): Option[GeocodePoint]

   val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)

  def findMatchingInterpretation(shp: SimpleFeature, response: GeocodeResponse) = {
    for {
      interpretation <- response.interpretations.asScala.headOption
      val feature = interpretation.feature
      if interpretation.what.isEmpty
      if feature.ids.asScala.exists(_.source == "geonameid")
      // check if the feature overlaps / is near
      if shp.getDefaultGeometry().asInstanceOf[Geometry].contains(
        geometryFactory.createPoint(
          new Coordinate(
            feature.geometry.center.lng,
            feature.geometry.center.lat
          )
        )
      )
    } yield {
      interpretation
    }
  }

  def process {
    val iter = new ShapefileIterator(inputFilename)

    val outStore = new ShapefileDataStore(new File(outputFilename).toURL())
    outStore.createSchema(iter.store.getFeatureSource().getSchema())
    val outFeatureWriter = outStore.getFeatureWriter(outStore.getTypeNames()(0), Transaction.AUTO_COMMIT);

    iter.foreach(shp => {
      for {
        query <- getQuery(shp)
      } {
        val req = new GeocodeRequest()
        req.setQuery(query)
        getWoeRestrict(shp).foreach(l => req.setWoeRestrict(l.asJava))
        getLatLngHint(shp).foreach(req.setLl)
        println(req)
        for {
          responseOpt <- client.geocode(req)
          response <- Option(responseOpt)
          matchingInterpretation <- findMatchingInterpretation(shp, response)
          geonameid <- matchingInterpretation.feature.ids.asScala.lift(0).map(_.id)
        } {
          val writeFeature = outFeatureWriter.next()
          writeFeature.setAttributes(shp.getAttributes)
          writeFeature.setAttribute("geonameid", geonameid)
          outFeatureWriter.write()
        }
      }
    })
  }
}

object ThriftClient extends GeocoderFinagleThiftClient("prodapp-geocoder-6", 35000) {
  def main(args: Array[String]) {
    // Wrap the raw Thrift service in a Client decorator. The client provides
    // a convenient procedural interface for accessing the Thrift server.

    val req = new GeocodeRequest()
    req.setQuery("nyc")

    client.geocode(req) onSuccess { response =>                                                   // 3
      println("Received response: " + response)
    } ensure {
      service.release()                                                                    // 4
    } onFailure {
      case e => println("failed: " + e)
    }
  }
}
