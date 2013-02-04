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
import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.geotools.feature.simple.SimpleFeatureTypeImpl
import org.opengis.feature.`type`.{AttributeDescriptor, AttributeType}
import java.io.File
import org.geotools.data.Transaction
import org.opengis.feature.Property
import com.foursquare.geo.UsStateCodes

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

class GeocoderFinagleThiftClient(host: String, port: Int) {
  // Create a raw Thrift client service. This implements the
  // ThriftClientRequest => Future[Array[Byte]] interface.
  lazy val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()               // 1
    //.hosts(new InetSocketAddress(8080))
    .hosts(new InetSocketAddress(host, port))
    .codec(ThriftClientFramedCodec())
    .hostConnectionLimit(10)
    //.logger(java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME))
    .connectionTimeout(10.seconds)        // max time to spend establishing a TCP connection
    .build()

  lazy val client: Geocoder.ServiceToClient = new Geocoder.ServiceToClient(service, new TBinaryProtocol.Factory())         // 2
}

class TigerCountyMatcher(client: Geocoder.ServiceToClient, inputFilename: String, outputFilename: String)
    extends BasicGeocodeMatcher(client, inputFilename, outputFilename) {
  override val getQueryColumnsNames = List("NAME")
  override def getQuery(f: SimpleFeature) = {
    val str = f.propMap("NAME") + UsStateCodes.fipsMap(f.propMap("STATEFP").toString.toInt) + "US"
    Some(str)
  }

  override def getWoeRestrict(f: SimpleFeature) = Some(List(YahooWoeType.SUBURB))
}

class ZillowShapeMatcher(client: Geocoder.ServiceToClient, inputFilename: String, outputFilename: String)
    extends BasicGeocodeMatcher(client, inputFilename, outputFilename) {
  override val getQueryColumnsNames = List("NAME", "CITY", "COUNTY", "STATE")
  override def getQuery(f: SimpleFeature) = {
    super.getQuery(f).map(_.replace("New York City-", ""))
  }

  override def getWoeRestrict(f: SimpleFeature) = Some(List(YahooWoeType.SUBURB))
}

class AuthoritativeDataShapeMatcher(client: Geocoder.ServiceToClient, inputFilename: String, outputFilename: String)
    extends BasicGeocodeMatcher(client, inputFilename, outputFilename) {
  override val getQueryColumnsNames = List("qs_name")

  override def getQuery(f: SimpleFeature) = {
    var query = super.getQuery(f).get

    val ccCandidateOpt = inputFilename.split("_").lift(0)
    for {
      ccCandidate <- ccCandidateOpt
      if ccCandidate != "eu"
    } {
      query + " " + ccCandidate
    }

    Some(query)
  }

  override def getWoeRestrict(f: SimpleFeature) = {
    if (inputFilename.contains("adm1")) {
      Some(List(YahooWoeType.ADMIN1))
    } else if (inputFilename.contains("adm2")) {
      Some(List(YahooWoeType.ADMIN2))
    } else if (inputFilename.contains("localities") || inputFilename.contains("adm3")) {
      Some(List(YahooWoeType.TOWN, YahooWoeType.SUBURB, YahooWoeType.ADMIN3))
    } else {
      None
    }
  }
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
    (for {
      interpretation <- response.interpretations.asScala.headOption
      val feature = interpretation.feature
      if interpretation.what.isEmpty
      if feature.ids.asScala.exists(_.source == "geonameid")
    } yield {
      // check if the feature overlaps / is near
      if (shp.getDefaultGeometry().asInstanceOf[Geometry].contains(
        geometryFactory.createPoint(
          new Coordinate(
            feature.geometry.center.lng,
            feature.geometry.center.lat
          )
        )
      )) {
        Some(interpretation)
      } else {
        println("contianment failed for %s".format(getQuery(shp)))
        None
      }
    }).getOrElse(None)
  }

  def process {
    val iter = new ShapefileIterator(inputFilename)

    val outStore = new ShapefileDataStore(new File(outputFilename).toURL())
    val oldSchema = iter.store.getFeatureSource().getSchema
    val descriptorList:java.util.List[AttributeDescriptor] = new java.util.ArrayList[AttributeDescriptor]()
    oldSchema.getDescriptors().asScala.foreach{ case d if d.isInstanceOf[AttributeDescriptor] => descriptorList.add(d.asInstanceOf[AttributeDescriptor]) }
    

    val geonameTB = new AttributeTypeBuilder()
    geonameTB.setName("geonameid")
    geonameTB.setBinding(classOf[String])
    geonameTB.setNillable(false)
    descriptorList.add(geonameTB.buildDescriptor("geonameid"))

    val newSchema = new SimpleFeatureTypeImpl(new NameImpl("twofishesGeometry"),
                                              descriptorList,
                                              oldSchema.getGeometryDescriptor,
                                              oldSchema.isAbstract,
                                              oldSchema.getRestrictions,
                                              oldSchema.getSuper,
                                              null)


    outStore.createSchema(newSchema)
    val outFeatureWriter = outStore.getFeatureWriter(outStore.getTypeNames()(0), Transaction.AUTO_COMMIT);

    iter.take(10).foreach(shp => {
      for {
        query <- getQuery(shp)
      } {
        val req = new GeocodeRequest()
        req.setAllowedSources(List("geonameid").asJava)
        req.setQuery(query)
        getWoeRestrict(shp).foreach(l => req.setWoeRestrict(l.asJava))
        getLatLngHint(shp).foreach(req.setLl)
        // println(req)

        val geonameidOpt = 
          for {
            response <- Option(client.geocode(req).get)
            //response <- Option(responseOpt)
            matchingInterpretation <- findMatchingInterpretation(shp, response)
            geonameid <- matchingInterpretation.feature.ids.asScala.lift(0).map(_.id)
          } yield{
            geonameid
          }

        geonameidOpt match {
          case Some(geonameid) => {
            val writeFeature = outFeatureWriter.next()
            val attributes = shp.getAttributes()
            //println(attributes)
            attributes.add(geonameid)
            //println(attributes.size)
            //println(attributes)
            writeFeature.setAttributes(attributes)
            //writeFeature.setAttribute("geonameid", geonameid)
            outFeatureWriter.write()
          }
          case None => {
            println("failed to match " + req)
          }
        }
      }
    })
    outFeatureWriter.close()
  }
}

object ThriftClient extends GeocoderFinagleThiftClient("localhost", 8080) {
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
