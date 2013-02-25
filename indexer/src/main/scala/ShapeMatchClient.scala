package com.foursquare.twofishes

import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes.util.Helpers
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import com.vividsolutions.jts.util.GeometricShapeFactory
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.factory.Hints
import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.geotools.feature.simple.SimpleFeatureTypeImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.opengis.feature.simple.SimpleFeature
import scala.collection.JavaConverters._
import org.opengis.feature.`type`.{AttributeDescriptor, AttributeType}
import com.foursquare.geo.UsStateCodes
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

object CommandLineMatcher {
  def main(args: Array[String]) {
    var port: Int = -1
    var host: String = null
    var inputFilename: String = null
    var outputFilename: String = null
    var queryColumnNames: List[String] = null
    var woeRestricts: Option[List[YahooWoeType]] = None
    var querySuffix: Option[String] = None
    var parserName = "generic"
    var limit: Option[Int] = None

    val parser = 
      new scopt.OptionParser("twofishes", "0.12") {
        intOpt("p", "port", "port twofishes server is running on",
          { v: Int => port = v } )

        opt("h", "host", "twofishes host",
          { v: String => host = v } )

        opt("i", "input", "input shapefile",
          { v: String => inputFilename = v } )

        opt("o", "ouput", "ouput shapefile",
          { v: String => outputFilename = v } )

        opt("s", "query_suffix", "string to append to all queries, like a countrycode",
          { v: String => querySuffix = Some(v) } )

        opt("c", "colnames", "comma separated list of column names to join to make geocode query",
          { v: String => queryColumnNames = v.split(",").toList } )

        opt("w", "woe_restricts", "comma separated list of woe type names to restrict searches (like SUBURB,TOWN or 22,7)",
          { v: String => woeRestricts = Some(v.split(",").toList.map(v =>
            Helpers.TryO ( v.toInt ) match {
              case Some(intV) => YahooWoeType.findByValue(intV)
              case None => YahooWoeType.valueOf(v)
            }
          ))
        })

        opt("parser", "custom parser to use for makign queries. avail: nps",
          { v: String => parserName = v } )

        intOpt("l", "limit", "limit on the number of shapes to process",
          { v: Int => limit = Some(v) } )
      }

    if (!parser.parse(args)) {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
    }

    inputFilename = inputFilename.replace("~", System.getProperty("user.home"))
    outputFilename = outputFilename.replace("~", System.getProperty("user.home"))

    if (parserName == "nps") {
      new NPSDataMatcher(new GeocoderFinagleThiftClient(host, port).client,
        inputFilename, outputFilename).process(limit)
    } else {
      new CommandLineMatcher(
        new GeocoderFinagleThiftClient(host, port).client,
          inputFilename, outputFilename, queryColumnNames, woeRestricts, querySuffix
      ).process(limit)
    }
    println("exiting ....")
    System.exit(0)
  }
}

class CommandLineMatcher(
    client: Geocoder.ServiceToClient,
    inputFilename: String,
    outputFilename: String,
    queryColumnNames: List[String],
    woeRestricts: Option[List[YahooWoeType]],
    querySuffix: Option[String]
  ) extends BasicGeocodeMatcher(client, inputFilename, outputFilename) {

  override def getWoeRestrict(shp: SimpleFeature) = woeRestricts

  override val getQueryColumnsNames = queryColumnNames

  override def getQuery(f: SimpleFeature) = {
    super.getQuery(f).map(query => {
      querySuffix match {
        case Some(suffix) => query + " " + suffix
        case None => query
      }
    })
  }
}

class NPSDataMatcher(client: Geocoder.ServiceToClient, inputFilename: String, outputFilename: String)
    extends BasicGeocodeMatcher(client, inputFilename, outputFilename) {
  override val getQueryColumnsNames = List("UNIT_NAME", "UNIT_TYPE", "STATE")

  override def getQuery(f: SimpleFeature) = {
    var query = f.propMap("UNIT_NAME")
    var parkType = f.propMap("UNIT_TYPE")
    if (!query.contains(parkType)) {
      query += " " + parkType
    }

    query += " " + f.propMap("STATE")

    Some(query)
  }

  override def getWoeRestrict(f: SimpleFeature) = Some(List(YahooWoeType.PARK))
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

  def findMatchingInterpretations(shp: SimpleFeature, response: GeocodeResponse): (List[GeocodeInterpretation], List[String]) = {
    var messages: List[String] = Nil
    val matches = (for {
      interpretation <- response.interpretations.asScala.toList
      val feature = interpretation.feature
      if interpretation.what.isEmpty
      if feature.ids.asScala.exists(_.source == "geonameid")
    } yield {
      // check if the feature overlaps / is near
      val shpGeom = shp.getDefaultGeometry().asInstanceOf[Geometry]

      // make a 1km circle to give us a buffer around the edge
      val coord = new Coordinate(
        feature.geometry.center.lng,
        feature.geometry.center.lat
      )

      val crs = shp.getFeatureType().getCoordinateReferenceSystem()
      // val hints = new Hints(Hints.DEFAULT_COORDINATE_REFERENCE_SYSTEM , crs)
      val hints = new Hints(Hints.CRS , crs)
      val geomFactory = JTSFactoryFinder.getGeometryFactory(hints)

      val sizeDegrees = 10000 / 111319.9
      val gsf = new GeometricShapeFactory(geomFactory)
      gsf.setSize(sizeDegrees)
      gsf.setNumPoints(100)
      gsf.setCentre(coord)
      val testGeom = gsf.createCircle()

      if (shpGeom.intersects(testGeom)) {
        Some(interpretation)
      } else {
        messages ::= ("\tcontainment failed for %s (%s, %s) %s in %s".format(getQuery(shp),
            feature.geometry.center.lat,
           feature.geometry.center.lng,
           testGeom,        
           shp.getDefaultGeometry()))
        None
      }
    }).flatMap(a => a)

    (matches, messages)
  }

  def process(limit: Option[Int] = None) {
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

    iter.take(limit.getOrElse(iter.size)).zipWithIndex.foreach({ case (shp, index) => {
      if (index % 100 == 0) {
        println("finished %d of %d shapes".format(index, iter.size))
      }
      for {
        query <- getQuery(shp)
      } {
        val req = new GeocodeRequest()
        req.setAllowedSources(List("geonameid").asJava)
        req.setQuery(query)
        getWoeRestrict(shp).foreach(l => req.setWoeRestrict(l.asJava))
        getLatLngHint(shp).foreach(req.setLl)

        val geonameids: List[String] = {
          Helpers.TryO(client.geocode(req).get).toList.flatMap(response => {
            val (matchingInterpretations, messages) = findMatchingInterpretations(shp, response)
            if (matchingInterpretations.isEmpty) {
              println(messages.mkString("\n"))
            }
            matchingInterpretations.flatMap(_.feature.ids.asScala.lift(0).map(_.id))
          })
        }

        if (!geonameids.isEmpty) {
          val writeFeature = outFeatureWriter.next()
          val attributes = shp.getAttributes()
          //println(attributes)
          attributes.add(geonameids.mkString)
          //println(attributes.size)
          //println(attributes)
          writeFeature.setAttributes(attributes)
          //writeFeature.setAttribute("geonameid", geonameid)
          outFeatureWriter.write()
        } else {
          println("failed to match " + req)
        }
      }
    }})
    println("writing!")
    outFeatureWriter.close()
    println("DONE!")
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
