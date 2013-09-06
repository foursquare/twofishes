//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import org.apache.thrift.{TBase, TFieldIdEnum}
import com.foursquare.common.thrift.json.TReadableJSONProtocol
import com.foursquare.twofishes.util.Helpers
import com.foursquare.twofishes.util.Lists.Implicits._
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http.Http
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.ostrich.admin._
import com.twitter.ostrich.admin.config._
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Future, FuturePool, RingBuffer}
import java.io.InputStream
import java.net.InetSocketAddress
import java.util.Date
import java.util.concurrent.{ConcurrentHashMap, Executors}
import org.apache.thrift.{TBase, TFieldIdEnum, TSerializer}
import org.apache.thrift.protocol.{TBinaryProtocol, TSimpleJSONProtocol}
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import org.bson.types.ObjectId
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil
import scala.collection.mutable.ListBuffer
import scalaj.collection.Implicits._

class QueryLogHttpHandler(
  queryMap: ConcurrentHashMap[ObjectId, (TBase[_, _], Long)],
  recentQueries: Seq[(TBase[_, _], Long, Long)],
  slowQueries: Seq[(TBase[_, _], Long, Long)]
) extends Service[HttpRequest, HttpResponse] {
  def apply(request: HttpRequest) = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val currentTime = System.nanoTime

    val content = (queryMap.asScala.map({case (k, v) => {
      "Request has taken %dms so far\n%s".format((currentTime - v._2) / 1000000, v._1)
    }}).mkString("\n") +
    "\n-----------------------------------------\n" + "SLOW QUERIES\n"
      + slowQueries.reverse.map({case ((req, start, end)) => {
        "Query took %d ms --  started at %s, ended at %s\n%s".format(
          (end - start) / 1000000,
          new Date(start / 1000000),
          new Date(end / 1000000),
          req
        )
      }}).mkString("\n") +
      "\n-----------------------------------------\n" + "RECENT QUERIES\n"
      + recentQueries.reverse.map({case ((req, start, end)) => {
        "Query took %d ms --  started at %s, ended at %s\n%s".format(
          (end - start) / 1000000,
          new Date(start / 1000000),
          new Date(end / 1000000),
          req
        )
      }}).mkString("\n")
    )

    response.setHeader("Content-Type", "text/plain")
    response.setContent(ChannelBuffers.copiedBuffer(content, CharsetUtil.UTF_8))
    Future.value(response)
  }
}

class QueryLoggingGeocodeServerImpl(service: Geocoder.ServiceIface) extends Geocoder.ServiceIface {
  val queryMap = new ConcurrentHashMap[ObjectId, (TBase[_, _], Long)]

  val recentQueries = new RingBuffer[(TBase[_, _], Long, Long)](1000)
  val slowQueries = new RingBuffer[(TBase[_, _], Long, Long)](1000)

  val slowQueryHttpHandler = new QueryLogHttpHandler(queryMap, recentQueries, slowQueries)

  def queryLogProcessor[Req <: TBase[_, _], Res <: TBase[_, _]](r: Req, f: (Req => Future[Res])): Future[Res] = {
    // log the start of this query
    val start = System.nanoTime
    val id = new ObjectId()
    queryMap.put(id, (r, start))

    def logCompletion() {
      val end = System.nanoTime
      // greater than 1 second
      if (end - start > (1000*1000*1000)) {
        // log slow query
        println("%s took %dns %d ms".format(r, end - start, (end - start) / 1000000))
        slowQueries.synchronized {
          slowQueries += (r, start, end)
        }
      }
      recentQueries.synchronized {
        recentQueries += (r, start, end)
      }
      queryMap.remove(id)
    }

    f(r) ensure { logCompletion }
  }

  def geocode(r: GeocodeRequest): Future[GeocodeResponse] =
    queryLogProcessor(r, service.geocode)

  def reverseGeocode(r: GeocodeRequest): Future[GeocodeResponse] =
    queryLogProcessor(r, service.reverseGeocode)

  def bulkReverseGeocode(r: BulkReverseGeocodeRequest): Future[BulkReverseGeocodeResponse] = {
    queryLogProcessor(r, service.bulkReverseGeocode)
  }

  def bulkSlugLookup(r: BulkSlugLookupRequest): Future[BulkSlugLookupResponse] = {
    queryLogProcessor(r, service.bulkSlugLookup)
  }
}

class GeocodeServerImpl(store: GeocodeStorageReadService) extends Geocoder.ServiceIface {
  val queryFuturePool = FuturePool(StatsWrappedExecutors.create(24, 100, "geocoder"))

  def geocode(r: GeocodeRequest): Future[GeocodeResponse] = queryFuturePool {
    new GeocodeRequestDispatcher(store).geocode(r)
  }

  def reverseGeocode(r: GeocodeRequest): Future[GeocodeResponse] = queryFuturePool {
    new ReverseGeocoderImpl(store, r).reverseGeocode()
  }

  def bulkReverseGeocode(r: BulkReverseGeocodeRequest): Future[BulkReverseGeocodeResponse] = queryFuturePool {
    new BulkReverseGeocoderImpl(store, r).reverseGeocode()
  }

  def bulkSlugLookup(r: BulkSlugLookupRequest): Future[BulkSlugLookupResponse] = queryFuturePool {
    new BulkSlugLookupImpl(store, r).slugLookup()
  }
}

class HandleExceptions extends SimpleFilter[HttpRequest, HttpResponse] {
  def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
    // `handle` asynchronously handles exceptions.
    service(request) handle {
      case error: Exception =>
        println("got error: %s".format(error))
        error.printStackTrace
        val statusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR
        val errorResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, statusCode)
        errorResponse.setContent(ChannelBuffers.copiedBuffer(error.toString + "\n" + error.getStackTraceString, CharsetUtil.UTF_8))
        errorResponse
    }
  }
}

class GeocoderHttpService(geocoder: Geocoder.ServiceIface) extends Service[HttpRequest, HttpResponse] {
  val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(8))

  def handleGeocodeQuery(request: GeocodeRequest) =
    handleQuery(request, geocoder.geocode)

  def handleReverseGeocodeQuery(request: GeocodeRequest) =
    handleQuery(request, geocoder.reverseGeocode)

  def handleBulkReverseGeocodeQuery(params: CommonGeocodeRequestParams, points: Seq[(Double, Double)]) = {
    val request = BulkReverseGeocodeRequest.newBuilder
      .params(params)
      .latlngs(points.map(ll => GeocodePoint(ll._1, ll._2)))
      .result
    handleQuery(request, geocoder.bulkReverseGeocode)
  }

  def handleBulkSlugLookupQuery(params: CommonGeocodeRequestParams, slugs: Seq[String]) = {
    val request = BulkSlugLookupRequest.newBuilder
      .params(params)
      .slugs(slugs)
      .result
    handleQuery(request, geocoder.bulkSlugLookup)
  }

  def handleQuery[T, TType <: TBase[_ <: TBase[_ <: AnyRef, _ <: TFieldIdEnum], _ <: TFieldIdEnum]](
      request: T,
      queryProcessor: T => Future[TType]
  ): Future[DefaultHttpResponse] = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    queryProcessor(request).map(geocode => {
      val serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
      val json = serializer.toString(geocode);

      //"longId":72057594044179937
      // javascript can't deal with longs, so we hack it to be a string
      val fixedJson = """"longId":(\d+)""".r.replaceAllIn(json, m => "\"longId\":\"%s\"".format(m.group(1)))

      response.setHeader("Content-Type", "application/json; charset=utf-8")
      response.setContent(ChannelBuffers.copiedBuffer(fixedJson, CharsetUtil.UTF_8))
      response
    })
  }

  def inputStreamToByteArray(is: InputStream): Array[Byte] = {
    val buf = ListBuffer[Byte]()
    var b = is.read()
    while (b != -1) {
        buf.append(b.byteValue)
        b = is.read()
    }
    buf.toArray
  }

  def parseGeocodeRequest(params: Map[String, Seq[String]]): GeocodeRequest = {
    def getOpt(s: String): Option[String] = params.get(s).flatMap(_.headOption)

    val ll = getOpt("ll").map(v => {
      val ll = v.split(",").toList
      GeocodePoint(ll(0).toDouble, ll(1).toDouble)
    })

    val woeHint = getOpt("woeHint").toList.flatMap(hintStr => {
      hintStr.split(",").map(i =>
        if (Helpers.TryO(i.toInt).isDefined) {
          YahooWoeType.findByIdOrNull(i.toInt)
        } else {
          YahooWoeType.findByNameOrNull(i)
        }
      )
    })

    val woeRestrict = getOpt("woeRestrict").toList.flatMap(hintStr => {
      hintStr.split(",").map(i =>
        if (Helpers.TryO(i.toInt).isDefined) {
          YahooWoeType.findByIdOrNull(i.toInt)
        } else {
          YahooWoeType.findByNameOrNull(i)
        }
      )
    })

    val responseIncludes = getOpt("responseIncludes").toList.flatMap(str => {
      str.split(",").toList.map(i => {
        if (Helpers.TryO(i.toInt).isDefined) {
          ResponseIncludes.findByIdOrNull(i.toInt)
        } else {
          ResponseIncludes.findByNameOrNull(i)
        }
      })
    })

    GeocodeRequest.newBuilder
      .query(getOpt("query"))
      .slug(getOpt("slug"))
      .lang(getOpt("lang"))
      .cc(getOpt("cc"))
      .debug(getOpt("debug").map(_.toInt))
      .autocomplete(getOpt("autocomplete").map(_.toBoolean))
      .ll(ll)
      .radius(getOpt("radius").map(_.toInt))
      .maxInterpretations(getOpt("maxInterpretations").map(_.toInt))
      .allowedSources(getOpt("allowedSources").toList.flatMap(_.split(",")))
      .woeHint(woeHint)
      .woeRestrict(woeRestrict)
      .responseIncludes(responseIncludes)
      .result
  }

  def apply(request: HttpRequest) = {
    // This is how you parse request parameters
    val queryString = new QueryStringDecoder(request.getUri())
    val params = queryString.getParameters().asScala.mapValues(_.asScala)
    val path = queryString.getPath()

    if (path.startsWith("/static/")) {
      val dataRead = {
        inputStreamToByteArray(getClass.getResourceAsStream(path))
      }

      diskIoFuturePool(dataRead).map(data => {
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        if (path.endsWith("png")) {
          response.setHeader("Content-Type", "image/png")
        }
        response.setContent(ChannelBuffers.copiedBuffer(data))
        response
      })
    } else {
      val content = request.getContent()
      def getRequest[T <: com.foursquare.spindle.MetaRecord[T]](meta: T): T = {}
      var request = parseGeocodeRequest(params.toMap)
      val

      if (content.readable()) {
        val json = content.toString(CharsetUtil.UTF_8)
        val deserializer = new TDeserializer(new TReadableJSONProtocol.Factory(false))
        val request = GeocodeRequest.createRawRecord
        deserializer.deserialize(conversationEvent, eventJson.getBytes("UTF-8"))
        conversationEvent
        GeocodeR
      } else {

      }


      val commonParams = GeocodeRequestUtils.geocodeRequestToCommonRequestParams(request)
      if (params.getOrElse("method", Nil).has("bulkrevgeo")) {
        handleBulkReverseGeocodeQuery(commonParams, params.getOrElse("ll", Nil).map(v => {
          val ll = v.split(",").toList
          (ll(0).toDouble, ll(1).toDouble)
        }))
      } else if (params.getOrElse("method", Nil).has("bulksluglookup")) {
        handleBulkSlugLookupQuery(commonParams, params.getOrElse("slug", Nil))
      } else if (request.queryOption.isEmpty && request.slugOption.isEmpty) {
        handleReverseGeocodeQuery(request)
      } else {
        handleGeocodeQuery(request)
      }
    }
    // ).getOrElse({
    //     val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
    //     Future.value(response)
    //   })
    // }
  }
}

object ServerStore {
  def getStore(config: GeocodeServerConfig): GeocodeStorageReadService = {
    getStore(config.hfileBasePath, config.shouldPreload)
  }

  def getStore(path: String, shouldPreload: Boolean): GeocodeStorageReadService = {
    new HFileStorageService(path, shouldPreload)
  }
}

// object GeocodeThriftServer extends Application {
//   class GeocodeServer(store: GeocodeStorageReadService) extends Geocoder.Iface {
//     override def geocode(request: GeocodeRequest): GeocodeResponse = {
//       Stats.incr("geocode-requests", 1)
//       new GeocodeRequestDispatcher(store).geocode(request)
//     }

//     override def bulkReverseGeocode(request: BulkReverseGeocodeRequest): BulkReverseGeocodeResponse = {
//       Stats.incr("bulk-geocode-requests", 1)
//       new BulkReverseGeocoderImpl(store, request).reverseGeocode()
//     }

//     override def bulkSlugLookup(request: BulkSlugLookupRequest): BulkSlugLookupResponse = {
//       Stats.incr("bulk-slug-lookups", 1)
//       new BulkSlugLookupImpl(store, request).slugLookup()
//     }

//     override def reverseGeocode(request: GeocodeRequest): GeocodeResponse = {
//       Stats.incr("geocode-requests", 1)
//       new ReverseGeocoderImpl(store, request).reverseGeocode()
//     }
//   }

//   override def main(args: Array[String]) {
//     try {
//       val config = new GeocodeServerConfig(args)

//       val serverTransport = new TServerSocket(config.thriftServerPort)
//       val processor = new Geocoder.Processor(new GeocodeServer(ServerStore.getStore(config)))
//       val protFactory = new TBinaryProtocol.Factory(true, true)
//       val server = new TThreadPoolServer(processor, serverTransport, protFactory)

//       println("serving vanilla thrift on port %d".format(config.thriftServerPort))

//       server.serve();
//     } catch {
//       case x: Exception => x.printStackTrace();
//     }
//   }
// }

object GeocodeFinagleServer {
  def main(args: Array[String]) {
    val handleExceptions = new HandleExceptions

    LogHelper.init

    val config: GeocodeServerConfig = GeocodeServerConfigParser.parse(args)

    // Implement the Thrift Interface
    val processor = new QueryLoggingGeocodeServerImpl(new GeocodeServerImpl(ServerStore.getStore(config)))

    // Convert the Thrift Processor to a Finagle Service
    val service = new Geocoder.Service(processor, new TBinaryProtocol.Factory())

    println("serving finagle-thrift on port %d".format(config.thriftServerPort))
    println("serving http/json on port %d".format(config.thriftServerPort + 1))
    println("serving debug info on port %d".format(config.thriftServerPort + 2))
    println("serving slow query http/json on port %d".format(config.thriftServerPort + 3))

    val server: Server = ServerBuilder()
      .bindTo(new InetSocketAddress(config.thriftServerPort))
      .codec(ThriftServerFramedCodec())
      .reportTo(new FoursquareStatsReceiver)
      .name("geocoder")
      .build(service)

    val adminConfig = new AdminServiceConfig {
      httpPort = config.thriftServerPort + 2
      statsNodes = new StatsConfig {
        reporters = new TimeSeriesCollectorConfig
      }
    }
    val runtime = RuntimeEnvironment(this, Nil.toArray)
    val admin = adminConfig()(runtime)

    if (config.runHttpServer) {
      ServerBuilder()
        .bindTo(new InetSocketAddress(config.thriftServerPort + 1))
        .codec(Http())
        .name("geocoder-http")
        .reportTo(new FoursquareStatsReceiver)
        .build(handleExceptions andThen new GeocoderHttpService(processor))
    }

    ServerBuilder()
      .bindTo(new InetSocketAddress(config.thriftServerPort + 3))
      .codec(Http())
      .name("geocoder-slow-query-http")
      .reportTo(new FoursquareStatsReceiver)
      .build(handleExceptions andThen processor.slowQueryHttpHandler)
  }
}
