 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import collection.JavaConverters._
import com.foursquare.twofishes.util.Helpers
import com.twitter.ostrich.admin._
import com.twitter.ostrich.admin.config._
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http.Http
import com.twitter.ostrich.stats.{Stats, StatsProvider}
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.util.{Future, FuturePool}
import java.io.InputStream
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.{TBinaryProtocol, TSimpleJSONProtocol}
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil
import scala.collection.mutable.ListBuffer

class GeocodeServerImpl(store: GeocodeStorageReadService) extends Geocoder.ServiceIface {
  val queryFuturePool = FuturePool(StatsWrappedExecutors.create(24, 100, "geocoder"))

  def geocode(r: GeocodeRequest): Future[GeocodeResponse] = queryFuturePool {
    new GeocoderImpl(store, r).geocode()
  }

  def reverseGeocode(r: GeocodeRequest): Future[GeocodeResponse] = queryFuturePool {
    new GeocoderImpl(store, r).reverseGeocode()
  }
}

class HandleExceptions extends SimpleFilter[HttpRequest, HttpResponse] {
  def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
    // `handle` asynchronously handles exceptions.
    service(request) handle {
      case error =>
        val statusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR
        val errorResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, statusCode)
        errorResponse.setContent(ChannelBuffers.copiedBuffer(error.toString + "\n" + error.getStackTraceString, CharsetUtil.UTF_8))
        errorResponse
    }
  }
}

class GeocoderHttpService(geocoder: GeocodeServerImpl) extends Service[HttpRequest, HttpResponse] {
  val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(8))

  def handleGeocodeQuery(request: GeocodeRequest) = 
    handleQuery(request, geocoder.geocode)

  def handleReverseGeocodeQuery(request: GeocodeRequest) = 
    handleQuery(request, geocoder.reverseGeocode)

  def handleQuery(
      request: GeocodeRequest,
      queryProcessor: (GeocodeRequest) => Future[GeocodeResponse]
  ): Future[DefaultHttpResponse] = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    queryProcessor(request).map(geocode => {
      val serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
      val json = serializer.toString(geocode);

      response.setHeader("Content-Type", "application/json; charset=utf-8")
      response.setContent(ChannelBuffers.copiedBuffer(json, CharsetUtil.UTF_8))
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

  def parseGeocodeRequest(params: scala.collection.mutable.Map[java.lang.String,java.util.List[java.lang.String]]): GeocodeRequest = {
    val request = new GeocodeRequest()
    params.get("query").foreach(_.asScala.headOption.foreach(request.setQuery))
    params.get("slug").foreach(_.asScala.headOption.foreach(request.setSlug))
    
    params.get("lang").foreach(_.asScala.headOption.foreach(v =>
      request.setLang(v)))
    params.get("cc").foreach(_.asScala.headOption.foreach(v =>
      request.setCc(v)))
    params.get("debug").foreach(_.asScala.headOption.foreach(v =>
      request.setDebug(v.toInt)))
    params.get("autocomplete").foreach(_.asScala.headOption.foreach(v =>
      request.setAutocomplete(v.toBoolean)))
    params.get("ll").foreach(_.asScala.headOption.foreach(v => {
      val ll = v.split(",").toList
      request.setLl(new GeocodePoint(ll(0).toDouble, ll(1).toDouble))
    }))
    params.get("woeHint").foreach(_.asScala.headOption.foreach(hintStr => {
      val hints = hintStr.split(",").map(i =>
        if (Helpers.TryO(i.toInt).isDefined) {
          YahooWoeType.findByValue(i.toInt)
        } else {
          YahooWoeType.valueOf(i)
        }
      )
      request.setWoeHint(hints.toList.asJava)
    }))
    params.get("woeRestrict").foreach(_.asScala.headOption.foreach(hintStr => {
      val hints = hintStr.split(",").map(i =>
        if (Helpers.TryO(i.toInt).isDefined) {
          YahooWoeType.findByValue(i.toInt)
        } else {
          YahooWoeType.valueOf(i)
        }
      )
      request.setWoeRestrict(hints.toList.asJava)
    }))
    params.get("radius").foreach(_.asScala.headOption.foreach(v =>
      request.setRadius(v.toInt)))
    params.get("maxInterpretations").foreach(_.asScala.headOption.foreach(v =>
      request.setMaxInterpretations(v.toInt)))
    params.get("allowedSources").foreach(_.asScala.headOption.foreach(str => {
      request.setAllowedSources(str.split(",").toList.asJava)
    }))
    params.get("responseIncludes").foreach(_.asScala.headOption.foreach(str => {
      request.setResponseIncludes(str.split(",").toList.map(i => {
        if (Helpers.TryO(i.toInt).isDefined) {
          ResponseIncludes.findByValue(i.toInt)
        } else {
          ResponseIncludes.valueOf(i)
        }
      }).asJava)
    }))

    request
  }

  def apply(request: HttpRequest) = {
    // This is how you parse request parameters
    val queryString = new QueryStringDecoder(request.getUri())
    val params = queryString.getParameters().asScala
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
      val request = parseGeocodeRequest(params)

      if (request.query == null && request.slug == null) {
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

object GeocodeThriftServer extends Application {
  class GeocodeServer(store: GeocodeStorageReadService) extends Geocoder.Iface {
    override def geocode(request: GeocodeRequest): GeocodeResponse = {
      Stats.incr("geocode-requests", 1)
      new GeocoderImpl(store, request).geocode()
    }

    override def reverseGeocode(request: GeocodeRequest): GeocodeResponse = {
      Stats.incr("geocode-requests", 1)
      new GeocoderImpl(store, request).reverseGeocode()
    }
  }

  override def main(args: Array[String]) {
    try {
      val config = new GeocodeServerConfig(args)

      val serverTransport = new TServerSocket(config.thriftServerPort)
      val processor = new Geocoder.Processor(new GeocodeServer(ServerStore.getStore(config)))
      val protFactory = new TBinaryProtocol.Factory(true, true)
      val server = new TThreadPoolServer(processor, serverTransport, protFactory)
      
      println("serving vanilla thrift on port %d".format(config.thriftServerPort))

      server.serve();     
    } catch { 
      case x: Exception => x.printStackTrace();
    }
  }
}

object GeocodeFinagleServer {
  def main(args: Array[String]) {
    val handleExceptions = new HandleExceptions

    LogHelper.init

    val config = new GeocodeServerConfig(args)

    // Implement the Thrift Interface
    val processor = new GeocodeServerImpl(ServerStore.getStore(config))

    // Convert the Thrift Processor to a Finagle Service
    val service = new Geocoder.Service(processor, new TBinaryProtocol.Factory())

    println("serving finagle-thrift on port %d".format(config.thriftServerPort))
    println("serving http/json on port %d".format(config.thriftServerPort + 1))
    println("serving debug info on port %d".format(config.thriftServerPort + 2))

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
  }
}
