 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import collection.JavaConverters._
import com.twitter.finagle.builder.{ServerBuilder, Server}
import com.twitter.finagle.http.Http
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Future, FuturePool, Throw}
import java.io.InputStream
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import org.apache.thrift.protocol.{TBinaryProtocol, TSimpleJSONProtocol}
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.TSerializer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil
import scala.collection.mutable.ListBuffer

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

class GeocoderHttpService(geocoder: GeocodeRequest => Future[GeocodeResponse]) extends Service[HttpRequest, HttpResponse] {
  val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(8))

  def handleQuery(request: GeocodeRequest): Future[DefaultHttpResponse] = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    geocoder(request).map(geocode => {
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
      val request = new GeocodeRequest()
      params.get("query").foreach(_.asScala.headOption.foreach(request.setQuery))
      params.get("slug").foreach(_.asScala.headOption.foreach(request.setSlug))
      
      params.get("lang").foreach(_.asScala.headOption.foreach(v =>
        request.setLang(v)))
      params.get("cc").foreach(_.asScala.headOption.foreach(v =>
        request.setCc(v)))
      params.get("full").foreach(_.asScala.headOption.foreach(v =>
        request.setFull(v.toBoolean)))
      params.get("debug").foreach(_.asScala.headOption.foreach(v =>
        request.setDebug(v.toInt)))
      params.get("autocomplete").foreach(_.asScala.headOption.foreach(v =>
        request.setAutocomplete(v.toBoolean)))
      params.get("includePolygon").foreach(_.asScala.headOption.foreach(v =>
        request.setAutocomplete(v.toBoolean)))
      params.get("wktGeometry").foreach(_.asScala.headOption.foreach(v =>
        request.setWktGeometry(v.toBoolean)))
      params.get("ll").foreach(_.asScala.headOption.foreach(v => {
        val ll = v.split(",").toList
        request.setLl(new GeocodePoint(ll(0).toDouble, ll(1).toDouble))
      }))
      params.get("woeHint").foreach(_.asScala.headOption.foreach(hintStr => {
        val hints = hintStr.split(",").map(_.toInt).map(YahooWoeType.findByValue)
        request.setWoeHint(hints.toList.asJava)
      }))
      params.get("woeRestrict").foreach(_.asScala.headOption.foreach(hintStr => {
        val hints = hintStr.split(",").map(_.toInt).map(YahooWoeType.findByValue)
        request.setWoeRestrict(hints.toList.asJava)
      }))

      handleQuery(request)
    }
    // ).getOrElse({
    //     val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
    //     Future.value(response)
    //   })
    // }
  }
}

object ServerStore {
  val ioFuturePool = FuturePool(Executors.newFixedThreadPool(24))

  def getStore(config: GeocodeServerConfig): GeocodeStorageFutureReadService = {
    getStore(config.hfileBasePath)
  }

  def getStore(path: String): GeocodeStorageFutureReadService = {
    new WrappedGeocodeStorageFutureReadService(
      new HFileStorageService(path),
      ioFuturePool)
  }
}

object GeocodeThriftServer extends Application {
  def geocoder(store: GeocodeStorageFutureReadService)(request: GeocodeRequest): Future[GeocodeResponse] = {
    new GeocoderImpl(store, request).geocode()
  }


  class GeocodeServer(store: GeocodeStorageFutureReadService) extends Geocoder.Iface {
    override def geocode(request: GeocodeRequest): GeocodeResponse = {
      geocoder(store)(request).get
    }
  }

  override def main(args: Array[String]) {
    try {
      val config = new GeocodeServerConfig(args)
      val store = ServerStore.getStore(config)

      val serverTransport = new TServerSocket(config.thriftServerPort)
      val processor = new Geocoder.Processor(new GeocodeServer(store))
      val protFactory = new TBinaryProtocol.Factory(true, true)

      val server = new TThreadPoolServer(
          new TThreadPoolServer.Args(serverTransport).processor(processor));

      println("serving vanilla thrift on port %d".format(config.thriftServerPort))
      println("serving http/json on port %d".format(config.thriftServerPort + 1))

      ServerBuilder()
        .bindTo(new InetSocketAddress(config.thriftServerPort + 1))
        .codec(Http())
        .name("geocoder-http")
        .build(new GeocoderHttpService(geocoder(store)))

      server.serve();     
    } catch { 
      case x: Exception => x.printStackTrace();
    }
  }
}
