 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import collection.JavaConverters._
import com.twitter.finagle.builder.{ServerBuilder, Server}
import com.twitter.finagle.http.Http
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.finagle.Service
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

class GeocodeServerImpl(store: GeocodeStorageFutureReadService) extends Geocoder.ServiceIface {
  def geocode(r: GeocodeRequest): Future[GeocodeResponse] = {
    new GeocoderImpl(store).geocode(r)
  }
}

class GeocoderHttpService(geocoder: GeocodeServerImpl) extends Service[HttpRequest, HttpResponse] {
  val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(8))

  def handleQuery(request: GeocodeRequest): Future[DefaultHttpResponse] = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    geocoder.geocode(request).map(geocode => {
      val serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
      val json = serializer.toString(geocode);

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
      (for {
        queries <- params.get("query")
        query <- queries.asScala.lift(0)
      } yield { 
        val request = new GeocodeRequest(query)
        params.get("lang").foreach(_.asScala.headOption.foreach(v =>
          request.setLang(v)))
        params.get("cc").foreach(_.asScala.headOption.foreach(v =>
          request.setCc(v)))
        params.get("full").foreach(_.asScala.headOption.foreach(v =>
          request.setFull(v.toBoolean)))
        params.get("ll").foreach(_.asScala.headOption.foreach(v => {
          val ll = v.split(",").toList
          request.setLl(new GeocodePoint(ll(0).toDouble, ll(1).toDouble))
        }))

        handleQuery(request)
      }).getOrElse({
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
        Future.value(response)
      })
    }
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
  class GeocodeServer(store: GeocodeStorageFutureReadService) extends Geocoder.Iface {
    override def geocode(request: GeocodeRequest): GeocodeResponse = {
      new GeocoderImpl(store).geocode(request).get
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

    LogHelper.init

    val config = new GeocodeServerConfig(args)

    // Implement the Thrift Interface
    val processor = new GeocodeServerImpl(ServerStore.getStore(config))

    // Convert the Thrift Processor to a Finagle Service
    val service = new Geocoder.Service(processor, new TBinaryProtocol.Factory())

    println("serving finagle-thrift on port %d".format(config.thriftServerPort))
    println("serving http/json on port %d".format(config.thriftServerPort + 1))

    val server: Server = ServerBuilder()
      .bindTo(new InetSocketAddress(config.thriftServerPort))
      .codec(ThriftServerFramedCodec())
      .name("geocoder")
      .build(service)

    if (config.runHttpServer) {
      ServerBuilder()
        .bindTo(new InetSocketAddress(config.thriftServerPort + 1))
        .codec(Http())
        .name("geocoder-http")
        .build(new GeocoderHttpService(processor))
    }
  }
}


