 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofish

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

object Store {
  val store = new InMemoryReadService()
 // val store = new MongoGeocodeStorageService()
}

class GeocodeServerImpl extends Geocoder.ServiceIface {
  val mongoFuturePool = FuturePool(Executors.newFixedThreadPool(24))
  val store = Store.store

  def geocode(r: GeocodeRequest): Future[GeocodeResponse] = {
    new GeocoderImpl(mongoFuturePool, store).geocode(r)
  }
}

class GeocoderHttpService extends Service[HttpRequest, HttpResponse] {
  val mongoFuturePool = FuturePool(Executors.newFixedThreadPool(24))
  val store = Store.store

  val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(8))

  def handleQuery(request: GeocodeRequest): Future[DefaultHttpResponse] = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    new GeocoderImpl(mongoFuturePool, store).geocode(request).map(geocode => {
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

object GeocodeThriftServer extends Application {
  class GeocodeServer extends Geocoder.Iface {
    val mongoFuturePool = FuturePool(Executors.newFixedThreadPool(1))

    override def geocode(request: GeocodeRequest): GeocodeResponse = {
      new GeocoderImpl(mongoFuturePool, new MongoGeocodeStorageService()).geocode(request).get
    }
  }

  override def main(args: Array[String]) {
    try {
      val config = new GeocodeServerConfig(args)

      val serverTransport = new TServerSocket(config.thriftServerPort)
      val processor = new Geocoder.Processor(new GeocodeServer())
      val protFactory = new TBinaryProtocol.Factory(true, true)
      val server = new TThreadPoolServer(processor, serverTransport, protFactory)
      
      println("starting server")
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
    val processor = new GeocodeServerImpl()

    // Convert the Thrift Processor to a Finagle Service
    val service = new Geocoder.Service(processor, new TBinaryProtocol.Factory())

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
        .build(new GeocoderHttpService())
    }
  }
}


