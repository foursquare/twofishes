//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.geocoder

import collection.JavaConverters._
import com.twitter.finagle.builder.{ServerBuilder, Server}
import com.twitter.finagle.http.Http
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.finagle.Service
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.apache.thrift.protocol.{TBinaryProtocol, TSimpleJSONProtocol}
import org.apache.thrift.TSerializer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil

class GeocodeServerImpl extends Geocoder.ServiceIface  {
  def geocode(r: GeocodeRequest): Future[GeocodeResponse] = {
    val response = new GeocoderImpl(new MongoGeocodeStorageService()).geocode(r)
    Future.value(response)
  }
}

class GeocoderHttpService extends Service[HttpRequest, HttpResponse] {
  def handleQuery(query: String, lang: Option[String]) = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    val request = new GeocodeRequest(query)
    lang.map(l => request.setLang(l))
    val geocode = new GeocoderImpl(new MongoGeocodeStorageService()).geocode(request)

    val serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    val json = serializer.toString(geocode);

    response.setContent(ChannelBuffers.copiedBuffer(json, CharsetUtil.UTF_8))
    Future.value(response)
  }

  def apply(request: HttpRequest) = {
    // This is how you parse request parameters
    val params = new QueryStringDecoder(request.getUri()).getParameters().asScala
    val responseContent = params.toString()

    (for {
      queries <- params.get("query")
      query <- queries.asScala.lift(0)
    } yield { 
      val lang = params.get("lang").flatMap(_.asScala.lift(0))
      handleQuery(query, lang)
    }).getOrElse({
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
      Future.value(response)
    })
  }
}

// object GeocodeThriftServer extends Application {
//   class GeocodeServer extends Geocoder.Iface {
//     override def geocode(r: GeocodeRequest): GeocodeResponse = {
//       new GeocoderImpl(new MongoGeocodeStorageService()).geocode(request)
//     }
//   }

//   def main(args: Array[String]) {
//     try {
//       val serverTransport = new TServerSocket(8080)
//       val processor = new TimeServer.Processor(new GeocodeServer())
//       val protFactory = new TBinaryProtocol.Factory(true, true)
//       val server = new TThreadPoolServer(processor, serverTransport, protFactory)
      
//       println("starting server")
//       server.serve();     
//     } catch { 
//       case x: Exception => x.printStackTrace();
//     }
//   }
// }

object GeocodeFinagleServer {
  def main(args: Array[String]) {
    // Implement the Thrift Interface
    val processor = new GeocodeServerImpl()

    // Convert the Thrift Processor to a Finagle Service
    val service = new Geocoder.Service(processor, new TBinaryProtocol.Factory())

    val server: Server = ServerBuilder()
      .bindTo(new InetSocketAddress(8080))
      .codec(ThriftServerFramedCodec())
      .name("geocoder")
      .build(service)

    val server2: Server = ServerBuilder()
      .bindTo(new InetSocketAddress(8081))
      .codec(Http())
      .name("geocoder-http")
      .build(new GeocoderHttpService())


  }
}


