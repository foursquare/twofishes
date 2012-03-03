//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.geocoder

import com.twitter.finagle.builder.{ServerBuilder, Server}
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol

class GeocodeServerImpl extends Geocoder.ServiceIface  {
  def geocode(r: GeocodeRequest): Future[GeocodeResponse] = {
    println("Handling request")
    val response = new GeocodeResponse()
    Future.value(response)
  }
}

object GeocodeServer {
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
  }
}

