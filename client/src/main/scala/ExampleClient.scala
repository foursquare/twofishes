package com.foursquare.twofishes

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import scala.collection.JavaConverters._

object GeocoderTestClient {
  def main(args: Array[String]) {
    val transport = new TSocket("localhost", 8080);
    val protocol = new TBinaryProtocol(transport);
    val client = new Geocoder.Client(protocol);

    def gp(lat: Double, lng: Double) = new GeocodePoint().setLat(lat).setLng(lng)

    try {
      transport.open();
      val req = new BulkReverseGeocodeRequest()
      req.setLatlngs(List(gp(40.74,-74), gp(40.75,-75.0)).asJava)
      println("sending")
      val response = client.bulkReverseGeocode(req)
      println("came back")
      println(response)
      transport.close();
    } catch {
      case e => println(e)
    }
  }
}