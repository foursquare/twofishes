//  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import com.foursquare.common.thrift.json.TReadableJSONProtocol
import com.foursquare.geo.shapes.ShapefileS2Util
import com.foursquare.spindle.{MetaRecord, Record}
import com.foursquare.twofishes.util.Helpers
import com.foursquare.twofishes.util.Lists.Implicits._
import com.google.common.geometry.S2CellId
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http.Http
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.json.Json
import com.twitter.ostrich.admin._
import com.twitter.ostrich.admin.config._
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Await, Future, FuturePool, RingBuffer}
import com.vividsolutions.jts.io.WKTWriter
import com.weiglewilczek.slf4s.Logging
import java.io.InputStream
import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util.Date
import java.util.concurrent.{ConcurrentHashMap, Executors}
import org.apache.thrift.{TBase, TDeserializer, TFieldIdEnum, TSerializer}
import org.apache.thrift.protocol.{TBinaryProtocol, TSimpleJSONProtocol}
import org.bson.types.ObjectId
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
import scalaj.collection.Implicits._

class QueryLogHttpHandler(
  queryMap: ConcurrentHashMap[ObjectId, (TBase[_, _], Long)],
  recentQueries: Seq[(TBase[_, _], Long, Long)],
  slowQueries: Seq[(TBase[_, _], Long, Long)]
) extends Service[HttpRequest, HttpResponse] {
  def apply(request: HttpRequest) = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val currentTime = System.currentTimeMillis()

    val content = (queryMap.asScala.map({case (k, v) => {
      "Request has taken %dms so far\n%s".format(currentTime - v._2, v._1)
    }}).mkString("\n") +
    "\n-----------------------------------------\n" + "SLOW QUERIES\n"
      + slowQueries.reverse.map({case ((req, start, end)) => {
        "Query took %d ms --  started at %s, ended at %s\n%s".format(
          (end - start),
          new Date(start),
          new Date(end),
          req
        )
      }}).mkString("\n") +
      "\n-----------------------------------------\n" + "RECENT QUERIES\n"
      + recentQueries.reverse.map({case ((req, start, end)) => {
        "Query took %d ms --  started at %s, ended at %s\n%s".format(
          (end - start),
          new Date(start),
          new Date(end),
          req
        )
      }}).mkString("\n")
    )

    response.setHeader("Content-Type", "text/plain")
    response.setContent(ChannelBuffers.copiedBuffer(content, CharsetUtil.UTF_8))
    Future.value(response)
  }
}

class QueryLoggingGeocodeServerImpl(service: Geocoder.ServiceIface) extends Geocoder.ServiceIface with Logging {
  if (Charset.defaultCharset() != Charset.forName("UTF-8")) {
    throw new Exception(
      "Default charset (%s) is not UTF-8, which can cause problems.\n".format(
        Charset.defaultCharset().name()) +
      "See: http://perlgeek.de/en/article/set-up-a-clean-utf8-environment"
    )
  }

  val queryMap = new ConcurrentHashMap[ObjectId, (TBase[_, _], Long)]

  val recentQueries = new RingBuffer[(TBase[_, _], Long, Long)](1000)
  val slowQueries = new RingBuffer[(TBase[_, _], Long, Long)](1000)

  val slowQueryHttpHandler = new QueryLogHttpHandler(queryMap, recentQueries, slowQueries)

  def queryLogProcessor[Req <: TBase[_, _], Res <: TBase[_, _]](r: Req, f: (Req => Future[Res])): Future[Res] = {
    // log the start of this query
    val start = System.currentTimeMillis()
    val id = new ObjectId()
    queryMap.put(id, (r, start))

    def logCompletion() {
      val end = System.currentTimeMillis()
      // greater than 500 ms
      if (end - start > 500) {
        // log slow query
        logger.info("%s took %d ms".format(r, end - start))
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

  def refreshStore(r: RefreshStoreRequest): Future[RefreshStoreResponse] = {
    queryLogProcessor(r, service.refreshStore)
  }

  def getS2CellInfos(r: S2CellInfoRequest): Future[S2CellInfoResponse] = {
    queryLogProcessor(r, service.getS2CellInfos)
  }
}

class GeocodeServerImpl(
  store: GeocodeStorageReadService,
  doWarmup: Boolean,
  enablePrivateEndpoints: Boolean = false,
  queryFuturePool: FuturePool = FuturePool(StatsWrappedExecutors.create(24, 100, "geocoder"))
) extends Geocoder.ServiceIface with Logging {
  if (doWarmup) {
    for {
      time <- 0.to(2)
    } {
      val batchSize = 50 // FIXME: magic

      var lines = new BufferedSource(getClass.getResourceAsStream("/warmup/geocodes.txt")).getLines.take(10000).grouped(batchSize).toVector

      logger.info("Warming up by geocoding %d queries".format(lines.size * batchSize))
      lines.zipWithIndex.foreach { case (batch, index) =>
        if (index % 20 == 0) {
          logger.info("finished %d queries".format(index * batchSize))
        }
        val work = Future.collect(batch.map { line => queryFuturePool {
          new GeocodeRequestDispatcher(store).geocode(
            GeocodeRequest.newBuilder.responseIncludes(
              Vector(ResponseIncludes.S2_COVERING, ResponseIncludes.S2_INTERIOR, ResponseIncludes.WKB_GEOMETRY)
            ).query(line).result
          )
          new GeocodeRequestDispatcher(store).geocode(GeocodeRequest.newBuilder.query(line).autocomplete(true).result)
        }})
        Await.result(work)
      }
      logger.info("done")

      val revgeoLines = new BufferedSource(getClass.getResourceAsStream("/warmup/revgeo.txt")).getLines.take(10000).grouped(batchSize).toVector

      logger.info("Warming up by reverse geocoding %d queries".format(revgeoLines.size * batchSize))
      revgeoLines.zipWithIndex.foreach { case (batch, index) =>
        if (index % 20 == 0) {
          logger.info("finished %d queries".format(index * batchSize))
        }
        val work = Future.collect(batch.map { line => queryFuturePool {
          val parts = line.split(",")
          new ReverseGeocoderImpl(store, GeocodeRequest.newBuilder.ll(GeocodePoint(parts(0).toDouble, parts(1).toDouble)).result).doGeocode()
          new ReverseGeocoderImpl(store, GeocodeRequest.newBuilder.ll(GeocodePoint(parts(0).toDouble, parts(1).toDouble)).radius(300).result).doGeocode()
        }})
        Await.result(work)
      }
      logger.info("done")

      val slugLines = 1.to(10000).map(id => "geonameid:%d".format(id)).grouped(batchSize)
      logger.info("Warming up by geocoding %d slugs".format(slugLines.size * batchSize))
      slugLines.zipWithIndex.foreach { case (batch, index) =>
        if (index % 20 == 0) {
          logger.info("finished %d queries".format(index * batchSize))
        }
        val work = Future.collect(batch.map { line => queryFuturePool {
          new GeocodeRequestDispatcher(store).geocode(GeocodeRequest.newBuilder.slug(line).result)
        }})
        Await.result(work)
      }
      logger.info("done")
    }
    logger.info("done")
    val labels = Stats.getLabels()
    Stats.clearAll()
    labels.foreach({case (k, v) => Stats.setLabel(k, v)})
    System.gc()
  }

  def geocode(r: GeocodeRequest): Future[GeocodeResponse] = queryFuturePool {
    new GeocodeRequestDispatcher(store).geocode(r)
  }

  def reverseGeocode(r: GeocodeRequest): Future[GeocodeResponse] = queryFuturePool {
    new ReverseGeocoderImpl(store, r).doGeocode()
  }

  def bulkReverseGeocode(r: BulkReverseGeocodeRequest): Future[BulkReverseGeocodeResponse] = queryFuturePool {
    new BulkReverseGeocoderImpl(store, r).doGeocode()
  }

  def bulkSlugLookup(r: BulkSlugLookupRequest): Future[BulkSlugLookupResponse] = queryFuturePool {
    new BulkSlugLookupImpl(store, r).doGeocode()
  }

  def refreshStore(r: RefreshStoreRequest): Future[RefreshStoreResponse] = {
    var success = true
    if (enablePrivateEndpoints) {
      try {
        store.refresh
      } catch {
        case e: Exception => success = false
      }
    }
    Future.value(RefreshStoreResponse(enablePrivateEndpoints && success))
  }

  def getS2CellInfos(r: S2CellInfoRequest): Future[S2CellInfoResponse] = {
    if (enablePrivateEndpoints) {
      val wktWriter = new WKTWriter
      val cellInfos = (for {
        idString <- r.cellIdsAsStrings
        id <- Helpers.TryO(idString.toLong).toList
        s2CellId = new S2CellId(id)
      } yield {
        val wkt = wktWriter.write(ShapefileS2Util.fullGeometryForCell(s2CellId))
        S2CellIdInfo(id, s2CellId.level(), wkt)
      }).toList
      Future.value(S2CellInfoResponse(cellInfos))
    } else {
      Future.value(S2CellInfoResponse.newBuilder.result)
    }
  }
}

class HandleExceptions extends SimpleFilter[HttpRequest, HttpResponse] with Logging {
  def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
    // `handle` asynchronously handles exceptions.
    service(request) handle {
      case error: Exception =>
        logger.error("got error: %s".format(error))
        error.printStackTrace
        val statusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR
        val errorResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, statusCode)
        errorResponse.setHeader("Content-Type", "application/json; charset=utf-8")
        val errorMap = Map(
          "exception" -> error.toString,
          "stacktrace" -> error.getStackTraceString)
        errorResponse.setContent(ChannelBuffers.copiedBuffer(Json.build(errorMap).toString, CharsetUtil.UTF_8))
        errorResponse
    }
  }
}

class GeocoderHttpService(geocoder: Geocoder.ServiceIface) extends Service[HttpRequest, HttpResponse] {
  val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(8))

  def handleGeocodeQuery(request: GeocodeRequest, callback: Option[String]) =
    handleQuery(request, geocoder.geocode, callback)

  def handleReverseGeocodeQuery(request: GeocodeRequest, callback: Option[String]) =
    handleQuery(request, geocoder.reverseGeocode, callback)

  def handleBulkReverseGeocodeQuery(
    params: CommonGeocodeRequestParams,
    points: Seq[(Double, Double)],
    callback: Option[String]
  ) = {
    val request = BulkReverseGeocodeRequest.newBuilder
      .params(params)
      .latlngs(points.map(ll => GeocodePoint(ll._1, ll._2)))
      .result
    handleQuery(request, geocoder.bulkReverseGeocode, callback)
  }

  def handleBulkSlugLookupQuery(
    params: CommonGeocodeRequestParams,
    slugs: Seq[String],
    callback: Option[String]
  ) = {
    val request = BulkSlugLookupRequest.newBuilder
      .params(params)
      .slugs(slugs)
      .result
    handleQuery(request, geocoder.bulkSlugLookup, callback)
  }

  def fixLongArray(key: String, input: String): String = {
    val re = "\"%s\":\\[([^\\]]+)\\]".format(key).r
    re.replaceAllIn(input, m => { "\"%s\":[%s]".format(key, m.group(1).split(",").map(l => "\"%s\"".format(l)).mkString(","))})
  }

  def handleQuery[T, TType <: TBase[_ <: TBase[_ <: AnyRef, _ <: TFieldIdEnum], _ <: TFieldIdEnum]](
      request: T,
      queryProcessor: T => Future[TType],
      callback: Option[String]
  ): Future[DefaultHttpResponse] = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    queryProcessor(request).map(geocode => {
      val serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
      val json = {
        val json = serializer.toString(geocode);

        //"longId":72057594044179937
        // javascript can't deal with longs, so we hack it to be a string
        var fixedJson = """"longId":(\d+)""".r.replaceAllIn(json, m => "\"longId\":\"%s\"".format(m.group(1)))
        fixedJson = fixLongArray("parentIds", fixedJson)
        fixedJson = fixLongArray("longIds", fixedJson)
        fixedJson = fixLongArray("s2Covering", fixedJson)
        fixedJson = fixLongArray("s2Interior", fixedJson)

        callback.map(cb => {
          val sb = new StringBuilder(fixedJson.size + cb.size + 10)
          sb ++= cb
          sb += '('
          sb ++= fixedJson
          sb += ')'
          sb.toString
        }).getOrElse(fixedJson)
      }

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

    val autocompleteBias: Option[AutocompleteBias] = getOpt("autocompleteBias").flatMap(i => {
      Helpers.TryO(i.toInt).flatMap(AutocompleteBias.findById _)
        .orElse(AutocompleteBias.findByName(i))
    })

    GeocodeRequest.newBuilder
      .query(getOpt("query"))
      .slug(getOpt("slug"))
      .lang(getOpt("lang"))
      .cc(getOpt("cc"))
      .debug(getOpt("debug").map(_.toInt))
      .radius(getOpt("radius").map(_.toInt))
      .strict(getOpt("strict").map(_.toBoolean))
      .autocomplete(getOpt("autocomplete").map(_.toBoolean))
      .autocompleteBias(autocompleteBias)
      .ll(ll)
      .maxInterpretations(getOpt("maxInterpretations").map(_.toInt))
      .allowedSources(getOpt("allowedSources").toList.flatMap(_.split(",")))
      .woeHint(woeHint)
      .woeRestrict(woeRestrict)
      .responseIncludes(responseIncludes)
      .result
  }

  def apply(request: HttpRequest) = {
    val queryString = new QueryStringDecoder(request.getUri())
    val params = queryString.getParameters().asScala.mapValues(_.asScala)
    val path = queryString.getPath()
    val callback = params.get("callback").flatMap(_.headOption)

    def getJsonRequest[R <: TBase[_ <: TBase[_, _], _ <: TFieldIdEnum] with Record[R]](meta: MetaRecord[R, _]): R = {
      var json: Option[String] = params.get("json").map(a => a(0))

      val content = request.getContent()
      if (content.readable()) {
        json = Some(content.toString(CharsetUtil.UTF_8))
      }
      val deserializer = new TDeserializer(new TReadableJSONProtocol.Factory(false))
      val thriftRequest = meta.createRecord
      deserializer.deserialize(thriftRequest, json.get.getBytes("UTF-8"))
      thriftRequest
    }

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
    } else if (path.startsWith("/search/geocode")) {
      handleGeocodeQuery(getJsonRequest(GeocodeRequest), callback)
    } else if (path.startsWith("/search/reverseGeocode")) {
      handleReverseGeocodeQuery(getJsonRequest(GeocodeRequest), callback)
    } else if (path.startsWith("/search/bulkReverseGeocode")) {
      handleQuery(getJsonRequest(BulkReverseGeocodeRequest), geocoder.bulkReverseGeocode, callback)
    } else if (path.startsWith("/search/bulkSlugLookup")) {
      handleQuery(getJsonRequest(BulkSlugLookupRequest), geocoder.bulkSlugLookup, callback)
    } else if (path.startsWith("/private/refreshStore")) {
      handleQuery(getJsonRequest(RefreshStoreRequest), geocoder.refreshStore, callback)
    } else if (path.startsWith("/private/getS2CellInfos")) {
      handleQuery(getJsonRequest(S2CellInfoRequest), geocoder.getS2CellInfos, callback)
    } else if (params.size > 0) {
      val request = parseGeocodeRequest(params.toMap)

      val commonParams = GeocodeRequestUtils.geocodeRequestToCommonRequestParams(request)
      if (params.getOrElse("method", Nil).has("bulkrevgeo")) {
        handleBulkReverseGeocodeQuery(commonParams, params.getOrElse("ll", Nil).map(v => {
          val ll = v.split(",").toList
          (ll(0).toDouble, ll(1).toDouble)
        }), callback)
      } else if (params.getOrElse("method", Nil).has("bulksluglookup")) {
        handleBulkSlugLookupQuery(commonParams, params.getOrElse("slug", Nil), callback)
      } else if (request.queryOption.isEmpty && request.slugOption.isEmpty) {
        handleReverseGeocodeQuery(request, callback)
      } else {
        handleGeocodeQuery(request, callback)
      }
    } else {
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
      val msg = new BufferedSource(getClass.getResourceAsStream("/static/index.html")).getLines.mkString("\n")
      response.setContent(ChannelBuffers.copiedBuffer(msg, CharsetUtil.UTF_8))
      Future.value(response)
    }
  }
}

object ServerStore {
  def getStore(config: GeocodeServerConfig): GeocodeStorageReadService = {
    getStore(config.hfileBasePath, config.shouldPreload, config.hotfixBasePath)
  }

  def getStore(hfileBasePath: String, shouldPreload: Boolean, hotfixBasePath: String): GeocodeStorageReadService = {
    val underlying = new HFileStorageService(hfileBasePath, shouldPreload)
    val hotfixSource = if (hotfixBasePath.nonEmpty) {
      new JsonHotfixSource(hotfixBasePath)
    } else {
      new EmptyHotfixSource
    }

    new HotfixableGeocodeStorageService(
      underlying,
      new ConcreteHotfixStorageService(hotfixSource, underlying))
  }
}

object GeocodeFinagleServer extends Logging {
  def main(args: Array[String]) {
    val handleExceptions = new HandleExceptions

    val version = getClass.getPackage.getImplementationVersion
    if (version != null) {
      logger.info("starting version %s".format(version))
      Stats.setLabel("version", version)
    }

    val config: GeocodeServerConfig = GeocodeServerConfigSingleton.init(args)

    // Implement the Thrift Interface
    val processor = new QueryLoggingGeocodeServerImpl(new GeocodeServerImpl(ServerStore.getStore(config), config.shouldWarmup, config.enablePrivateEndpoints))

    // Convert the Thrift Processor to a Finagle Service
    val service = new Geocoder.Service(processor, new TBinaryProtocol.Factory())

    logger.info("serving finagle-thrift on port %d".format(config.thriftServerPort))
    logger.info("serving http/json on port %d".format(config.thriftServerPort + 1))
    logger.info("serving debug info on port %d".format(config.thriftServerPort + 2))
    logger.info("serving slow query http/json on port %d".format(config.thriftServerPort + 3))

    val server: Server = ServerBuilder()
      .bindTo(new InetSocketAddress(config.host, config.thriftServerPort))
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
        .bindTo(new InetSocketAddress(config.host, config.thriftServerPort + 1))
        .codec(Http())
        .name("geocoder-http")
        .reportTo(new FoursquareStatsReceiver)
        .build(handleExceptions andThen new GeocoderHttpService(processor))
    }

    ServerBuilder()
      .bindTo(new InetSocketAddress(config.host, config.thriftServerPort + 3))
      .codec(Http())
      .name("geocoder-slow-query-http")
      .reportTo(new FoursquareStatsReceiver)
      .build(handleExceptions andThen processor.slowQueryHttpHandler)
  }
}
