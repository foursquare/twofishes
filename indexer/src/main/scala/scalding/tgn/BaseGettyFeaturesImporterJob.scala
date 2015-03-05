// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding.tgn

import com.foursquare.geo.quadtree.CountryRevGeo
import com.foursquare.hadoop.scalding.SpindleSequenceFileSource
import com.foursquare.twofishes.{DisplayName, YahooWoeType, _}
import com.foursquare.twofishes.scalding.{TwofishesImporterInputSpec, TwofishesImporterJob}
import com.foursquare.twofishes.util.{GettyId, StoredFeatureId}
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import org.apache.hadoop.io.LongWritable
import org.geotools.geojson.GeoJSONUtil
import org.geotools.geojson.feature.FeatureJSON
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

class BaseGettyFeaturesImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesImporterJob(name, inputSpec, args) {
  lines.flatMap(line => {
      /*
      {
    "geometry": {
        "coordinates": [
            -118.0,
            65.0
        ],
        "type": "Point"
    },
    "properties": {
        "hierarchy": [
            "tgn:1000296",
            "tgn:7005685",
            "tgn:1000001",
            "tgn:7029392"
        ],
        "id": 1000296,
        "name:xx": "Northwest Territories",
        "placetype": [
            "aat:300387064",
            "aat:300135982"
        ]
    },
    "type": "Feature"
}
    */

    parseOpt(line) match {
      case Some(json) => {
        val hierarchyStrings: Seq[String] = (json \ "properties" \ "hierarchy").asInstanceOf[JArray].arr.map(_.asInstanceOf[JString].s)
        val hierarchyIds = for {
          hid <- hierarchyStrings
          fid <- StoredFeatureId.fromHumanReadableString(hid)
        } yield {
          fid.longId
        }

        val id: Int = (json \ "properties" \ "id").asInstanceOf[JInt].num.toInt
        val rawPlacetype: Seq[String] = (json \ "properties" \ "placetype").asInstanceOf[JArray].arr.map(_.asInstanceOf[JString].s)
        val names: Seq[DisplayName] =
          (
            (json \ "properties").asInstanceOf[JObject].obj.filter { case (key, _) => key.startsWith("name:")}
              .map { case (key, value) => {
              (key.replace("name:", ""), value.asInstanceOf[JString].s)
            }
            }
              .map { case (lang, name) => DisplayName(lang, name)}
            )

        val feature = new FeatureJSON().readFeature(GeoJSONUtil.toReader(line))
        val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
        val centerLatLng: (Double, Double) = (geom.getCentroid().getY(), geom.getCentroid().getX())

        val ccOpt = CountryRevGeo.getNearestCountryCode(centerLatLng._1, centerLatLng._2)
        val featureId = GettyId(id)

        val geocodeRecord = GeocodeRecord(
          _id = featureId.longId,
          names = Nil,
          cc = ccOpt.getOrElse("XX"),
          _woeType = YahooWoeType.UNKNOWN.id,
          lat = centerLatLng._1,
          lng = centerLatLng._2,
          displayNames = names.toList,
          ids = List(featureId.longId),
          parents = hierarchyIds.toList
        )

        val servingFeature = geocodeRecord.toGeocodeServingFeature()
        Some(new LongWritable(servingFeature.longId) -> servingFeature)
      }

      case None => {
        // logger.error("couldn't parse json from line: %s".format(line))
        None
      }
    }
  }).group
    .head
    .write(TypedSink[(LongWritable, GeocodeServingFeature)](SpindleSequenceFileSource[LongWritable, GeocodeServingFeature](outputPath)))
}
