// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.importers.geonames

import com.foursquare.geo.shapes.{FsqSimpleFeature, GeoJsonIterator, ShapeIterator, ShapefileIterator}
import com.foursquare.twofishes.DisplayName
import com.foursquare.twofishes.util.{FeatureNamespace, GeonamesNamespace, Helpers, StoredFeatureId}
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.io.WKBWriter
import com.weiglewilczek.slf4s.Logging
import java.io.{File, FileWriter, Writer}
import org.apache.commons.net.util.Base64

// Tool to flatten all polygons and matching metadata to a single text file
// to simplify scalding index build
// run from twofishes root directory using the following command:
// ./sbt "indexer/run-main com.foursquare.twofishes.importers.geonames.PolygonDataFlattener"
//
// NOTE(rahul): This is a temporary workaround until I find/write an implementation
// of FileInputFormat and RecordReader for shapefiles/geojson that can split
// geojson is easy
// https://github.com/mraad/Shapefile works for shapefiles but cannot split yet
object PolygonDataFlattener extends Logging {

  var idCounter: Long = 0
  def getId(): Long = {
    idCounter += 1
    idCounter
  }

  val wkbWriter = new WKBWriter

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    if (these != null) {
      (these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)).filterNot(_.isDirectory)
    } else {
      Array()
    }
  }

  val parenNameRegex = "^(.*) \\((.*)\\)$".r
  def getNames(config: PolygonMappingConfig, feature: FsqSimpleFeature): Seq[DisplayName] = {
    config.nameFields.view.flatMap({case (lang, nameFields) => {
      nameFields.flatMap(nameField => feature.propMap.get(nameField)).filterNot(_.isEmpty).map(name =>
        DisplayName(lang, name)
      )
    }}).toSeq
  }
  def getFixedNames(config: PolygonMappingConfig, feature: FsqSimpleFeature) = {
    val originalFeatureNames = getNames(config, feature)

    var featureNames: Seq[DisplayName] = originalFeatureNames

    // some OSM feature names look like A (B), make that into two extra names
    featureNames ++= originalFeatureNames.flatMap(n => {
      // If the name looks like "A (B)"
      parenNameRegex.findFirstIn(n.name).toList.flatMap(_ => {
        // split it into "A" and "B"
        List(
          DisplayName(n.lang, parenNameRegex.replaceAllIn(n.name, "$1")),
          DisplayName(n.lang, parenNameRegex.replaceAllIn(n.name, "$2"))
        )
      })
    })

    // Additionally, some names are actually multiple comma separated names,
    // so split those into individual names too
    featureNames = featureNames.flatMap(n => {
      n.name.split(",").map(_.trim).filterNot(_.isEmpty).map(namePart =>
        DisplayName(n.lang, namePart)
      )
    })

    // Additionally, some names are actually multiple slash separated names
    // so split those into individual names too
    featureNames = featureNames.flatMap(n => {
      n.name.split("/").map(_.trim).filterNot(_.isEmpty).map(namePart =>
        DisplayName(n.lang, namePart)
      )
    })


    featureNames.filterNot(_.name.isEmpty)
  }

  def processFeatureIterator(
    defaultNamespace: FeatureNamespace,
    features: ShapeIterator,
    polygonMappingConfig: Option[PolygonMappingConfig],
    outputWriter: Writer
  ) {
    val fparts = features.file.getName().split("\\.")

    for {
      (rawFeature, index) <- features.zipWithIndex
      feature = new FsqSimpleFeature(rawFeature)
      geom <- feature.geometry
    } {
      if (index % 100 == 0) {
        logger.info("processing feature %d in %s".format(index, features.file.getName))
      }
      
      val polygonId = getId()

      val source = polygonMappingConfig.flatMap(_.source).getOrElse(features.file.getName())
      
      val geomBase64String = Base64.encodeBase64URLSafeString(wkbWriter.write(geom))

      val fidsFromFileName = fparts.lift(0).flatMap(p => Helpers.TryO(p.toInt.toString))
        .flatMap(i => StoredFeatureId.fromHumanReadableString(i, Some(defaultNamespace))).toList

      val geoidColumnNameToNamespaceMapping =
        List(
          "qs_gn_id" -> GeonamesNamespace,
          "gn_id" -> GeonamesNamespace) ++
          FeatureNamespace.values.map(namespace => (namespace.name -> namespace))
      val geoidsFromFile = for {
        (columnName, namespace) <- geoidColumnNameToNamespaceMapping
        values <- feature.propMap.get(columnName).toList
        value <- values.split(",")
        if value.nonEmpty
      } yield {
        // if value already contains ':' it was human-readable to begin with
        if (value.contains(":")) {
          value
        } else {
          "%s:%s".format(namespace.name, value)
        }
      }
      val fidsFromFile = geoidsFromFile
        .filterNot(_.isEmpty).map(_.replace(".0", ""))
        .flatMap(i => StoredFeatureId.fromHumanReadableString(i, Some(defaultNamespace)))

      val fids: Seq[StoredFeatureId] = if (fidsFromFileName.nonEmpty) {
        fidsFromFileName
      } else if (fidsFromFile.nonEmpty) {
        fidsFromFile
      } else {
        Nil
      }
      
      val (names, woeTypes) = (fids, polygonMappingConfig) match {
        case (Nil, Some(polygonMatchingConfig)) => (getFixedNames(polygonMatchingConfig, feature), polygonMatchingConfig.getWoeTypes)
        case _ => (Nil, Nil)
      }
      
      outputWriter.write("%s\t%s\t%s\t%s\t%s\t%s\n".format(
        polygonId.toString,
        source,
        fids.map(_.longId).mkString(","),
        names.map(dn => "%s:%s".format(dn.lang, dn.name)).mkString("|"),
        woeTypes.map(list => list.map(_.name).mkString(",")).mkString("|"),
        geomBase64String
      ))
    }
  }

  def load(defaultNamespace: FeatureNamespace, f: File, outputWriter: Writer): Unit = {
    val fparts = f.getName().split("\\.")
    val extension = fparts.lastOption.getOrElse("")
    val shapeFileExtensions = List("shx", "dbf", "prj", "xml", "cpg")

    if ((extension == "json" || extension == "geojson") && !PolygonMappingConfig.isMappingFile(f)) {
      processFeatureIterator(
        defaultNamespace,
        new GeoJsonIterator(f),
        PolygonMappingConfig.getMappingForFile(f),
        outputWriter
      )
    } else if (extension == "shp") {
      processFeatureIterator(
        defaultNamespace,
        new ShapefileIterator(f),
        PolygonMappingConfig.getMappingForFile(f),
        outputWriter
      )
    } else if (shapeFileExtensions.has(extension)) {
      // do nothing, shapefile aux file
      Nil
    }
  }

  def main(args: Array[String]): Unit = {
    val defaultNameSpace = GeonamesNamespace
    val polygonDirs = List(
      new File("data/computed/polygons"),
      new File("data/private/polygons"))
    val polygonFiles = polygonDirs.flatMap(recursiveListFiles).sorted

    val fileWriter = new FileWriter("data/private/flattenedPolygons.txt", false)

    polygonFiles.foreach(f => load(defaultNameSpace, f, fileWriter))

    fileWriter.close()
    logger.info("Done.")
  }
}
