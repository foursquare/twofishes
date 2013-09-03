package com.foursquare.twofishes.importers.geonames

import org.apache.hadoop.io.LongWritable
import com.foursquare.geo.shapes.FsqSimpleFeatureImplicits._
import com.foursquare.geo.shapes.ShapefileIterator
import com.foursquare.twofishes._
import com.foursquare.twofishes.Identity._
import com.foursquare.twofishes.util.{GeonamesId, GeonamesNamespace, Helpers, NameNormalizer, StoredFeatureId}
import com.foursquare.twofishes.util.Helpers._
import com.foursquare.twofishes.util.Lists.Implicits._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBWriter, WKTReader}
import java.io.{File, FileWriter, PrintStream}
import org.bson.types.ObjectId
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable.{HashMap, HashSet}
import scala.io.Source
import scalaj.collection.Implicits._
import com.twitter.scalding.typed.{Grouped, TypedSink}
import com.twitter.scalding.filecache.{DistributedCacheFile}
import com.twitter.scalding._
import TDsl._
import backtype.hadoop.ThriftSerialization

// TODO
// add hierarchy phase
// add bbox phase
// load other altnames files (glob dir on hdfs)
// load other feature files (glob dir on hdfs)


// sort by age
object StoredFeatureIdOrdering extends Ordering[StoredFeatureId] {
  def compare(a: StoredFeatureId, b: StoredFeatureId) = {
    a.longId compare b.longId
  }
}

case class CountryInfo(
  langs: List[String],
  name: String,
  adminId: String
)

class ScaldingParseJob(args : Args) extends Job(args) {
  def getFilePath(file: String): String = {
    "data/" + file
  }

  override def ioSerializations =
    super.ioSerializations ++ List(classOf[backtype.hadoop.ThriftSerialization])

  var parserConfig: GeonamesImporterConfig = GeonamesImporterConfigParser.parse(args.toList.toArray)

  val cachedCountryInfoFile = DistributedCacheFile(getFilePath("downloaded/countryInfo.txt"))
  @transient private lazy val countryInfoMap = parseCountryInfo()

  val cachedRewriteFiles =
    List(DistributedCacheFile(getFilePath("custom/rewrites.txt")),
      DistributedCacheFile(getFilePath("private/rewrites.txt")))
  @transient private lazy val rewriteTable = new TsvHelperFileParser(cachedRewriteFiles.map(_.file))

  // tokenlist
  val cachedDeleteFiles =
    List(DistributedCacheFile(getFilePath("custom/delete.txt")))
  @transient private lazy val deletesList: List[String] = cachedDeleteFiles.flatMap(f =>
    scala.io.Source.fromFile(f.file).getLines.toList)

  val cachedAliasFiles =
    List(DistributedCacheFile(getFilePath("custom/aliases.txt")),
      DistributedCacheFile(getFilePath("private/aliases.txt")))
  @transient lazy val aliasTable = new GeoIdTsvHelperFileParser(GeonamesNamespace, cachedAliasFiles.map(_.file))

  def parseCountryInfo(): Map[String, CountryInfo] = {
    val lines = scala.io.Source.fromFile(cachedCountryInfoFile.file).getLines
    lines
      .filter(line => !line.startsWith("#"))
      .map(line => {
        val parts = line.split("\t")
        val cc = parts(0)
        val englishName = parts(4)
        val langs = parts(15).split(",").map(l => l.split("-")(0)).toList
        val geonameid = parts(16)
        (cc, CountryInfo(langs, englishName, geonameid))
      }).toMap
  }

  def parseAlternateNames(): TypedPipe[(Long, AlternateNameEntry)] = {
//    val altDirs = List(
      // new File("data/computed/alternateNames/"),
      // new File("data/private/alternateNames/")
    // )

    val lines: TypedPipe[String] = TypedPipe.from(TextLine(
      getFilePath("downloaded/alternateNames.txt")))
    for {
      line <- lines
      val parts = line.split("\t").toList
      if (parts.size >= 3)
      val nameid = parts(0)
      val geonameid = parts(1)
      val lang = parts.lift(2).getOrElse("")
      fid <- StoredFeatureId.fromHumanReadableString(geonameid, Some(GeonamesNamespace))
      if (lang != "post")
    } yield {
      val name = parts(3)
      val isPrefName = parts.lift(4).exists(_ == "1")
      val isShortName = parts.lift(5).exists(_ == "1")

      val entry = AlternateNameEntry(
        nameId = nameid,
        name = name,
        lang = lang,
        isPrefName = isPrefName,
        isShortName = isShortName
      )
      (fid.longId, entry)
    }
  }

  val wkbWriter = new WKBWriter()
  val wktReader = new WKTReader()

  // TODO: load other feature files, glob
  def parseFromFile(filename: String,
    lineProcessor: String => Option[GeonamesFeature],
    typeName: String,
    allowBuildings: Boolean = false): TypedPipe[(Long, GeonamesFeature)] = {

    val lines: TypedPipe[String] = TypedPipe.from(TextLine(filename))

    (for {
      line <- lines
      f <- lineProcessor(line)
      geonameid <- f.geonameid
      fid <- StoredFeatureId.fromHumanReadableString(geonameid, Some(GeonamesNamespace))
      if (
        !f.featureClass.isStupid &&
        !(f.name.contains(", Stadt") && f.countryCode == "DE") &&
        // !f.geonameid.exists(ignoreList.contains) &&
        (!f.featureClass.isBuilding || parserConfig.shouldParseBuildings || allowBuildings)
      )
    } yield {
      (fid.longId, f)
    })
  }

  def doRewrites(names: List[String]): List[String] = {
    val nameSet = new scala.collection.mutable.HashSet[String]()
    rewriteTable.gidMap.foreach({case(from, toList) => {
      names.foreach(name => {
        toList.values.foreach(to => {
          nameSet += name.replaceAll(from, to)
        })
      })
    }})
    nameSet.toList
  }

  def doDelete(name: String): List[String] = {
    deletesList.flatMap(delete => {
      val newName = name.replaceAll(delete + "\\b", "").split(" ").filterNot(_.isEmpty).mkString(" ")
      if (newName != name) {
        Some(newName)
      } else {
        None
      }
    })
  }

  def rewriteNames(names: List[String]): (List[String], List[String]) = {
    val deleteModifiedNames: List[String] = names.flatMap(doDelete)

    val deaccentedNames = names.map(NameNormalizer.deaccent).filterNot(n =>
      names.contains(n))

    val rewrittenNames = doRewrites((names ++ deleteModifiedNames)).filterNot(n =>
      names.contains(n))

    (deaccentedNames, (deleteModifiedNames ++ rewrittenNames).distinct)
  }

  def processFeatureName(
    cc: String,
    lang: String,
    name: String,
    isPrefName: Boolean,
    isShortName: Boolean): List[DisplayName] = {
    if (lang != "post") {
      val originalNames = List(name)
      val (deaccentedNames, allModifiedNames) = rewriteNames(originalNames)

      def buildDisplayName(name: String, flags: Int) = {
        DisplayName(lang, name, flags)
      }

      def isLocalLang(lang: String) =
        countryInfoMap.get(cc).exists(_.langs.contains(lang))

      def processNameList(names: List[String], flags: Int): List[DisplayName] = {
        names.map(n => {
          var finalFlags = flags
          if (isLocalLang(lang)) {
            finalFlags |= FeatureNameFlags.LOCAL_LANG.getValue
          }
          buildDisplayName(n, finalFlags)
        })
      }

      val originalFlags = if (isPrefName) {
        FeatureNameFlags.PREFERRED.getValue
      } else { 0 }

      processNameList(originalNames, originalFlags) ++
      processNameList(deaccentedNames, originalFlags | FeatureNameFlags.DEACCENT.getValue) ++
      processNameList(allModifiedNames, originalFlags | FeatureNameFlags.ALIAS.getValue)
    } else {
      Nil
    }
  }

  def parseAdminFile(input: String) = {
    parseFromFile(input, (line: String) =>
      GeonamesFeature.parseFromAdminLine(0, line), "features", allowBuildings = false)
  }

  def featuresToThrift(values: Iterator[(GeonamesFeature, List[AlternateNameEntry])]) = {
    val features = values.toList

    if (features.size == 0) {
      throw new Exception("had no features for some reason")
    }


    if (features.size > 1) {
      throw new Exception("had multiple features for %s".format(features(0)._1.geonameid))
    }

    val feature = features(0)._1
    val altNames = features(0)._2

    // Build ids
    val geonameId = feature.geonameid.flatMap(id => {
      StoredFeatureId.fromHumanReadableString(id, defaultNamespace = Some(GeonamesNamespace))
    }).get

    val ids: List[StoredFeatureId] = List(geonameId)/* ++
      concordanceMap.get(geonameId).flatMap(id =>
        if (id.contains(":")) {
          StoredFeatureId.fromHumanReadableString(id)
        } else { None }
      )*/

    val preferredEnglishAltName = altNames.find(altName =>
      altName.lang == "en" && altName.isPrefName
    )

    val hasPreferredEnglishAltName = preferredEnglishAltName.isDefined

    // If we don't have an altname with lang=en marked as preferred, then assume that
    // the primary name geonames gives us is english preferred
    var displayNames: List[DisplayName] = Nil

    if (!preferredEnglishAltName.exists(_.name =? feature.name)) {
      displayNames ++= processFeatureName(
        feature.countryCode, "en", feature.name,
        isPrefName = !hasPreferredEnglishAltName,
        isShortName = false
      )
    }

    if (feature.featureClass.woeType == YahooWoeType.COUNTRY) {
      countryInfoMap.get(feature.countryCode).foreach(info =>
        displayNames ::=
          DisplayName("en", info.name, FeatureNameFlags.PREFERRED.getValue() | FeatureNameFlags.COLLOQUIAL.getValue())
      )
    }


    feature.asciiname.foreach(asciiname => {
      if (feature.name != asciiname && asciiname.nonEmpty) {
        displayNames ::=
          DisplayName("en", asciiname,
            FeatureNameFlags.DEACCENT.getValue)
      }
    })

    if (feature.featureClass.woeType.getValue == YahooWoeType.COUNTRY.getValue) {
      displayNames ::= DisplayName("abbr", feature.countryCode, 0)
    }

    // Build names
    val aliasedNames: List[String] = aliasTable.get(geonameId)

    // val allNames = feature.allNames ++ aliasedNames
    // val allModifiedNames = rewriteNames(allNames)
    // val normalizedNames = (allNames ++ allModifiedNames).map(n => NameNormalizer.normalize(n))
    // normalizedNames.toSet.toList.filterNot(_.isEmpty)

    aliasedNames.foreach(n =>
      displayNames ::= DisplayName("en", n, FeatureNameFlags.ALT_NAME.getValue)
    )

    val englishName = preferredEnglishAltName.getOrElse(feature.name)
    val alternateNames = altNames.filterNot(n =>
      (n.name == englishName) && (n.lang != "en")
    )
    displayNames ++= alternateNames.flatMap(altName => {
      processFeatureName(
        feature.countryCode, altName.lang, altName.name, altName.isPrefName, altName.isShortName)
    })

    // the admincode is the internal geonames admin code, but is very often the
    // same short name for the admin area that is actually used in the country
    if (feature.featureClass.isAdmin1 || feature.featureClass.isAdmin2) {
      displayNames ++= feature.adminCode.toList.map(code => {
        DisplayName("abbr", code, FeatureNameFlags.ABBREVIATION.getValue)
      })
    }

    val extraParents: List[StoredFeatureId] =
      feature.extraColumns.get("parents").toList.flatMap(_.split(",").toList).flatMap(pStr =>
        StoredFeatureId.fromHumanReadableString(pStr))

    var lat = feature.latitude
    var lng = feature.longitude

    val canGeocode = feature.extraColumns.get("canGeocode").map(_.toInt).getOrElse(1) > 0

    val polygonExtraEntry: Option[Geometry] = feature.extraColumns.get("geometry").map(polygon => {
      wktReader.read(polygon)
    })

    val record = GeocodeRecord(
      _id = geonameId.longId,
      ids = ids.map(_.longId),
      names = Nil,
      cc = feature.countryCode,
      _woeType = feature.featureClass.woeType.getValue,
      lat = lat,
      lng = lng,
      parents = extraParents.map(_.longId),
      population = feature.population,
      displayNames = displayNames,
      // boost = boost,
      // boundingbox = bbox,
      canGeocode = canGeocode,
      // slug = slug,
      polygon = polygonExtraEntry.map(wkbWriter.write),
      hasPoly = polygonExtraEntry.map(e => true)
    )

    List(record.toGeocodeServingFeature).iterator
  }

  def runMain() {
    val features = if (!parserConfig.parseWorld) {
      val countries = parserConfig.parseCountry.split(",")
      //countries.foreach(f => {
      val f = countries.head
       // logger.info("Parsing %s".format(f))
       //   parseAdminInfoFile("data/computed/adminCodes-%s.txt".format(f))
       parseAdminFile(getFilePath("downloaded/%s.txt".format(f)))
      //   if (config.importPostalCodes) {
      //     parser.parsePostalCodeFile("data/downloaded/zip/%s.txt".format(f))
      //   }
      //})
    } else {
      // parseAdminInfoFile("data/computed/adminCodes.txt")
      parseAdminFile(getFilePath("downloaded/allCountries.txt"))
      // if (config.importPostalCodes) {
      //   parser.parsePostalCodeFile("data/downloaded/zip/allCountries.txt")
      // }
    }

    val altNames: Grouped[Long, List[AlternateNameEntry]] = parseAlternateNames()
      .group.mapValueStream(v => List(v.toList).iterator)

    features.group.join(altNames).mapValueStream(featuresToThrift)
      .toTypedPipe
      .write(TypedSink[(Long, GeocodeServingFeature)](SequenceFile("/tmp/xxxx.seq")))
  }

  runMain()
}

//   def parseAdminFile(filename: String, allowBuildings: Boolean = false) {
//     parseFromFile(filename, (index: Int, line: String) =>
//       GeonamesFeature.parseFromAdminLine(index, line), "features", allowBuildings)
//   }

//   def parseFeature(feature: GeonamesFeature): GeocodeRecord = {





//     }








//     def fixParent(p: String): Option[String] = {
//       adminIdMap.get(p) orElse {
//         //println("missing admin lookup for %s".format(p))
//         None
//       }
//     }

//     // Build parents

//     val parents: List[StoredFeatureId] =
//       feature.parents.flatMap(fixParent).map(p => GeonamesId(p.toLong))
//     val hierarchyParents: List[StoredFeatureId] =
//       hierarchyTable.getOrElse(geonameId, Nil).filterNot(p => parents.has(p))

//     val allParents: List[StoredFeatureId] = extraParents ++ parents ++ hierarchyParents

//     val boost: Option[Int] =
//       feature.extraColumns.get("boost").map(_.toInt) orElse
//         boostTable.get(geonameId).headOption.flatMap(boost =>
//           TryO { boost.toInt }
//         )

//     val bbox = feature.extraColumns.get("bbox").flatMap(bboxStr => {
//       // west, south, east, north
//       val parts = bboxStr.split(",").map(_.trim)
//       parts.toList match {
//         case w :: s :: e :: n :: Nil => {
//           Some(BoundingBox(Point(n.toDouble, e.toDouble), Point(s.toDouble, w.toDouble)))
//         }
//         case _ => {
//           logger.error("malformed bbox: " + bboxStr)
//           None
//         }
//       }
//     }) orElse bboxTable.get(geonameId)


//     val latlngs = moveTable.get(geonameId)
//     if (latlngs.size > 0) {
//       lat = latlngs(0).toDouble
//       lng = latlngs(1).toDouble
//     }



//     val slug: Option[String] = slugIndexer.getBestSlug(geonameId)

//     if (slug.isEmpty &&
//       List(YahooWoeType.TOWN, YahooWoeType.SUBURB, YahooWoeType.COUNTRY, YahooWoeType.ADMIN1, YahooWoeType.ADMIN2).has(feature.featureClass.woeType)) {
//       slugIndexer.missingSlugList.add(geonameId.humanReadableString)
//     }

//     var attributesSet = false
//     lazy val attributesBuilder = {
//       attributesSet = true
//       GeocodeFeatureAttributes.newBuilder
//     }

//     naturalEarthPopulatedPlacesMap.get(geonameId).map(sfeature => {
//       sfeature.propMap.get("adm0cap").foreach(v =>
//         attributesBuilder.adm0cap(v.toDouble.toInt == 1)
//       )
//       sfeature.propMap.get("scalerank").foreach(v =>
//         attributesBuilder.scalerank(v.toInt)
//       )
//       sfeature.propMap.get("natscale").foreach(v =>
//         attributesBuilder.natscale(v.toInt)
//       )
//       sfeature.propMap.get("labelrank").foreach(v =>
//         attributesBuilder.labelrank(v.toInt)
//       )
//     })

//     if (feature.featureClass.isAdmin1Capital) {
//       attributesBuilder.adm1cap(true)
//     }

//     feature.population.foreach(pop =>
//       attributesBuilder.population(pop)
//     )

//     feature.extraColumns.get("sociallyRelevant").map(v =>
//       attributesBuilder.sociallyRelevant(v.toBoolean)
//     )

//     feature.extraColumns.get("neighborhoodType").map(v =>
//       attributesBuilder.neighborhoodType(NeighborhoodType.findByNameOrNull(v))
//     )



//     if (attributesSet) {
//       record.setAttributes(Some(attributesBuilder.result))
//     }

//     record
//   }
