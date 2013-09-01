package com.foursquare.twofishes.importers.geonames

package com.foursquare.twofishes.importers.geonames

import com.twitter.scalding._
import TDsl._

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

case class CountryInfo(
  langs: List[String],
  name: String,
  adminId: String
)

class CountryInfoJob(args : Args) extends Job(args) {
  val typedLines : TypedPipe[String] = TypedPipe.from(TextLine( args("countryInput") ))
  typedLines
    .filter(!line.startsWith("#"))
    .map(line => {
      val parts = line.split("\t")
      val cc = parts(0)
      val englishName = parts(4)
      val langs = parts(15).split(",").map(l => l.split("-")(0)).toList
      val geonameid = parts(16)
      (cc, CountryInfo(langs, englishName, geonameid))
    })
}

//   def parseAlternateNames(): DList[(StoredFeatureId, AlternateNameEntry)] {
//     val lines = fromTextFile(
//       "data/computed/alternateNames/*",
//       "data/private/alternateNames/*",
//       "data/downloaded/alternateNames.txt"
//     )

//     for {
//       line <- lines
//       val parts = line.split("\t").toList
//       if (parts.size >= 3)
//       val nameid = parts(0)
//       val geonameid = parts(1)
//       val lang = parts.lift(2).getOrElse("")
//       val fid = StoredFeatureId.fromHumanReadableString(geonameid, Some(GeonamesNamespace))
//       if (lang != "post")
//     } yield {
//       val name = parts(3)
//       val isPrefName = parts.lift(4).exists(_ == "1")
//       val isShortName = parts.lift(5).exists(_ == "1")

//       val entry = AlternateNameEntry(
//         nameId = nameid,
//         name = name,
//         lang = lang,
//         isPrefName = isPrefName,
//         isShortName = isShortName
//       )
//       (fid, entry)
//     }
//   }

//   def parseAdminFile(filename: String, allowBuildings: Boolean = false) {
//     parseFromFile(filename, (index: Int, line: String) =>
//       GeonamesFeature.parseFromAdminLine(index, line), "features", allowBuildings)
//   }

//   def parseFeature(feature: GeonamesFeature): GeocodeRecord = {
//     // Build ids
//     val geonameId = feature.geonameid.flatMap(id => {
//       StoredFeatureId.fromHumanReadableString(id, defaultNamespace = Some(GeonamesNamespace))
//     }).get

//     val ids: List[StoredFeatureId] = List(geonameId)/* ++
//       concordanceMap.get(geonameId).flatMap(id =>
//         if (id.contains(":")) {
//           StoredFeatureId.fromHumanReadableString(id)
//         } else { None }
//       )*/

//     val preferredEnglishAltName = alternateNamesMap.getOrElse(geonameId, Nil).find(altName =>
//       altName.lang == "en" && altName.isPrefName
//     )

//     val hasPreferredEnglishAltName = preferredEnglishAltName.isDefined

//     // If we don't have an altname with lang=en marked as preferred, then assume that
//     // the primary name geonames gives us is english preferred
//     var displayNames: List[DisplayName] = Nil

//     if (!preferredEnglishAltName.exists(_.name =? feature.name)) {
//       displayNames ++= processFeatureName(
//         feature.countryCode, "en", feature.name,
//         isPrefName = !hasPreferredEnglishAltName,
//         isShortName = false
//       )
//     }

//     if (feature.featureClass.woeType == YahooWoeType.COUNTRY) {
//       countryNameMap.get(feature.countryCode).foreach(name =>
//         displayNames ::=
//           DisplayName("en", name, FeatureNameFlags.PREFERRED.getValue() | FeatureNameFlags.COLLOQUIAL.getValue())
//       )
//     }

//     feature.asciiname.foreach(asciiname => {
//       if (feature.name != asciiname && asciiname.nonEmpty) {
//         displayNames ::=
//           DisplayName("en", asciiname,
//             FeatureNameFlags.DEACCENT.getValue)
//       }
//     })

//     if (feature.featureClass.woeType.getValue == YahooWoeType.COUNTRY.getValue) {
//       displayNames ::= DisplayName("abbr", feature.countryCode, 0)
//     }

//     // Build names
//     val aliasedNames: List[String] = aliasTable.get(geonameId)

//     // val allNames = feature.allNames ++ aliasedNames
//     // val allModifiedNames = rewriteNames(allNames)
//     // val normalizedNames = (allNames ++ allModifiedNames).map(n => NameNormalizer.normalize(n))
//     // normalizedNames.toSet.toList.filterNot(_.isEmpty)

//     aliasedNames.foreach(n =>
//       displayNames ::= DisplayName("en", n, FeatureNameFlags.ALT_NAME.getValue)
//     )

//     val englishName = preferredEnglishAltName.getOrElse(feature.name)
//     val alternateNames = alternateNamesMap.getOrElse(geonameId, Nil).filterNot(n =>
//       (n.name == englishName) && (n.lang != "en")
//     )
//     displayNames ++= alternateNames.flatMap(altName => {
//       processFeatureName(
//         feature.countryCode, altName.lang, altName.name, altName.isPrefName, altName.isShortName)
//     })

//     // the admincode is the internal geonames admin code, but is very often the
//     // same short name for the admin area that is actually used in the country

//     if (feature.featureClass.isAdmin1 || feature.featureClass.isAdmin2) {
//       displayNames ++= feature.adminCode.toList.map(code => {
//         DisplayName("abbr", code, FeatureNameFlags.ABBREVIATION.getValue)
//       })
//     }

//     def fixParent(p: String): Option[String] = {
//       adminIdMap.get(p) orElse {
//         //println("missing admin lookup for %s".format(p))
//         None
//       }
//     }

//     // Build parents
//     val extraParents: List[StoredFeatureId] =
//       feature.extraColumns.get("parents").toList.flatMap(_.split(",").toList).flatMap(pStr =>
//         StoredFeatureId.fromHumanReadableString(pStr))
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

//     var lat = feature.latitude
//     var lng = feature.longitude

//     val latlngs = moveTable.get(geonameId)
//     if (latlngs.size > 0) {
//       lat = latlngs(0).toDouble
//       lng = latlngs(1).toDouble
//     }

//     val canGeocode = feature.extraColumns.get("canGeocode").map(_.toInt).getOrElse(1) > 0

//     val polygonExtraEntry: Option[Geometry] = feature.extraColumns.get("geometry").map(polygon => {
//       wktReader.read(polygon)
//     })

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

//     val record = GeocodeRecord(
//       _id = geonameId.longId,
//       ids = ids.map(_.longId),
//       names = Nil,
//       cc = feature.countryCode,
//       _woeType = feature.featureClass.woeType.getValue,
//       lat = lat,
//       lng = lng,
//       parents = allParents.map(_.longId),
//       population = feature.population,
//       displayNames = displayNames,
//       boost = boost,
//       boundingbox = bbox,
//       canGeocode = canGeocode,
//       slug = slug,
//       polygon = polygonExtraEntry.map(wkbWriter.write),
//       hasPoly = polygonExtraEntry.map(e => true)
//     )

//     if (attributesSet) {
//       record.setAttributes(Some(attributesBuilder.result))
//     }

//     record
//   }

//   def parseFromFile(filename: String,
//     lineProcessor: (Int, String) => Option[GeonamesFeature],
//     typeName: String,
//     allowBuildings: Boolean = false): DList[(StoredFeatureId, GeonamesFeature)] = {

//     val lines = fromTextFile(filename)

//     (for {
//       line <- lines
//       feature <- lineProcessor(index, line)
//       if (
//         !f.featureClass.isStupid &&
//         !(f.name.contains(", Stadt") && f.countryCode == "DE") &&
//         !f.geonameid.exists(ignoreList.contains) &&
//         (!f.featureClass.isBuilding || config.shouldParseBuildings || allowBuildings)
//       )
//     } yield {
//       (feature.id, feature)
//     })
//   }

//   def run() {
//     val config = new GeonamesImporterConfig(args)

//     if (!config.parseWorld) {
//       // val countries = config.parseCountry.split(",")
//       // countries.foreach(f => {
//       //   parser.logger.info("Parsing %s".format(f))
//       //   parseAdminInfoFile("data/computed/adminCodes-%s.txt".format(f))
//       //   parser.parseAdminFile(
//       //     "data/downloaded/%s.txt".format(f))

//       //   if (config.importPostalCodes) {
//       //     parser.parsePostalCodeFile("data/downloaded/zip/%s.txt".format(f))
//       //   }
//       // })
//     } else {
//       // parseAdminInfoFile("data/computed/adminCodes.txt")
//       parseAdminFile(
//         "data/downloaded/allCountries.txt")
//       // if (config.importPostalCodes) {
//       //   parser.parsePostalCodeFile("data/downloaded/zip/allCountries.txt")
//       // }
//     }


//     Tsv( args("input") )
//       .flatMap('line -> 'word) { line : String => line.split("""\s+""") }
//       .groupBy('word) { _.size }
//       .write( Tsv( args("output") ) )
//   }
// }
