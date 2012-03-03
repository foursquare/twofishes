import java.io.File
//import org.slf4j._

object GeonamesImporterConfig {
  val shouldParseBuildings = false

}

trait LogHelper {
//  val logger = LoggerFactory.getLogger(classOf[this])
  class FakeLogger {
    def debug(s: String) { println(s) }
    def error(s: String) { println(s) }
    def info(s: String)  { println(s) }
  }

  val logger = new FakeLogger()
}

 object GeonamesFeatureColumns extends Enumeration {
   type GeonamesFeatureColumns = Value
   val GEONAMEID, PLACE_NAME, NAME, ASCIINAME, ALTERNATENAMES, LATITUDE, LONGITUDE,
      FEATURE_CLASS, FEATURE_CODE, COUNTRY_CODE, CC2, ADMIN1_CODE, ADMIN2_CODE, ADMIN3_CODE,
      ADMIN4_CODE, ADMIN1_NAME, ADMIN2_NAME, ADMIN3_NAME, POPULATION, ELEVATION, GTOPO30, TIMEZONE,
      MODIFICATION_DATE, ACCURACY = Value
}

import GeonamesFeatureColumns._

object GeonamesFeature extends LogHelper {
  // fake column names


  val adminColumns = List(
    GEONAMEID,
    NAME,
    ASCIINAME,
    ALTERNATENAMES,
    LATITUDE,
    LONGITUDE,
    FEATURE_CLASS,
    FEATURE_CODE,
    COUNTRY_CODE,
    CC2,
    ADMIN1_CODE,
    ADMIN2_CODE,
    ADMIN3_CODE,
    ADMIN4_CODE,
    POPULATION,
    ELEVATION,
    GTOPO30,
    TIMEZONE,
    MODIFICATION_DATE
  )

  def parseFromAdminLine(index: Int, line: String): Option[GeonamesFeature] = {
    val parts = line.split("\t")
    if (parts.size != adminColumns.size) {
      logger.error("line %d has the wrong number of columns. Has %d, needs %d".format(
        index, parts.size, adminColumns.size))
      None
    } else {
      val colMap = adminColumns.zip(parts).toMap
      Some(new GeonamesFeature(colMap))
    }
  }
}

object AdminLevel extends Enumeration {
  type AdminLevel = Value
  val COUNTRY, ADM1, ADM2, ADM3, ADM4, OTHER = Value
}

import AdminLevel._

// http://www.geonames.org/export/codes.html
class GeonamesFeatureClass(featureClass: Option[String], featureCode: Option[String]) {
  def isBuilding = featureClass.exists(_ == "S")
  def isCountry = featureCode.exists(_.contains("PCL"))
  def isAdmin = adminLevel != OTHER

  def adminLevel: AdminLevel.Value = {
    if (isCountry) {
      COUNTRY
    } else {
      featureCode.map(_ match {
        case "ADM1" => ADM1
        case "ADM2" => ADM2
        case "ADM3" => ADM3
        case "ADM4" => ADM4
        case _ => OTHER
      }).getOrElse(OTHER) 
    }
  }
}

class GeonamesFeature(values: Map[GeonamesFeatureColumns.Value, String]) {
  val featureClass = new GeonamesFeatureClass(values.get(FEATURE_CLASS), values.get(FEATURE_CODE))

  def adminCode(level: AdminLevel.Value): Option[String] = {
    level match {
      case COUNTRY => values.get(COUNTRY_CODE)
      case ADM1 => values.get(ADMIN1_CODE)
      case ADM2 => values.get(ADMIN2_CODE)
      case ADM3 => values.get(ADMIN3_CODE)
      case ADM4 => values.get(ADMIN4_CODE)
      case _ => None
    }
  }

  def makeAdminId(level: AdminLevel.Value): String = {
    AdminLevel.values.filter(_ <= level).flatMap(l =>
      adminCode(l)).mkString("-")
  }

  def parents = {
  }
}

class GeonamesParser extends LogHelper {
  def parseFeature(feature: GeonamesFeature) {

  }

  def parseFromFile(filename: String) {
    val lines = scala.io.Source.fromFile(new File(filename)).getLines
    lines.zipWithIndex.foreach({case (line, index) => {
      if (index % 1000 == 0) {
        logger.info("imported %d features so far".format(index))
      }
      val feature = GeonamesFeature.parseFromAdminLine(index, line)
      feature.foreach(f => {
        if (!f.featureClass.isBuilding || GeonamesImporterConfig.shouldParseBuildings) {
          parseFeature(f)
        }
      })
    }})
  }
}