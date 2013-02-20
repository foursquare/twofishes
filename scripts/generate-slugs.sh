echo "
import com.mongodb.casbah.Imports._
import com.foursquare.twofishes._
import com.foursquare.twofishes.importers.geonames._

MongoGeocodeDAO.find(MongoDBObject()).foreach(f => {
if (List(YahooWoeType.TOWN, YahooWoeType.SUBURB, YahooWoeType.COUNTRY, YahooWoeType.ADMIN1, YahooWoeType.ADMIN2).contains(f.woeType)) {
  val id = f.ids.find(_.startsWith(\"geonameid\")).getOrElse(f.ids(0))
  GeonamesParser.slugIndexer.missingSlugList.add(id)
}
})
GeonamesParser.slugIndexer.buildMissingSlugs" | ./sbt indexer/console > output.txt
