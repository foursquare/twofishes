echo "
import com.mongodb.casbah.Imports._
import com.foursquare.twofishes._
import com.foursquare.twofishes.importers.geonames._

MongoGeocodeDAO.find(MongoDBObject()).foreach(f => {
if (List(YahooWoeType.TOWN, YahooWoeType.SUBURB, YahooWoeType.COUNTRY, YahooWoeType.ADMIN1, YahooWoeType.ADMIN2).contains(f.woeType)) {
  val id = f.featureIds.find(_.namespace == GeonamesNamespace).getOrElse(f.featureIds(0))
  GeonamesParser.slugIndexer.addMissingSlug(id)
}
})
GeonamesParser.slugIndexer.buildMissingSlugs" | ./sbt indexer/console > output.txt
