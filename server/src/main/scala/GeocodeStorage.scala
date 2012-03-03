import com.novus.salat._
import com.novus.salat.global._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoConnection

case class GeocodeRecord(
  names: List[String]
)

trait GeocodeStorageService {
  def getByName(name: String): Iterator[GeocodeRecord]
  def insert(record: GeocodeRecord): Unit
}

object MongoGeocodeDAO extends SalatDAO[GeocodeRecord, ObjectId](
  collection = MongoConnection()("geocoder")("features"))

class MongoGeocodeStorageService extends GeocodeStorageService {
  override def getByName(name: String): Iterator[GeocodeRecord] = {
    MongoGeocodeDAO.find(MongoDBObject("names" -> name))
  }

  def insert(record: GeocodeRecord) {
    MongoGeocodeDAO.insert(record)
  }
}