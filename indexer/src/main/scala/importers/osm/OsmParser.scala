import crosby.binary._
import crosby.binary.file.BlockInputStream
import scala.collection.JavaConversions._
import java.io._

class OsmParser extends BinaryParser {
  def doParse(filename: String) {
    val fis = new FileInputStream(filename)
    val bis = new BlockInputStream(fis, this)
    bis.process()
  }

  override protected def parseRelations(rels: java.util.List[Osmformat.Relation]) {
  }

  /** Parse a DenseNode protocol buffer and send the resulting nodes to a sink.  */
  override protected def parseDense(nodes: Osmformat.DenseNodes) {
    var lastId: Long = 0
    var lastLat: Long = 0
    var lastLon: Long = 0
 
    0.until(nodes.getIdCount()).foreach(nid => {
      val lat = nodes.getLat(nid) + lastLat
      val lon = nodes.getLon(nid) + lastLon
      val id = nodes.getId(nid) + lastId
      lastLat = lat
      lastLon = lon
      lastId = id

      println("%s @ %s,%s".format(id, lat, lon))

      0.until(nodes.getKeysValsCount(), 2).foreach(kvid => {
        val keyid = nodes.getKeysVals(kvid)
        val valid = nodes.getKeysVals(kvid + 1)
        val key = getStringById(keyid)
        val value = getStringById(valid)
        println("%s: %s".format(key, value))
      })
    })
  }


  /** Parse a list of Node protocol buffers and send the resulting nodes to a sink.  */
  override protected def parseNodes(nodes: java.util.List[Osmformat.Node]) {
    nodes.foreach(node => {
      0.to(node.getKeysCount()).foreach(kIndex => {
        val key = getStringById(node.getKeys(kIndex));
        val value = getStringById(node.getVals(kIndex));
        println("%s %s".format(key, value))
      })
    })
  }
  /** Parse a list of Way protocol buffers and send the resulting ways to a sink.  */
  override protected def parseWays(ways: java.util.List[Osmformat.Way]) { }
  /** Parse a header message. */
  override protected def parse(header: Osmformat.HeaderBlock) { }

  protected def complete() {}
}