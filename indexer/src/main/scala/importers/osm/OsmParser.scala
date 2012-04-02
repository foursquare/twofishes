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

  protected def parseRelations(rels: java.util.List[Osmformat.Relation]) {}
  /** Parse a DenseNode protocol buffer and send the resulting nodes to a sink.  */
  protected def parseDense(nodes: Osmformat.DenseNodes) {}
  /** Parse a list of Node protocol buffers and send the resulting nodes to a sink.  */
  protected def parseNodes(nodes: java.util.List[Osmformat.Node]) {
    nodes.foreach(node => {
      0.to(node.getKeysCount()).foreach(kIndex => {
        val key = getStringById(node.getKeys(kIndex));
        val value = getStringById(node.getVals(kIndex));
        println("%s %s".format(key, value))
      })
    })
  }
  /** Parse a list of Way protocol buffers and send the resulting ways to a sink.  */
  protected def parseWays(ways: java.util.List[Osmformat.Way]) {}
  /** Parse a header message. */
  protected def parse(header: Osmformat.HeaderBlock) {}

  protected def complete() {}
}