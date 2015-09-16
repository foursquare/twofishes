package com.foursquare.twofishes

import java.io.File
import java.net.URI
import com.foursquare.twofishes.io.MapFileConcurrentReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, MapFile, SequenceFile, Text, Writable,
    WritableComparator}
import org.apache.hadoop.util.Options
import scalaj.collection.Implicits._

class MemoryMappedSequenceFileReader(conf: Configuration, val shouldPreload: Boolean, options: SequenceFile.Reader.Option*)
    extends SequenceFile.Reader(conf, options: _*) {

  override protected def openFile(fs: FileSystem, path: Path, bufferSize: Int, length: Long): FSDataInputStream = {
    assert(fs.isInstanceOf[LocalFileSystem])
    val mmapIS = new MMapInputStream(fs.asInstanceOf[LocalFileSystem].pathToFile(path).toString)
    if (shouldPreload) {
      mmapIS.preload
    }
    new FSDataInputStream(mmapIS)
  }
}

/** A custom MapFileReader that uses MMap under the covers, and exposes the
  * Metadata from the MapFile's data file to mimic the fileInfo capability in
  * HFile's.
  *
  * Note the crazy extends {} syntax is there so we can have that
  * initialization happen before the parent's constructor is invoked. That way,
  * metadataOpt is set to None before the parent's ctor is called, then when
  * the parent's ctor calls createDataFileReader, metadataOpt gets set. If we
  * just declared that variable normally, then the parent's ctor would call
  * createDataFileReader, which would set metadataOpt, then the child ctor
  * would run and overwrite it to None. Jorge suggested this workaround for
  * that problem.
  */
class MemoryMappedMapFileReader(val path: Path, val conf: Configuration, val shouldPreload: Boolean)
    extends { private var metadataOpt: Option[SequenceFile.Metadata] = None } with MapFileConcurrentReader(path, conf) {

  // Called by the parent's ctor.
  override protected def createDataFileReader(
      dataFile: Path, conf: Configuration, options: SequenceFile.Reader.Option*): SequenceFile.Reader = {
    val newOptions = Options.prependOptions(options.toArray, SequenceFile.Reader.file(dataFile))
    val dataReader = new MemoryMappedSequenceFileReader(conf, shouldPreload, newOptions: _*)
    metadataOpt = Some(dataReader.getMetadata)
    dataReader
  }

  def metadata: SequenceFile.Metadata =
    metadataOpt.getOrElse(throw new RuntimeException("Data SequenceFile has not yet been opened!"))
}

object MapFileUtils {
  def readerAndInfoFromLocalPath(path: String, shouldPreload: Boolean):
    (MapFileConcurrentReader, Map[String, String]) = {
    val fs = new LocalFileSystem
    val conf = new Configuration
    fs.initialize(URI.create("file:///"), conf)

    val reader = new MemoryMappedMapFileReader(new Path("file://" + path), conf, shouldPreload)
    val fileInfo: Map[String, String] =
      reader.metadata.getMetadata.asScala.toMap.map(kv => kv._1.toString -> kv._2.toString)

    // Call reader.midKey to force the index to be read in immediately instead
    // of on the first invocation.
    reader.midKey

    (reader, fileInfo)
  }

  case class WriteOptions[V <: Writable](
    indexInterval: Int = 1,
    compress: Boolean = false,
    keyComparator: WritableComparator,
    valueClass: Class[V])

  val DefaultByteKeyValueWriteOptions = WriteOptions[BytesWritable](
    keyComparator = new BytesWritable.Comparator,
    valueClass = classOf[BytesWritable])

  def writerToLocalPath(
      path: String,
      info: Map[String, String],
      writeOptions: WriteOptions[_] = DefaultByteKeyValueWriteOptions): MapFile.Writer = {
    val fs = new LocalFileSystem
    val conf = new Configuration
    fs.initialize(URI.create("file:///"), conf)

    val dir = new File(path)
    if (!dir.exists) {
      dir.mkdirs
      assert(dir.exists && dir.isDirectory)
    } else {
      throw new RuntimeException(
        "%s exists, will not overwrite it to create a MapFile.".format(dir))
    }

    val infoMetadata = new SequenceFile.Metadata()
    info.foreach(kv => infoMetadata.set(new Text(kv._1), new Text(kv._2)))

    val writer = new MapFile.Writer(
      conf,
      new Path(path),
      MapFile.Writer.comparator(writeOptions.keyComparator),
      MapFile.Writer.valueClass(writeOptions.valueClass),
      MapFile.Writer.compression(
        if (writeOptions.compress) SequenceFile.CompressionType.BLOCK else SequenceFile.CompressionType.NONE),
      SequenceFile.Writer.metadata(infoMetadata))

    writer.setIndexInterval(writeOptions.indexInterval)

    writer
  }
}
