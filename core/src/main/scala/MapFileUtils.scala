package com.foursquare.twofishes

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import java.net.URI
import java.io.{File, FileReader, FileWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, LocalFileSystem, Path, PositionedReadable, Seekable}
import org.apache.hadoop.io.{BytesWritable, MapFile, SequenceFile, Writable, WritableComparator}
import org.apache.hadoop.util.Options
import scalaj.collection.Implicits._

class MemoryMappedSequenceFileReader(conf: Configuration, val shouldPreload: Boolean, options: SequenceFile.Reader.Option*)
    extends SequenceFile.Reader(conf, options: _*) {

  override protected def openFile(fs: FileSystem, path: Path, bufferSize: Int, length: Long): FSDataInputStream = {
    assert(fs.isInstanceOf[LocalFileSystem])
    val mmapIS = new MMapInputStream(path.toString)
    if (shouldPreload) {
      mmapIS.preload
    }
    new FSDataInputStream(mmapIS)
  }
}

class MemoryMappedMapFileReader(val path: Path, val conf: Configuration, val shouldPreload: Boolean)
    extends MapFile.Reader(path, conf) {

  override protected def createDataFileReader(
      dataFile: Path, conf: Configuration, options: SequenceFile.Reader.Option*): SequenceFile.Reader = {
    val newOptions = Options.prependOptions(options.toArray, SequenceFile.Reader.file(dataFile))
    return new MemoryMappedSequenceFileReader(conf, shouldPreload, newOptions: _*)
  }
}

object MapFileUtils {
  def readerAndInfoFromLocalPath(path: String, shouldPreload: Boolean): (MapFile.Reader, Map[String, String]) = {
    val fs = new LocalFileSystem
    val conf = new Configuration
    fs.initialize(URI.create("file:///"), conf)

    val file = new File(path, "info.tsv")
    val csv: Seq[Array[String]] = new CSVReader(new FileReader(file), '\t').readAll.asScala
    if (csv.exists(_.size != 2)) {
      throw new RuntimeException("Expected file info at %s to be a CSV with two columns per line.".format(file))
    } else if (csv.size != csv.map(_.head).distinct.size) {
      throw new RuntimeException("Expected first column at %s to be unique, got a duplicate key. All keys: %s".format(
        file, csv.map(_.head).mkString(",")))
    }

    val reader = new MemoryMappedMapFileReader(new Path(path), conf, shouldPreload)
    (reader, csv.map(arr => (arr(0) -> arr(1))).toMap)
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

    val file = new File(path, "info.tsv")
    val csv = new CSVWriter(new FileWriter(file), '\t')
    csv.writeAll(info.toList.map(kv => Array(kv._1, kv._2)).asJava)
    csv.close

    val writer = new MapFile.Writer(
      conf,
      new Path(path),
      MapFile.Writer.comparator(writeOptions.keyComparator),
      MapFile.Writer.valueClass(writeOptions.valueClass),
      MapFile.Writer.compression(
        if (writeOptions.compress) SequenceFile.CompressionType.BLOCK else SequenceFile.CompressionType.NONE))

    writer.setIndexInterval(writeOptions.indexInterval)

    writer
  }
}
