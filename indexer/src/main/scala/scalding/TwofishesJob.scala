// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import cascading.tap.hadoop.HfsProps
import cascading.util.Update
import com.twitter.scalding._
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{LongWritable, Text, BytesWritable}
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.conf.Configuration
import scala.util.matching.Regex
import com.twitter.scalding.filecache.{DistributedCacheFile, CachedFile}

class TwofishesJob(name: String, args: Args) extends Job(args) {

  val inputBaseDir = args("input")
  val outputBaseDir = args("output")

  val outputPath = concatenatePaths(outputBaseDir, name)

  implicit object PolygonMatchingWritableOrdering extends Ordering[PolygonMatchingKeyWritable] {
    def compare(x: PolygonMatchingKeyWritable, y: PolygonMatchingKeyWritable) = x.compareTo(y)
  }

  implicit object TextOrdering extends Ordering[Text] {
    def compare(x: Text, y: Text) = x.compareTo(y)
  }

  protected def concatenatePaths(base: String, relative: String): String = {
    // TODO: strip trailing and leading slashes
    base + "/" + relative
  }

  protected def listFilesWrapper(fs: FileSystem, path: Path, recursive: Boolean): Seq[String] = {
    var files = new ListBuffer[String]()
    val iterator = fs.listFiles(path, recursive)
    while (iterator.hasNext) {
      val locatedFileStatus = iterator.next
      files += locatedFileStatus.getPath.toString
    }
    files.toSeq
  }

  protected def getFilesInDirectoryByEnumerationSpec(spec: DirectoryEnumerationSpec): Seq[String] = {
    val path = new Path(concatenatePaths(inputBaseDir, spec.relativePath))
    val fs = path.getFileSystem(new Configuration)
    val filterRegex = spec.filter.map(pattern => new Regex(pattern))

    if (fs.exists(path) && fs.isDirectory(path)) {
      (for {
        fileName <- listFilesWrapper(fs, path, spec.recursive)
        // either there is no filter or the filename matches it
        if filterRegex.forall(r => r.findFirstIn(fileName).nonEmpty)
      } yield fileName).toSeq
    } else {
      Nil
    }
  }

  protected def getFilesByRelativePaths(relativePaths: Seq[String]): Seq[String] = {
    relativePaths.flatMap(relativePath => {
      val path = new Path(concatenatePaths(inputBaseDir, relativePath))
      val fs = path.getFileSystem(new Configuration)
      if (fs.exists(path)) {
        Seq(path.toString)
      } else {
        Nil
      }
    })
  }

  // make file locally available to mappers/reducers
  // meant for small info files typically used to populate lookup tables
  // do not use for large files
  protected def getCachedFileByRelativePath(relativePath: String): CachedFile = {
    // will throw if the file doesn't exist
    val filePath = getFilesByRelativePaths(Seq(relativePath)).head
    DistributedCacheFile(filePath)
  }

  def onSuccess(): Unit = ()

  override def run: Boolean = {
    val result = super.run
    if (result) onSuccess()
    result
  }

  override def config: Map[AnyRef, AnyRef] = {
    System.setProperty(Update.UPDATE_CHECK_SKIP, "true")
    val userName = System.getProperty("user.name", "unknown")
    super.config + (HfsProps.TEMPORARY_DIRECTORY -> s"/tmp/cascading-$userName")
  }
}