// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.BytesWritable
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.conf.Configuration
import scala.util.matching.Regex
import com.twitter.scalding.filecache.{DistributedCacheFile, CachedFile}

class TwofishesJob(name: String, args: Args) extends Job(args) {

  val inputBaseDir = args("input")
  val outputBaseDir = args("output")

  val outputPath = concatenatePaths(outputBaseDir, name)

  implicit object BytesWritableOrdering extends Ordering[BytesWritable] {
    def compare(x: BytesWritable, y: BytesWritable) = x.compareTo(y)
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
        if !filterRegex.exists(r => r.findFirstIn(fileName).isEmpty)
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
}