// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import com.weiglewilczek.slf4s.Logging
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.apache.hadoop.conf.Configuration
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer

class TwofishesJob(name: String, args: Args) extends Job(args) with Logging {
  val inputBaseDir = args("input")
  val outputBaseDir = args("output")
  val conf = new Configuration

  private def concatenatePaths(base: String, relative: String): String = {
    // TODO: strip trailing and leading slashes
    base + "/" + relative
  }

  private def listFilesWrapper(fs: FileSystem, path: Path, recursive: Boolean): Seq[String] = {
    var files = new ListBuffer[String]()
    val iterator = fs.listFiles(path, recursive)
    while (iterator.hasNext) {
      val locatedFileStatus = iterator.next
      files += locatedFileStatus.getPath.toString
    }
    files.toSeq
  }

  def getFilesInDirectoryByRelativePath(relativePath: String, filter: Option[String] = None, recursive: Boolean = false): Seq[String] = {
    val path = new Path(concatenatePaths(inputBaseDir, relativePath))
    val fs = path.getFileSystem(conf)
    val filterRegex = filter.map(pattern => new Regex(pattern))

    if (fs.isDirectory(path)) {
      (for {
        fileName <- listFilesWrapper(fs, path, recursive)
        // either there is no filter or the filename matches it
        if !filterRegex.exists(r => r.findFirstIn(fileName).isEmpty)
      } yield fileName).toSeq
    } else {
      Nil
    }
  }

  def getFilesByRelativePaths(relativePaths: Seq[String]): Seq[String] = {
    relativePaths.flatMap(relativePath => {
      val path = new Path(concatenatePaths(inputBaseDir, relativePath))
      val fs = path.getFileSystem(conf)
      if (fs.exists(path)) {
        Seq(path.toString)
      } else {
        Nil
      }
    })
  }

  val outputPath = concatenatePaths(outputBaseDir, name)

  def onSuccess(): Unit = ()

  override def run: Boolean = {
    logger.info("Starting phase: %s".format(name))
    val result = super.run
    if (result) onSuccess()
    logger.info("Finished phase: %s".format(name))
    result
  }
}