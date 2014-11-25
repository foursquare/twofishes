// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import com.twitter.scalding.typed.EmptyTypedPipe

class TwofishesImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesJob(name, args) {

  val inputBaseDir = args("input")

  private def listFilesWrapper(fs: FileSystem, path: Path, recursive: Boolean): Seq[String] = {
    var files = new ListBuffer[String]()
    val iterator = fs.listFiles(path, recursive)
    while (iterator.hasNext) {
      val locatedFileStatus = iterator.next
      files += locatedFileStatus.getPath.toString
    }
    files.toSeq
  }

  private def getFilesInDirectoryByEnumerationSpec(spec: DirectoryEnumerationSpec): Seq[String] = {
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

  private def getFilesByRelativePaths(relativePaths: Seq[String]): Seq[String] = {
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

  private def getAllInputFiles(inputSpec: TwofishesImporterInputSpec): Seq[String] = {
    getFilesByRelativePaths(inputSpec.relativeFilePaths) ++
      inputSpec.directories.flatMap(getFilesInDirectoryByEnumerationSpec(_))
  }

  val inputFiles = getAllInputFiles(inputSpec)

  val lines: TypedPipe[String] = if (inputFiles.nonEmpty) {
    TypedPipe.from(MultipleTextLineFiles(inputFiles: _*))
  } else {
    EmptyTypedPipe(flowDef, mode)
  }
}