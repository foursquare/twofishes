// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._

class TwofishesImporterJob(
  name: String,
  inputSpec: TwofishesImporterInputSpec,
  args: Args
) extends TwofishesJob(name, args) {

  private def getAllInputFiles(inputSpec: TwofishesImporterInputSpec): Seq[String] = {
    getFilesByRelativePaths(inputSpec.relativeFilePaths) ++
      inputSpec.directories.flatMap(getFilesInDirectoryByEnumerationSpec(_))
  }

  private val inputFiles = getAllInputFiles(inputSpec)

  val lines: TypedPipe[String] = if (inputFiles.nonEmpty) {
    TypedPipe.from(MultipleTextLineFiles(inputFiles: _*))
  } else {
    TypedPipe.empty
  }
}
