// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

case class DirectoryEnumerationSpec(
  relativePath: String,
  filter: Option[String] = None,
  recursive: Boolean = false)

case class TwofishesImporterInputSpec(
  relativeFilePaths: Seq[String],
  directories: Seq[DirectoryEnumerationSpec])
