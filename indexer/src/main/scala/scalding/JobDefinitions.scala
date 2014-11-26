// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding.Args
import com.foursquare.twofishes.importers.geonames.GeonamesFeature

class GeonamesFeaturesImporterJob(args: Args) extends BaseFeaturesImporterJob(
  name = "geonames_features_import",
  lineProcessor = GeonamesFeature.parseFromAdminLine,
  allowBuildings = false,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq("downloaded/allCountries.txt"),
    directories = Nil),
  args = args
)

class SupplementalFeaturesImporterJob(args: Args) extends BaseFeaturesImporterJob(
  name = "supplemental_features_import",
  lineProcessor = GeonamesFeature.parseFromAdminLine,
  allowBuildings = true,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Nil,
    directories = Seq(
      DirectoryEnumerationSpec("computed/features"),
      DirectoryEnumerationSpec("private/features"))),
  args = args
)

class PostalCodeFeaturesImporterJob(args: Args) extends BaseFeaturesImporterJob(
  name = "postcode_features_import",
  lineProcessor = GeonamesFeature.parseFromPostalCodeLine,
  allowBuildings = false,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq("downloaded/zip/allCountries.txt"),
    directories = Nil),
  args = args
)

class BoundingBoxImporterJob(args: Args) extends BaseBoundingBoxImporterJob(
  name = "bbox_import",
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Nil,
    directories = Seq(
      DirectoryEnumerationSpec("custom/bboxes"),
      DirectoryEnumerationSpec("private/bboxes"))),
  args: Args
)

class DisplayBoundingBoxImporterJob(args: Args) extends BaseBoundingBoxImporterJob(
  name = "display_bbox_import",
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Nil,
    directories = Seq(
      DirectoryEnumerationSpec("custom/display_bboxes"),
      DirectoryEnumerationSpec("private/display_bboxes"))),
  args = args
)

class AlternateNamesImporterJob(args: Args) extends BaseAlternateNamesImporterJob(
  name = "altnames_import",
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq("downloaded/alternateNames.txt"),
    directories = Seq(
      DirectoryEnumerationSpec("computed/alternateNames"),
      DirectoryEnumerationSpec("private/alternateNames"))),
  args = args)

class HierarchyImporterJob(args: Args) extends BaseHierarchyImporterJob(
  name = "hierarchy_import",
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "downloaded/hierarchy.txt",
      "private/hierarchy.txt",
      "custom/hierarchy.txt"),
    directories = Nil),
  args = args)