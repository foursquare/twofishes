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

class HierarchyImporterJob(args: Args) extends BaseRelationsImporterJob(
  name = "hierarchy_import",
  // hierarchy is specified as parent, child but aggregated on child so invert from and to
  fromColumnIndex = 1,
  toColumnIndex = 0,
  lineAcceptor = {parts => {
    val hierarchyType = parts.lift(2).getOrElse("")
    (hierarchyType == "ADM")
  }},
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "downloaded/hierarchy.txt",
      "private/hierarchy.txt",
      "custom/hierarchy.txt"),
    directories = Nil),
  args = args)

class ConcordancesImporterJob(args: Args) extends BaseRelationsImporterJob(
  name = "concordances_import",
  fromColumnIndex = 0,
  toColumnIndex = 1,
  lineAcceptor = { parts => true },
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "computed/concordances.txt",
      "private/concordances.txt"),
    directories = Nil),
  args = args)

class ExtraRelationsImporterJob(args: Args) extends BaseRelationsImporterJob(
  name = "extra_relations_import",
  fromColumnIndex = 0,
  toColumnIndex = 1,
  lineAcceptor = { parts => true },
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "custom/extra-relations.txt"),
    directories = Nil),
  args = args)

class BoostsImporterJob(args: Args) extends BaseBoostsImporterJob(
  name = "boosts_import",
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "private/boosts.txt",
      "custom/boosts.txt"),
    directories = Nil),
  args = args)