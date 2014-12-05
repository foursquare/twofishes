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

class SlugsImporterJob(args: Args) extends BaseSlugsImporterJob(
  name = "slugs_import",
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "private/slugs.txt",
      "custom/slugs.txt"),
    directories = Nil),
  args = args)

class IgnoresImporterJob(args: Args) extends BaseFeatureEditsImporterJob(
  name = "ignores_import",
  lineProcessor = FeatureEditLineProcessors.processIgnoreLine,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "custom/ignores.txt"),
    directories = Nil),
  args = args)

class MovesImporterJob(args: Args) extends BaseFeatureEditsImporterJob(
  name = "moves_import",
  lineProcessor = FeatureEditLineProcessors.processMoveLine,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "custom/moves.txt"),
    directories = Nil),
  args = args)

class NameDeletesImporterJob(args: Args) extends BaseFeatureEditsImporterJob(
  name = "name_deletes_import",
  lineProcessor = FeatureEditLineProcessors.processNameDeleteLine,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Seq(
      "custom/name-deletes.txt"),
    directories = Nil),
  args = args)

class NameTransformsImporterJob(args: Args) extends BaseFeatureEditsImporterJob(
  name = "name_transforms_import",
  lineProcessor = FeatureEditLineProcessors.processNameTransformLine,
  inputSpec = TwofishesImporterInputSpec(
    relativeFilePaths = Nil,
    directories = Seq(
      DirectoryEnumerationSpec("custom/name-transforms"),
      DirectoryEnumerationSpec("private/name-transforms")
    )),
  args = args)

class BoundingBoxJoinIntermediateJob(args: Args) extends BaseFeatureJoinIntermediateJob(
  name = "bbox_join_intermediate",
  leftSources = Seq(
    "geonames_features_import",
    "supplemental_features_import",
    "postcode_features_import"),
  rightSources = Seq("bbox_import"),
  joiner = FeatureJoiners.boundingBoxJoiner,
  args = args)

class DisplayBoundingBoxJoinIntermediateJob(args: Args) extends BaseFeatureJoinIntermediateJob(
  name = "display_bbox_join_intermediate",
  leftSources = Seq("bbox_join_intermediate"),
  rightSources = Seq("display_bbox_import"),
  joiner = FeatureJoiners.displayBoundingBoxJoiner,
  args = args)

class ExtraRelationsJoinIntermediateJob(args: Args) extends BaseFeatureJoinIntermediateJob(
  name = "extra_relations_join_intermediate",
  leftSources = Seq("display_bbox_join_intermediate"),
  rightSources = Seq("extra_relations_import"),
  joiner = FeatureJoiners.extraRelationsJoiner,
  args = args)

class BoostsJoinIntermediateJob(args: Args) extends BaseFeatureJoinIntermediateJob(
  name = "boosts_join_intermediate",
  leftSources = Seq("extra_relations_join_intermediate"),
  rightSources = Seq("boosts_import"),
  joiner = FeatureJoiners.extraRelationsJoiner,
  args = args)
