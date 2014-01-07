# Overview

This document describes the various input files that are read in the process of creating a twofishes index.

They are grouped into semi-logical directories:

- data/downloaded -- this is where data downloaded from geonames and natural earth is saved. The download-country.sh and download-world.sh scripts save data here
- data/custom -- various transforms on the geonames data that is checked into git
- data/private is a mirror of much of the file structure of data/custom and data/downloaded for files that the developer doesn't want checked into git. It is listed in .gitignore

## data/downloaded
The scripts download-country.sh and download-world.sh populate this directory with data from geonames (and one datafile from natural earth that isn't quite working).

The formats for geonames files can be found at <http://download.geonames.org/export/dump/readme.txt>

### common files
from geonames:

- alternateNames.txt
- hierarchy.txt
- admin1CodesASCII.txt
- countryInfo.txt

download-country.sh is used if you want to build twofishes for only one country. 

- downloaded/[COUNTRYCODE].txt 
- downloaded/zip/[COUNTRYCODE].txt

download-world.sh is the default case, build the entire world index. 

- downloaded/allCountries.txt 
- downloaded/zip/allCountries.txt

## data/custom
This is a directory of files checked into git that performs various transforms on the incoming geonames data. They are manual overrides that massage the data into being a better geocoder. The format of the files is a bit of a mess.

##### rewrites.txt
- format: pipe-delimited regexp + comma separated list of replacements
   - ex: Saint\b|St,Ste
   - ex: Mountain\b|Mount,Mt,Mtn
- purpose: For every name of every feature, generate new names by replacing the regexp on the left with each of the replacements on the right.
   - For example, having Mount\b|Mt,Mountain,Mtn in rewrites.txt, it indexes Mount Laurel, Mt Laurel, Mtn Laurel and Mountain Laurel when processing Mount Laurel.

##### shortens.txt
- format: pipe-delimited country code + string to delete
- format: pipe-delimited country code + string to delete + replacement + name flags
   - ex: ID|Provinsi
   - ex: *|Province del [any country]
   - ex: *|Township of||ALT_NAME
- purpose: For each feature in the matched country (* for any), if the string is present, delete it, and mark the resulting string as a SHORT_NAME which will usually be preferred for display. (Only takes the shortest name generated per input name, so we don't end up with "of X" and "X" when processing "Province of X")

##### deletes.txt
- format: string to delete
   - ex: Township
   - ex: Township of
- purpose: Similar to shortens.txt and should probably be combined. All deletes are global, and generate ALT_NAME rather than SHORT_NAME, so they are not preferred for display. Does not do the intelligent deduping of shortens.txt
 
##### name-deletes.txt
- format: pipe-delimited geonameid + name
    - ex: 2511174|Tenerife
- purpose: sometimes, the geonames data has incorrect names on a POI. So incorrect that it is messing up geocoding. This deletes all instance of 'name' from the POI matching geonameid and does not index those names.

##### ignores.txt
- format: one geonameid per line
- purpose: entirely skips indexing of POIs with these geonameids.

##### extra-relations.txt
- format: pipe-delimited child geonameid + parent geonameid
    - ex: 3646738|3632191 [ caracas -> miranda, VE ]
- purpose: add an extra relation to the child geonameid, effectively making 'parent geonameid' a parent of child, though not used in the parent hierarchy for constructing names. Sometimes the administrative hierarchy in geonames is wrong, or doesn't reflect common usage. This allows us to add parents so geocoding succeeds.

##### boosts.txt
- format: pipe-delimited geonameid + integer
    - ex: 5368361|10000000
- purpose: add a scoring boost to the geonameid, beyond what is found in data/private/boosts.txt

##### name-transforms/*txt
- format: geonameid[space]language[pipe]name
- format: geonameid[space]language[pipe]name[pipe]comma-separated list of name flags to apply
    - ex: 5391997 en|San Francisco|ALT_NAME
    - ex: 443093 en|Qatzrin
- purpose: for each matched geonameid, either add or modify the matching name+language. If not found, add the name. If found, add the flags to the existing name. If no flags are specified, defaults to making the name PREFERRED. If PREFERRED is specified, unsets PREFERRED from all other names in that language (so this name becomes the one preferred name for that language).

## data/private and data/computed
Mostly a mirror of data/custom combined with data/downloaded.

data/private is meant for when a developer wants to add data and be sure that it won't get checked into git. data/computed is either files I've checked into git computed from other sources or are generated as part of download-X.sh

#### bounding boxes
searched for in

- data/computed/bboxes/*
- data/private/bboxes/*
- 
ex: geonameid:4246863	-88.3033218384	39.8994483948	-88.2112045288	39.9387893677

Added to features that don't have a polygon shape as the bounding box. If a feature has a bounding box, the polygon bounding box is used.

#### "display" bounding boxes
searched for in

- data/computed/display_bboxes/*
- data/private/display_bboxes/*

same format as bboxes. Added in addition to a bounding box and returned in a different field. Meant for when the developer wants to have a real bbox and one meant for display. One usage of this would be a display bbox for manhattan that only includes lower manhattan (because fitting all of manhattan on screen requires zooming out so far)

#### extra features
search for in

- data/computed/features
- data/private/features

Same format as geonames allCountries.txt tsv if the developer wants to add features that are not in geonames but map into the hierarchy.

### data/custom mirrors

- boosts.txt
- rewrites.txt
- hierarchy.txt

### data/downloaded mirrors

- alternateNames/*


## Polygons
Polygons are loaded from 

- data/computed/polygons/*
- data/private/polygons/*

The directories are searched recursively, so subdirectories of these can help organize the data.

They can come in a number of formats

#### geojson polygons
Files that end in .json or .geojson are parsed as geojson.

First, if the file is named INTEGER.json, all polygons in the file will be added to the geoname feature with geonameid INTEGER.

Otherwise, each feature is processed individually and attached to the geonameid named in the geojson properties block by one of the following keys: geonameid, qs_gn_id or gn_id in that order.

#### shapefile polygons
When a file is found with the extension ".shp", the shapefile is loaded.

INTEGER.shp is not supported.

Similar to geojson processing, we look for a geonameid in the properties of each feature/shape with the keys geonameid, qs_gn_id or gn_id in that order. 








