[![Build Status](https://travis-ci.org/foursquare/twofishes.svg?branch=master)](https://travis-ci.org/foursquare/twofishes)

A coarse, splitting geocoder and reverse geocoder in scala -- Prebuilt indexes and binaries available at [twofishes.net](http://twofishes.net/). Discussion at [google groups](https://groups.google.com/forum/?fromgroups#!forum/twofishes).

What is a Geocoder?
===================

A geocoder is a piece of software that translates from strings to coordinates. "New York, NY" to "40.74,  -74.0". This is an implementation of a coarse (city level, meaning it can't understand street addresses) geocoder that also supports splitting (breaking off the non-geocoded part in the final response).

Overview
========

This geocoder was designed around the geonames data, which is relatively small, and easy to parse in a short amount of time in a single thread without much post-processing. Geonames is a collection of simple text files that represent political features across the world. The geonames data has the nice property that all the features are listed with stable identifiers for their parents, the bigger political features that contain them (rego park -> queens county -> new york state -> united states). In one pass, we can build a database where each entry is a feature with a list of names for indexing, names for display, and a list of parents.

The Data
========

- [Twofishes Input Data File Documentation](docs/twofishes_inputs.md)


Geonames is great, but not perfect. Southeast Asia doesn't have the most comprehensive coverage. Geonames doesn't have bounding boxes, so we add some of those from http://code.flickr.com/blog/2011/01/08/flickr-shapefiles-public-dataset-2-0/ where possible.

Geonames is licensed under CC-BY http://www.geonames.org/. They take a pretty liberal interpretation of this and just ask for about page attribution if you make use of the data.
Flickr shapefiles are public domain

Reverse Geocoding and Polygons
==============================
To enable reverse geocoding in twofishes, you need to add polygon data to the inputs. geonames does not distribute polygons, nor does the twofishes distribution contain shapefiles. Shapefiles must be in epsg:4326 projection. The following script will write a copy of your shapefile with an extra property that is the geonameid of the matching feature.

I will add automated scripts for this soon, but for now, if you have shapefiles that map to existing geonames features that you want to put into twofishes
*   load geonames in postgis accoridng to https://github.com/colemanm/gazetteer/blob/master/docs/geonames_postgis_import.md
*   run the shape-gn-matchr script from  https://github.com/blackmad/shputils (see instructions below)
*   take the resulting shapefile (the shp, shx, cpg, dbf) and put them in data/private/polygons

examples:

US place (locality) data -- ftp://ftp2.census.gov/geo/tiger/TIGER2010/PLACE/2010/
~/shputils/shape-gn-matchr.py --shp_name_keys=NAME10 tl_2010_35_place10.shp gn-tl_2010_35_place10.shp

US county data -- ftp://ftp2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/
../shputils/shape-gn-matchr.py --dbname=gis --shp_name_keys=NAME10 --allowed_gn_classes='' --allowed_gn_codes=ADM2 --fallback_allowed_gn_classes='' --fallback_allowed_gn_codes='' tl_2010_us_county10.shp gn-us-adm2.shp

MX locality data -- http://blog.diegovalle.net/2013/02/download-shapefiles-of-mexico.html
ogr2ogr -t_srs EPSG:4326  mx-4326.shp MUNICIPIOS.shp
./shputils/shape-gn-matchr.py --dbname=gis --shp_name_keys=NOM_MUN mx-4326.shp gn-mx-localities.shp

Requirements
============
*   Java (jre and jdk)
*   [Mongo](http://www.mongodb.org/display/DOCS/Quickstart)
*   curl
*   unzip

First time setup
================
*   git clone https://github.com/foursquare/twofishes.git
*   cd twofishes
*   If you want to download country: ./download-country.sh [ISO 3166 country code] (For example US, GB, etc)
*   If you want to download world: ./download-world.sh

Data import
===========
*   mongod --dbpath /local/directory/for/output/
*   If you want to import countries: ./parse.py -c US /output/dir (Note that you can specify list of countries separating them by comma: US,GB,RU)
*   If you want to import world: ./parse.py -w /output/dir

Serving
=======
*   ./serve.py -p 8080 /output/dir – Where /output/dir will contain a subdirectory whose name will be the date of the most recent build, for example `2013-02-25-01-08-23.803740`. You need to point to this subdirectory or to a folder called `latest` which is created during the build process (in the twofishes directory) and is a symlink to the most recent dated subdirectory.
*   server should be responding to finagle-thrift on the port specified (8080 by default), and responding to http requests at the next port up: <http://localhost:8081/?query=rego+park+ny> <http://localhost:8081/static/geocoder.html#rego+park>
*   use the --host flag to specify a bind address (defaults to 0.0.0.0)
*   to enable hotfixes and allow refreshing, use the --hotfix\_basepath and --enable\_private\_endpoints params as detailed under [Hotfixes](#hotfixes) below 

NOTE: mongod is not required for serving, only index building.

A better option is to run "./sbt server/assembly" and then use the resulting server/target/server-assembly-VERSION.jar. Serve that with java -jar JARFILE --hfile_basepath /directory

Hotfixes
========
<a name="hotfixes"></a>
Hotfixes are expressed as fine-grained edits on top of features in the index. Features can be quickly added, removed or modified on a live server without requiring a full index rebuild and redeploy. Most fields on a [GeocodeServingFeature](https://github.com/foursquare/twofishes/blob/master/interface/src/main/thrift/geocoder.thrift#L216) and fields on its nested structs can be edited via a [GeocodeServingFeatureEdit](https://github.com/foursquare/twofishes/blob/master/interface/src/main/thrift/feature_edits.thrift#L35) object.

To enable hotfix support, the server can be pointed to a hotfix directory at startup via the --hotfix\_basepath param. Any .json files found in this directory will be deserialized from JSON to Thrift.

There is only basic tooling to build these JSON hotfix files at present. In [JsonHotfixFileBuilder.scala](https://github.com/foursquare/twofishes/blob/master/server/src/main/scala/JsonHotfixFileBuilder.scala), use `GeocodeServingFeatureEdit.newBuilder` to build up individual hotfixes in code. Then run build-hotfix-file.py specifying an output file. I will provide a better way shortly.

The server can reload hotfixes on-demand via the /refreshStore endpoint. There is no authentication on this endpoint (or any other private endpoints), so it is disabled by default. Use the --enable\_private\_endpoints param to enable at your own risk, only if your servers are not publicly accessible. When enabled, calling this endpoint on an individual server will cause it to re-scan the hotfix_basepath directory. Use the helper script refresh-store.py.

Troubleshooting
===============
If you see a java OutOfMemory error at start, you may need to up your # of mapped files

on linux: sysctl -w vm.max\_map\_count = 131072

Talking to the Server
=====================
- [Twofishes Request Format Documentation](docs/twofishes_requests.md)

Technical Details
=================
I use mongo to save state during the index building phase (so that, for instance, we can parse the alternateNames file, which adds name+lang pairs to features defined in a separate file, or adding the flickr bounding boxes). A final pass goes over the database, dereferences ids and outputs some hadoop mapfiles and hfiles. These two hfiles are all that is required for serving the data.

If we were doing heavier processing on the incoming data, a mapreduce that spits out hfiles might make more sense.

When we parse a query, we do a rough recursive descent parse, starting from the left. If being used to split geographic queries like "pizza new york" we expect the "what" to be on the left. All of the features found in a parse must be parents of the smallest

The geocoder currently may return multiple valid parses, however, it only returns the longest possible parses. For "Springfield, US" we will return multiple features that match that query (there are dozens of springfields in the US). It will not return a parse of "Springfield" near "US" with only US geocoded if it can find a longer parse, but it will return multiple valid interpretations of the longest parse.

Performance
===========
Twofishes can handle 100s of queries a second at < 5ms/query on average.

Point reverse geocoding is absurdly performant -- 1000s of queries a second at < 1ms/query.

Future
======
I'd like to integrate more data from OSM and possibly an entire build solely from OSM. I'd also like to get supplemental data from the Foursquare database where possible. If I was feeling more US-centric, I'd parse the TIGER-line data for US polygons, but I'm expecting those to mostly be in OSM.

Also US-centric are zillow neighborhood polygons, also CC-by-SA. I might add an "attribution" field to the response for certain datasources. I'm not looking forward to writing a conflater with precedence for overlapping features from different data sets.

Contributors
============
* David Blackman <blackmad@twitter.com>
* Rahul Maddimsetty <rahul@foursquare.com>
* John Gallagher <john@foursquare.com>
* Jeff Kao <jeffk@foursquare.com>
* Arkadiy Kukarkin <parkan@gmail.com>
* Neil Sanchala

Many thanks for assistance:
* Jorge Ortiz

Unrelated
=========
These are the two fishes I grilled the night I started coding the original python implementation <https://twitter.com/#!/whizziwig/statuses/154431957630066688>

