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
- purpose: For each feature in the matched country (* for any), if the string is present, delete it, and mark the resulting string as a SHORT_NAME which will usually be preferred for display.

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
