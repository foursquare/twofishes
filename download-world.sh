#!/bin/bash

set -x

unzip -v >/dev/null 2>&1 || { echo >&2 "I require 'unzip' but it's not installed.  Aborting."; exit 1; }
curl -h >/dev/null 2>&1 || { echo >&2 "I require 'curl' but it's not installed.  Aborting."; exit 1; }

mkdir -p data/downloaded/
mkdir -p data/downloaded/zip/

FILE=data/downloaded/allCountries.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/dump/allCountries.zip
   unzip -o $FILE.zip
   mv allCountries.txt $FILE
   rm $FILE.zip
fi

FILE=data/downloaded/zip/allCountries.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/zip/allCountries.zip
   unzip -o $FILE.zip
   mv allCountries.txt $FILE
   rm $FILE.zip
fi

source scripts/download-common.sh
./scripts/extract-wiki-buildings.py
./scripts/extract-adm.py
