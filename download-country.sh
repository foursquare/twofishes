#!/bin/bash

if [ "$1" = "" ]; then
  echo "usage: $0 <2-letter-country-code>"
  echo "ex: $0 US"
  echo "ex: $0 JP"
  exit 1
fi

unzip -v >/dev/null 2>&1 || { echo >&2 "I require 'unzip' but it's not installed.  Aborting."; exit 1; }
curl -h  >/dev/null 2>&1 || { echo >&2 "I require 'curl' but it's not installed.  Aborting."; exit 1; }

COUNTRY=$1

set -x

mkdir -p data/downloaded/
mkdir -p data/downloaded/zip/

FILE=data/downloaded/$COUNTRY.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/dump/$COUNTRY.zip
   unzip $FILE.zip
   mv $COUNTRY.txt $FILE
   rm readme.txt
   rm $FILE.zip
fi


FILE=data/downloaded/zip/$COUNTRY.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/zip/$COUNTRY.zip
   unzip $FILE.zip
   mv $COUNTRY.txt $FILE
   rm readme.txt
   rm $FILE.zip
fi

source scripts/download-common.sh
./scripts/extract-wiki-buildings.py $COUNTRY
./scripts/extract-adm.py $COUNTRY
