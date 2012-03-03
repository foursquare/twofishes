#!/bin/sh

set -x

mkdir -p data/downloaded/
mkdir -p data/downloaded/zip/

FILE=data/downloaded/allCountries.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/dump/allCountries.zip
   unzip $FILE.zip
   mv allCountries.txt $FILE
   rm $FILE.zip
fi

FILE=data/downloaded/alternateNames.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/dump/alternateNames.zip
   unzip $FILE.zip
   mv alternateNames.txt $FILE
   rm $FILE.zip
fi

FILE=data/downloaded/zip/allCountries.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/zip/allCountries.zip
   unzip $FILE.zip
   mv allCountries.txt $FILE
   rm $FILE.zip
fi