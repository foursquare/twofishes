#!/bin/sh

set -x

mkdir -p data/downloaded/
mkdir -p data/downloaded/zip/

FILE=data/downloaded/US.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/dump/US.zip
   unzip $FILE.zip
   mv US.txt $FILE
   rm readme.txt
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

FILE=data/downloaded/zip/US.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/zip/US.zip
   unzip $FILE.zip
   mv US.txt $FILE
   rm readme.txt
   rm $FILE.zip
fi
