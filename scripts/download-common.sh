FILE=data/downloaded/alternateNames.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/dump/alternateNames.zip
   unzip -o $FILE.zip
   mv alternateNames.txt $FILE
   rm $FILE.zip
fi

FILE=data/downloaded/hierarchy.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE.zip http://download.geonames.org/export/dump/hierarchy.zip
   unzip $FILE.zip
   mv hierarchy.txt $FILE
   rm $FILE.zip
fi

FILE=data/downloaded/admin1CodesASCII.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE http://download.geonames.org/export/dump/admin1CodesASCII.txt
fi

FILE=data/downloaded/admin2Codes.txt
if [ -f $FILE ];
then
   echo "File $FILE exists."
else
   curl -o $FILE http://download.geonames.org/export/dump/admin2Codes.txt
fi

file=data/downloaded/countryInfo.txt
if [ -f $file ];
then
   echo "file $file exists."
else
   curl -o $file http://download.geonames.org/export/dump/countryInfo.txt
fi
cp $file countryinfo/src/main/resources/

file=data/downloaded/ne_10m_populated_places_simple.dbf
if [ -f $file ];
then
   echo "file $file exists."
else
   curl -L -o data/downloaded/ne_10m_populated_places_simple.zip http://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_populated_places_simple.zip
   unzip -d data/downloaded/ data/downloaded/ne_10m_populated_places_simple.zip
   rm data/downloaded/ne_10m_populated_places_simple.zip
fi

