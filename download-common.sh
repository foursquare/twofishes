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