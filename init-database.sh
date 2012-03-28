#!/bin/sh

echo "*** dropping previous table, if exists"
mongo localhost:27017/geocoder --eval "db.features.drop()"
echo "*** creating indexes"
./mongo-indexes.sh
echo "*** done"
