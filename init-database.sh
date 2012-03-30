#!/bin/sh

echo "*** dropping previous table, if exists"
mongo localhost:27017/geocoder --eval "db.features.drop()"
mongo localhost:27017/geocoder --eval "db.name_index.drop()"
mongo localhost:27017/geocoder --eval "db.fid_index.drop()"
echo "*** creating indexes"
mongo localhost:27017/geocoder --eval "db.features.ensureIndex({'ids': -1})"
echo "*** done"
