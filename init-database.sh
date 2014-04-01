#!/bin/sh

echo "*** dropping previous table, if exists"
mongo localhost:27017/geocoder --eval "db.features.drop()"
mongo localhost:27017/geocoder --eval "db.name_index.drop()"
mongo localhost:27017/geocoder --eval "db.polygon_index.drop()"
mongo localhost:27017/geocoder --eval "db.polygon_mapping_index.drop()"
mongo localhost:27017/geocoder --eval "db.revgeo_index.drop()"
echo "*** creating indexes"
mongo localhost:27017/geocoder --eval "db.features.ensureIndex({'ids': -1})"
mongo localhost:27017/geocoder --eval "db.features.ensureIndex({'loc': '2dsphere', '_woeType': -1})"
mongo localhost:27017/geocoder --eval "db.features.ensureIndex({'_woeType': -1})"
mongo localhost:27017/geocoder --eval "db.features.ensureIndex({'hasPoly': -1})"
mongo localhost:27017/geocoder --eval "db.features.ensureIndex({'parents': -1, '_woeType': -1, 'population': -1})"
mongo localhost:27017/geocoder --eval "db.name_index.ensureIndex({'name': -1, 'pop': -1})"
mongo localhost:27017/geocoder --eval "db.revgeo_index.ensureIndex({'cellid': -1})"

echo "*** done"
