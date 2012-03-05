#!/bin/sh

mongo localhost:27017/geocoder --eval "db.features.drop()"
./mongo-indexes.sh
