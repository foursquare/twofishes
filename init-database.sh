#!/bin/sh

mongo localhost:27017/geocoder --eval "db.features.drop()"
mongo localhost:27017/geocoder --eval "db.name_index.drop()"
mongo localhost:27017/geocoder --eval "db.fid_index.drop()"
mongo localhost:27017/geocoder --eval "db.featurelets.drop()"
