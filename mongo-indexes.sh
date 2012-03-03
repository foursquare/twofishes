#!/bin/sh

mongo localhost:27017/geocoder --eval "db.features.ensureIndex({'p': 1})"
mongo localhost:27017/geocoder --eval "db.features.ensureIndex({'names': 1})"
