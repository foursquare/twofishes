#!/usr/bin/python

import geojson
import csv
import pymongo
import sys
import glob
from shapely.geometry import asShape

from pymongo import Connection
connection = Connection()
db = connection.geonames_global2
geonames = db.features

def loadBoundingBoxes(mappingFilename, delimiter, geojsonFiles, idPropName,
    labelPropName=None):
  mapping = {}
  mappingFile = csv.reader(open(mappingFilename), delimiter=delimiter)
# geonames -> FK
  for row in mappingFile:
    if row[1] not in mapping:
      mapping[row[1]] = []
    mapping[row[1]].append(row[0])

  for f in geojsonFiles:
    print 'Processing %s' % f
    data = geojson.loads(open(f).read())
    if 'features' in data:
      data = data['features']
    for f in data:
      id = str(f['properties'][idPropName])
      print "looking for %s" % id
      if id in mapping:
        for geonameid in mapping[id]:
          bounds = asShape(f['geometry']).bounds
          label = f['properties'][labelPropName] if labelPropName else ''
          (minlng, minlat, maxlng, maxlat) = bounds
          print (u'%s -> %s (%s) [%s,%s][%s,%s]' % (id, geonameid, label, minlat, minlng, maxlat, maxlng)).encode('utf-8')
          geonames.update({"geonameid": geonameid},
            {"$set": {"bb": {
              'ne': [maxlat, maxlng],
              'sw': [minlat, minlng]
            }}})

loadBoundingBoxes('data/geonames_woeid_bbox.tsv', '\t',
  glob.glob('data/flickr*geojson'), 'woe_id', 'label')
