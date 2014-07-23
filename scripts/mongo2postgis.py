#!/usr/bin/python

import pymongo
from pymongo import MongoClient
import shapely
from shapely import wkb, wkt
import psycopg2
from optparse import OptionParser

parser = OptionParser(usage="%prog [options]")
parser.add_option("-d", "--database", dest="databaseName", default="gis", help="postgres database")
parser.add_option("-t", "--table", dest="tableName", default="twofishes", help="destination postgres table")
parser.add_option("-u", "--user", dest="userName", default="postgres", help="postgres user")
(options, args) = parser.parse_args()

client = MongoClient()
db = client.geocoder
features = db.features
polygons = db.polygon_index
connection = psycopg2.connect(database=options.databaseName, user=options.userName)
cursor = connection.cursor()

sql = 'CREATE TABLE %s(id bigint PRIMARY KEY, name varchar(200), source varchar(100), woeType integer, geom geometry(MultiPolygon,4326))' % options.tableName
cursor.execute(sql)

count = 0
for feature in features.find({"hasPoly": True}):
  count += 1
  if (count % 10000 == 0):
    print("processed %d features" % count)

  for polygon in polygons.find({"_id": feature['polyId']}):
    source = polygon['source']
    geom = wkb.loads(polygon['polygon'])
    wktGeom = wkt.dumps(geom)
    name = ''
    enNames = [elem for elem in feature['displayNames'] if elem['lang'] == 'en']
    if (len(enNames) > 0):
      name = enNames[0]['name']
    else:
      if (len(feature['displayNames']) > 0):
        name = feature['displayNames'][0]['name']

    sql = 'INSERT into %s VALUES(%%s, %%s, %%s, %%s, ST_Multi(ST_GeomFromText(%%s, 4326)))' % options.tableName
    cursor.execute(sql, (feature['_id'], name, polygon['source'], feature['_woeType'], wktGeom))

sql = 'CREATE INDEX ON %s USING btree(source)' % options.tableName
cursor.execute(sql)
sql = 'CREATE INDEX ON %s USING btree(woeType)' % options.tableName
cursor.execute(sql)

connection.commit()
cursor.close()
connection.close()
client.disconnect()
