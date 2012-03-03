#!/usr/bin/python
import sys
import csv
import geojson

places = {}

print "reading geoplanet"
geoplanetfile = sys.argv[1]
lines = csv.reader(open(geoplanetfile), delimiter='\t')
for i, line in enumerate(lines):
  if i % 10000 == 0:
    print "parsed %d lines from geoplanet" % i
  places[line[0]] = [line[2].decode('utf-8'), line[5]]

def build_label(woeid):
  labels = []
  while woeid in places:
    labels.append(places[woeid][0])
    woeid = places[woeid][1]
  return u', '.join(labels)

out = open('woename_bbox.tsv', 'w')

files = sys.argv[2:]
for f in files:
  fdata = open(f).read()
  data = geojson.loads(fdata)
  for feature in data['features']:
    woeid = str(feature['properties']['woe_id'])
    label = feature['properties']['label']
    name = ''
    if woeid in places:
      name = places[woeid][0]
      if places[woeid][0] not in label:
        print (u'BAD LABEL: %s not in %s' % (places[woeid], label)).encode('utf-8')
        label = build_label(woeid)
        print (u'using: %s' % label).encode('utf-8')
    else:
      print "ERROR: %s not in places list" % woeid
    out.write((u'%s\t%s\t%s\t%s\n' % (woeid, name, label, feature['geometry']['bbox'])).encode('utf-8'))
