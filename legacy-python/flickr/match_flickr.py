#!/usr/bin/python
import sys
import csv
import json
import geojson
import urllib
import urllib2

out = open('geonames_woeid_bbox.tsv', 'w')

files = sys.argv[2:]
for f in files:
  fdata = open(f).read()
  data = geojson.loads(fdata)
  for feature in data['features']:
    woeid = str(feature['properties']['woe_id'])
    label = feature['properties']['label']

    url = u"http://dev-blackmad:9999/?query=%s" % urllib.quote(label.encode('utf-8'))
    response = urllib2.urlopen(url)
    data = response.read()
    jsonData = json.loads(data)

    geocodes = False
    match = False

    for interp in jsonData['interpretations']:
      if interp['what']:
        break

      bbox = feature['geometry']['bbox'] 
      geocodes = True

      if (
        interp['lat'] >= bbox[1] and 
        interp['lat'] <= bbox[3] and 
        interp['lng'] >= bbox[0] and 
        interp['lng'] <= bbox[2]
      ):
        match = True
        out.write((u'%s\t%s\t%s\t%s\n' % (interp['geonameid'], woeid, label, feature['geometry']['bbox'])).encode('utf-8'))

    if not geocodes:
      print (u'No geocodes for %s %s' % (woeid, label)).encode('utf-8')
    elif not match:
      print (u'Geocodes, but no match for %s %s' % (woeid, label)).encode('utf-8')
      for interp in jsonData['interpretations']:
        bbox = feature['geometry']['bbox'] 
        print "\t%s, %s" % (interp['lat'], interp['lng'])
        print "\t%s" % bbox

