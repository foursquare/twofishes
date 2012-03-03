#!/usr/bin/python

import re
import csv
import pymongo
import sys
import json

from geocoder import Geocoder

csv.field_size_limit(1000000000)
  
geocoder = Geocoder()

output = open("/export/hdc3/venues/patches.txt", 'a')

count = 0
for filename in sys.argv[1:]:
  print "processing file %s" % filename
  f = open(filename)
  for line in f:
    count += 1
    if count % 1000 == 0:
      print "imported %s queries" % count
    data = json.loads(line)
    name = "%s %s %s" % (data['city'], data['state'], data['cc'])
    name = re.sub('[!.,\\/]', '', name)
    name = re.sub(' +', ' ', name)
    name = name.strip()
    g = geocoder.geocode(name)
    if len(g[0]) == 0:
      print ("saving: %s" % name).encode('utf-8')
      try:
        output.write('\t'.join([
          data['city'],
          data['state'],
          data['cc'],
          str(data['lat']),
          str(data['lng']),
          str(data['synth_count'])
        ]).encode('utf-8'))
        output.write('\n')
      except:
        pass

