#!/usr/bin/python

import sys
import csv

csv.field_size_limit(1000000000)

if len(sys.argv) == 2:
  cc = sys.argv[1]
  reader = csv.reader(open('data/downloaded/%s.txt' % cc), dialect='excel-tab')
  writer = csv.writer(open('data/computed/adminCodes-%s.txt' % cc, 'w'), dialect='excel-tab')
else:
  reader = csv.reader(open('data/downloaded/allCountries.txt'), dialect='excel-tab')
  writer = csv.writer(open('data/computed/adminCodes.txt', 'w'), dialect='excel-tab')

for row in reader:
  geonameid = row[0]
  name = row[1]
  asciiname = row[2]
  fclass = row[6]
  fcode = row[7]
  countryCode = row[8]
  adminIds = [countryCode] + row[10:14]
  adminIds = [a for a in adminIds if a]

  validFeatureCodes = ['ADM1', 'ADM2', 'ADM3', 'ADM4']

  if fclass == 'A' and fcode in validFeatureCodes:
    adminLevel = int(fcode[3])
    adminId = '.'.join(adminIds)
    if adminLevel != (len(adminIds) - 1):
      print "didn't have enough adminids for %s %s @ %s %s" % (geonameid, name, fcode, adminId)
    else:
      outputRow = [adminId, name, asciiname, geonameid]
      writer.writerow(outputRow)


