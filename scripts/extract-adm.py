#!/usr/bin/python

import sys
import csv

csv.field_size_limit(1000000000)

if len(sys.argv) == 2:
  cc = sys.argv[1]
  reader = open('data/downloaded/%s.txt' % cc)
  writer = csv.writer(open('data/downloaded/adminCodes-%s.txt' % cc, 'w'), dialect='excel-tab')
else:
  reader = open('data/downloaded/allCountries.txt')
  writer = csv.writer(open('data/downloaded/adminCodes.txt', 'w'), dialect='excel-tab')

index = 0
for line in reader:
  row = line.split('\t')
  geonameid = row[0]
  name = row[1]
  asciiname = row[2]
  fclass = row[6]
  fcode = row[7]
  countryCode = row[8]
  adminIds = [countryCode] + row[10:14]
  adminIds = [a for a in adminIds if a]

  index += 1 

  validFeatureCodes = ['ADM1', 'ADM2', 'ADM3', 'ADM4']

  if fclass == 'A' and fcode in validFeatureCodes:
    adminLevel = int(fcode[3])
    adminId = '.'.join(adminIds)

    if adminLevel != (len(adminIds) - 1):
      #print "didn't have enough adminids for %s %s @ %s %s" % (geonameid, name, fcode, adminId)
      pass
    else:
      outputRow = [adminId, name, asciiname, geonameid]
      writer.writerow(outputRow)


