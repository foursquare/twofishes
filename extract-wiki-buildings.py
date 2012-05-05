#!/usr/bin/python

import os

try:
  os.mkdir("data/downloaded")
except:
  pass

alternateNames = open("data/downloaded/alternateNames.txt")
input = open("data/downloaded/allCountries.txt")
output = open("data/supplemental/buildings.txt", "w")

gidList = set()

for line in alternateNames:
  line.strip
  parts = line.split('\t')
  gid = parts[1]
  lang = parts[2]
  if lang == 'link':
    gidList.add(gid)

for line in input:
  parts = line.split('\t')
  if parts[0] in gidList and parts[6] == 'S':
    output.write(line)

