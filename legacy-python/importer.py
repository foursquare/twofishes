#!/usr/bin/python
#
# Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

import csv
import pymongo
import sys
from geotools import *
from geocoder import Geocoder
from optparse import OptionParser

usage = ""
parser = OptionParser(usage=usage)
parser.add_option("-d", "--drop", dest="drop", action="store_true", default=False, help="drop database first")
parser.add_option("-a", "--alt_names", dest="alt_names", action="store_true", default=False, help="parse altnames")
parser.add_option("-u", "--us_only", dest="us_only", action="store_true", default=False, help="parse US-only")
parser.add_option("-z", "--zips", dest="import_zips", action="store_true", default=False, help="import zips")
parser.add_option("-b", "--buildings", dest="index_buildings", action="store_true", default=False, help="index buildings")
parser.add_option("--create_missing_parents", dest="create_missing_parents", action="store_true", default=False, help="try to synthesize missing zipcode parents")

(options, args) = parser.parse_args()

csv.field_size_limit(1000000000)

from pymongo import Connection
connection = Connection()
db = connection.geonames_global2
geonames = db.features

# make sure we can return a response that includes a pretty name
# create prefix? suffix? table

GEONAMEID = "geonameid",
PLACE_NAME = 'place_name',
NAME = "name",
ASCIINAME = "asciiname",
ALTERNATENAMES = "alternatenames",
LATITUDE = "latitude",
LONGITUDE = "longitude",
FEATURE_CLASS = "feature class",
FEATURE_CODE = "feature code",
COUNTRY_CODE = "country code",
CC2 = "cc2",
ADMIN1_CODE = "admin1 code",
ADMIN2_CODE = "admin2 code",
ADMIN3_CODE = "admin3 code",
ADMIN4_CODE = "admin4 code",
ADMIN1_NAME = "admin1 name",
ADMIN2_NAME = "admin2 name",
ADMIN3_NAME = "admin3 name",
POPULATION = "population",
ELEVATION = "elevation",
GTOPO30 = "gtopo30",
TIMEZONE = "timezone",
MODIFICATION_DATE = "modification date",
ACCURACY = 'accuracy'

ADM1 = 'ADM1'
ADM2 = 'ADM2'
ADM3 = 'ADM3'
ADM4 = 'ADM4'
CC = 'CC'
POP = 'POP'

import itertools
import math
import traceback

def flatten(listOfLists):
    return list(itertools.chain.from_iterable(listOfLists))

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(unicode_csv_data,
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def log(category, error, d):
  print '%s: %s -- %s' % (category, error, d)

def error(message, row):
    print "AHHHHHH"
    print message
    print row
    traceback.print_exc()
    sys.exit(1)


class Feature:
  def __init__(self, values):
    self.values = values

  def make_name(self, column, name):
    if column in self.values and self.values[column] != '':
      return '%s-%s-%s' % (name, self.country_code(), self.values[column])
    return None

  def feature_code(self): return self.values.get(FEATURE_CODE, '')
  def feature_class(self): return self.values.get(FEATURE_CLASS)

  def cc_id(self): return self.make_adminid(0)
  def admin1_id(self): return self.make_adminid(1)
  def admin2_id(self): return self.make_adminid(2)
  def admin3_id(self): return self.make_adminid(3)
  def admin4_id(self): return self.make_adminid(4)
  def admin_ids(self): return [self.admin1_id(), self.admin2_id(), self.admin3_id(), self.admin4_id()]

  def admin1_code(self): return self.values.get(ADMIN1_CODE)
  def admin2_code(self): return self.values.get(ADMIN2_CODE)
  def admin3_code(self): return self.values.get(ADMIN3_CODE)
  def admin4_code(self): return self.values.get(ADMIN4_CODE)

  def geonameid(self): return self.values.get(GEONAMEID)

  def alternate_names(self): 
    altnames = self.values.get(ALTERNATENAMES)
    if altnames:
      return altnames.split(',')
    return []

  def name(self): return self.values[NAME]

  def all_names(self):
    names = [self.values[NAME]] + self.alternate_names()
    
    if self.isCountry():
      names += [self.values[COUNTRY_CODE]]
    return names

  def population(self): return self.values[POPULATION]

  def isCountry(self): return self.feature_code().startswith('PCL')

  def country_code(self): return self.values.get(COUNTRY_CODE, 'XX')

  def latitude(self): return float(self.values.get(LATITUDE) or '0.0')
  def longitude(self): return float(self.values.get(LONGITUDE) or '0.0')

  def accuracy(self): return int(self.values.get(ACCURACY, 1) or 1) 

  def parents(self):
    if self.isCountry():
      return []

    parent_types = [ COUNTRY_CODE, ADM1, ADM2, ADM3, ADM4 ]
    parents = [self.cc_id(), self.admin1_id(), self.admin2_id(), self.admin3_id(), self.admin4_id()]

    if self.feature_code() in parent_types:
      parents = parents[0:parent_types.index(self.feature_code())]

    return list(set(filter(None, parents)))

  def make_adminid(self, level):
    parents = [self.country_code(), self.admin1_code(), self.admin2_code(), self.admin3_code(), self.admin4_code()]

    parents = parents[0:level+1]

    ret = '-'.join(filter(None, parents))
    return ret

  def featureid(self):
    featureid = ''
    if self.feature_class() == 'A':
      code = self.feature_code()
      if code.startswith('ADM'):
        level = code[3]
        if level in ['1', '2', '3', '4']:
          featureid = self.admin_ids()[int(level) - 1]
          if not featureid:
            log('bad_features', 'missing adm code', self.values)
            pass
          featureid = self.make_adminid(int(level))
      elif self.isCountry(): featureid = self.cc_id()

    if featureid:
      return featureid
    else:
      return self.geonameid()

class RewritesParser:
  def __init__(self, filename):
    csvReader = unicode_csv_reader(open(filename), delimiter='\t')
    self.rewrites = []
    
    for row in csvReader:
      print row
      if len(row) > 1:
        for n in row[1].split(','):
          self.rewrites.append([normalize(row[0]), n])

    print self.rewrites

  def getAltNames(self, name):
    names = set()
    for rewrite in self.rewrites:
      names.add(normalize(name.replace(rewrite[0], rewrite[1])))

    return names 

class TableEntry:
  def __init__(self, v):
    self.v = v
    self.used = False

  def setUsed(self):
    self.used = True

class GidTableParser: 
  def __init__(self, filename):
    self.table = {}
    csvReader = unicode_csv_reader(open(filename), delimiter='\t')

    for row in csvReader:
      if len(row) > 1:
        self.table[row[0]] = TableEntry(row[1].split(','))

    print self.table

  def getGid(self, gid):
    gid = str(gid)
    val = self.table.get(gid)
    if val is not None:
      val.setUsed()
      return val.v

    return self.defaultValue()

  def printUnused(self):
    for k in self.table:
      if not self.table[k].used:
        print "UNUSED: %s %s" % (k, self.table[k].v)

class CustomAliasesParser(GidTableParser):
  def defaultValue(self):
    return []

class BoostsParser(GidTableParser):
  def defaultValue(self):
    return None

class NameOverrideParser:
  def __init__(self, filename):
    self.table = {}
    csvReader = unicode_csv_reader(open(filename), delimiter='\t')

    for row in csvReader:
      self.table[row[0]] = row[1:]

    print self.table

  def run(self):
    for gid in self.table:
      v = self.table[gid]
      features = [g for g in geonames.find({'geonameid': gid})]
      if len(features) != 1:
        print "ERROR: %d features for %s" % (len(features), gid)
      f = features[0]
      print f
      lang = v[0]
      name = v[1]
      found = False
      for n in f['displayNames']:
        if n['l'] == lang:
          if n['s'] == name:
            n['p'] = True
            found = True
          else:
            n['p'] = False
      if not found:
        print "ERROR: didn't find %s in %s on %s" % (name, lang, f)
        f['displayNames'].append({
          'l': lang,
          's': name,
          'p': True
        })
      print f
      geonames.save(f)

class AdminParser:
  def __init__(self, rewriter, aliases, boosts):
    self.rewriter = rewriter
    self.aliases = aliases
    self.boosts = boosts

  def columnNames(self): return [
    GEONAMEID,
    NAME,
    ASCIINAME,
    ALTERNATENAMES,
    LATITUDE,
    LONGITUDE,
    FEATURE_CLASS,
    FEATURE_CODE,
    COUNTRY_CODE,
    CC2,
    ADMIN1_CODE,
    ADMIN2_CODE,
    ADMIN3_CODE,
    ADMIN4_CODE,
    POPULATION,
    ELEVATION,
    GTOPO30,
    TIMEZONE,
    MODIFICATION_DATE,
  ]
 
  def parse(self, filename):
    csvReader = unicode_csv_reader(open(filename), delimiter='\t', quotechar = '\x07')

    for count, row in enumerate(csvReader):
      if count % 1000 == 0:
        print "imported %d features so far" % count
      if len(row) != len(self.columnNames()):
        error("col length mismatch: %d vs %d" % (len(row), len(self.columnNames())))

      values = {}
      for (key, val) in zip(self.columnNames(), row):
        values[key] = val

      feature = Feature(values)

      if feature.feature_class() == 'S' and not options.index_buildings:
        next

      parents = feature.parents()

      all_names = feature.all_names() + self.aliases.getGid(values[GEONAMEID])
      try:
        all_names = [normalize(n) for n in all_names]
        all_names = flatten([rewriter.getAltNames(name) for name in all_names])
      except:
        print values
        print feature.geonameid()
        error(all_names, row)
      
      # manual airport hack
      if feature.feature_code() == 'AIRP':
        all_names += ['%s airport' % n for n in all_names]

      deaccented_names = [deaccent(n) for n in all_names]
      all_names = list(set(all_names + deaccented_names))

      record = {
        'geonameid': feature.geonameid(),
        'cc': values[COUNTRY_CODE],
        'feature_code': feature.feature_code(),
        'feature_class': feature.feature_class(),
        'names': all_names,
        'lat': feature.latitude(),
        'lng': feature.longitude(),
        'displayNames': [{
          'l': 'en',
          's': feature.name(),
          'p': True
        }]
      }

      if parents:
        record['parents'] = parents
      
      if feature.featureid():
        record['featureid'] = feature.featureid()

      if feature.population():
        record[POP] = feature.population()

      boost = self.boosts.getGid(feature.geonameid())
      if boost:
        record['boost'] = int(boost[0])

      geonames.insert(record)


class PostalCodeParser:
  def columnNames(self): return [
    COUNTRY_CODE,
    NAME, # really zipcode
    PLACE_NAME,
    ADMIN1_NAME,
    ADMIN1_CODE,
    ADMIN2_NAME,
    ADMIN2_CODE,
    ADMIN3_NAME,
    ADMIN3_CODE,
    LATITUDE,
    LONGITUDE,
    ACCURACY
  ]

  def __init__(self, filename):
    self.csvReader = unicode_csv_reader(open(filename), delimiter='\t', quotechar = '\x07')
    self.lastRow = self.csvReader.next()
    self.count = 0
    self.geocoder = Geocoder()
    self.done = None

  def makeFeature(self, row):
    if len(row) != len(self.columnNames()):
      print row
      print("col length mismatch: %d vs %d" % (len(row), len(self.columnNames())))

    values = {}
    for (key, val) in zip(self.columnNames(), row):
      values[key] = val

    return Feature(values)

  # make this a real iterator?
  def getNextFeatureSet(self):
    first_feature = self.makeFeature(self.lastRow)
    self.lastRow = None
    features = [first_feature]

    for row in self.csvReader:
      self.count += 1
      if self.count % 1000 == 0:
        print "imported %d zips so far" % self.count

      self.lastRow = row
      feature = self.makeFeature(row)
      if feature.all_names() == first_feature.all_names():
        features.append(feature)
      else:
        break

    if self.lastRow is None:
      self.done = True

    return features

  # try to find each of the admin codes
  # if we can't find it, create and return them
  def find_and_create_parents(self, feature):
    def tooFar(candidate):
      km = distance_km(feature.latitude(), feature.longitude(), candidate[0]['lat'], candidate[0]['lng'])
      return km < 20000
 
    # try to find CC, then admin1, then admin2, then admin3, then place_name
    # if we find it and the name disagrees, add that
    # if we can't find it, try geocoding for it instead
    cc = feature.country_code()
    parents = [feature.cc_id()]

    def tryGeocoder(query, record):
      parents = []
      (geocodes, meta) = self.geocoder.geocode(query)
      geocodes = filter(tooFar, geocodes)
      if len(meta['query']) == 0 and len(geocodes) > 0:
        for g in geocodes:
          parents.append(g[0]['featureid'])
          if 'parents' in g[0]:
            parents += g[0]['parents']
      else:
        record['parents'] = list(set(filter(None, record['parents'])))
        geonames.insert(record)
        parents.append(record['featureid'])
      return parents

    fcodes = [ADM1, ADM2, ADM3]
    codes = [ feature.admin1_id(), feature.admin2_id(), feature.admin3_id() ]
    names = [ feature.values[ADMIN1_NAME], feature.values[ADMIN2_NAME], feature.values[ADMIN3_NAME] ]
    
    for i in xrange(0, len(codes)):
      code = codes[i]
      name = names[i]
      if not code:
        continue
      geocodes = [g for g in geonames.find({'featureid': code, 'cc': cc,
        'parents': { '$all': filter(None,codes[0:i]) + [feature.cc_id()] }
      })]
      if len(geocodes):
        parents.append(code)
        for g in geocodes:
          if normalize(name) not in g['names']:
            geonames.update({'_id': g['_id']}, {'$addToSet': { 'names': normalize(name) }}, safe=True)
      else:
        query = ' '.join(names[0:i+1]) + ' ' + cc
        parents += tryGeocoder(query,
          {
            'cc': feature.country_code(),
            'feature_code': 'A',
            'feature_class': fcodes[i],
            'featureid': code,
            'names': [normalize(name)],
            'lat': feature.latitude(),
            'lng': feature.longitude(),
            'parents': filter(None, parents),
          })
        
    # looking for place name
    query = feature.values[PLACE_NAME] + ' ' + ' '.join(names) + ' ' + cc
    parents += tryGeocoder(query,
      {
          'cc': feature.country_code(),
          'feature_code': 'P',
          'feature_class': 'PPL',
          'featureid': normalize(feature.values[PLACE_NAME]),
          'names': [normalize(feature.values[PLACE_NAME])],
          'lat': feature.latitude(),
          'lng': feature.longitude(),
          'parents': filter(None, parents),
          'displayNames': [{
            'l': 'en',
            's': feature.name(),
            'p': True
          }]
      })

    return list(set(parents))

  def parse(self):
    while not self.done:
      featureSet = self.getNextFeatureSet()

      parents = []

      last_feature = None
      for feature in featureSet:
        if last_feature and last_feature.values[PLACE_NAME] == feature.values[PLACE_NAME]:
          continue

        if feature.country_code() == 'PT':
          next
        parents = feature.parents()
        all_names = feature.all_names()

        # with better run-time ranking, we could do this instead of synthesizing parents
        # all_names.append(last_feature.values[PLACE_NAME])
        
        if options.create_missing_parents:
          parents += self.find_and_create_parents(feature)

        last_feature = feature

      # save the first feature
      feature = featureSet[0]
      try:
        record = {
          'featureid': '%s-%s' % (feature.country_code(), all_names[0]),
          'cc': feature.country_code(),
          'feature_code': 'Z',
          'feature_class': 'ZIP',
          'names': [normalize(n) for n in all_names],
          'lat': feature.latitude(),
          'lng': feature.longitude(),
          'parents': list(set(parents)),
          'accuracy': feature.accuracy(),
          'displayNames': [{
            'l': 'en',
            's': feature.name(),
            'p': True
          }]
        }
      except:
        error(values, row)

      geonames.insert(record)


class AlternateNamesParser:
  def process(self, filename):
    csvReader = unicode_csv_reader(open(filename), delimiter='\t')

    for i, row in enumerate(csvReader):
      if i % 1000 == 0:
        print "processed %d altnames" % i
      geonameid = row[1]
      lang = row[2]
      altName = row[3]
      prefName = (row[4] == '1')
      shortName = (row[5] == '1')

      if lang == 'link':
        next

      geonames.update({"geonameid": geonameid},
        {"$push": {"displayNames": {
          'l': lang,
          's': altName,
          'p': prefName
        }}})

if __name__ == "__main__":
  aliases = CustomAliasesParser('custom/aliases.txt')
  boosts = BoostsParser('custom/boosts.txt')
  rewriter = RewritesParser('custom/rewrites.txt')

  if options.drop:
    geonames.drop()

  if options.us_only:
    AdminParser(rewriter, aliases, boosts, names).parse('data/US.txt')
  else:
    AdminParser(rewriter, aliases, boosts).parse('data/allCountries.txt')

  geonames.create_index('names')
  geonames.create_index([('featureid', pymongo.DESCENDING)])
  geonames.create_index([('geonameid', pymongo.DESCENDING)])

  if options.alt_names:
    AlternateNamesParser().process('data/alternateNames.txt')

  if options.import_zips:
    if options.us_only:
      PostalCodeParser('data/zip/US.txt').parse()
    else:
      PostalCodeParser('data/zip/allCountries.txt').parse()


  aliases.printUnused()
  boosts.printUnused()

  nameOverrides = NameOverrideParser('custom/names.txt')
  nameOverrides.run()
