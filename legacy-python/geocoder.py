#!/usr/bin/python
#
# Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

import csv
import functools
import pymongo
import sys
import json

from geotools import *
from optparse import OptionParser

from pymongo import Connection
connection = Connection()
db = connection.geonames_global2
geonames = db.features

# todo
# online deduping
# highlighting?
# if I were me, I would add hacks for us+4, canadian+3 and GB+3 zipcodes

class Geocoder:
  def __init__(self, tryHard=True):
    self.tryHard = tryHard
    
  def find_name(self, tokens):
    name = ' '.join(tokens)
    #print 'monging for: %s' % name
    features = geonames.find({'names': name})
    return [f for f in features]

  def generate_parses(self, tokens, cache):
    if len(tokens) == 0:
      return [[]]

    # better cache key pleez
    cache_key = len(tokens)
    if cache_key not in cache:
      parses = []
      for i in xrange(1, len(tokens) + 1):
        #print "trying: %d to %d: %s" % (0, i, ' '.join(tokens[0:i]))
        features = self.find_name(tokens[0:i])
        #print "have %d matches" % len(features)
        sub_parses = self.generate_parses(tokens[i:], cache)
        for f in features:
          #print "feature: %s" % f
          for p in sub_parses:
            #print "sub_parse: %s" % p
            parse = [f] + p
            parse = sorted(parse, key=self.feature_comparator)
            if self.is_valid(parse):
              #print "Valud"
              parses.append(parse)

      #print "setting %s" % cache_key
      cache[cache_key] = parses

    return cache[cache_key]


  # this should really use the geonames class hierarchy, not parent len
  def feature_comparator(self, feature):
    if feature['feature_class'] == 'ZIP':
      return -10000
    else:
      return -1*len(feature.get('parents', []))

  def is_valid(self, parse):
    # parse is a list of features 
    if self.is_valid_helper(parse):
      return True
    else:
      #was invalid, so try zip fuzzing logic
      most_specific = parse[0]
      if most_specific['feature_class'] == 'ZIP':
        is_valid_without_zip = self.is_valid_helper(parse[1:])
        if is_valid_without_zip:
          # is our zip < 200km from the next smallest feature
          dist = distance_km(most_specific['lat'], most_specific['lng'], parse[1]['lat'], parse[1]['lng'])
          #print "%d km from %s to %s %s" % (dist, most_specific['names'][0], parse[1]['names'][0], parse[1]['featureid'])
          #print "%s,%s to %s,%s" % (most_specific['lat'], most_specific['lng'], parse[1]['lat'], parse[1]['lng'])
          if dist < 200:
            return True

      return False
      

  def is_valid_helper(self, parse): 
    most_specific = parse[0]
   
    # don't allow "spots" (buildings, etc) without tryHard
    if not self.tryHard and most_specific['feature_class'] == 'S':
      return False
    
    for feature in parse[1:]:
      if feature['_id'] == most_specific['_id']:
        continue
      if feature['cc'] != most_specific['cc']:
        return False
      if 'featureid' not in feature:
        return False
      if 'parents' not in most_specific:
        return False
      if feature['featureid'] not in most_specific['parents']:
        return False

    return True

  def filter_valid(self, parses):
    return filter(self.is_valid, parses)
 
  def geocode(self, string, ccHint=None, llHint=None, lang=None):
    def rank_parse(parse):
      signal = int(parse[0].get('POP') or 0)
      
      # if we have a repeated feature, downweight this like crazy
      # so st petersburg, st petersburg works
      # but doesn't break new york, ny
      for x in parse[1:]:
        if x['_id'] == parse[0]['_id']:
          signal -= 100000000

      # prefer a more aggressive parse ... bleh
      # this prefers "mt laurel" over "laurel" in "mt"
      signal -= 20000 * len(parse)

      if (ccHint == parse[0]['cc']):
        signal += 100000
      signal += parse[0].get('boost', 0)

      if llHint:
        dist = distance_km(llHint[0], llHint[1], parse[0]['lat'], parse[0]['lng'])
        # tie-break with distance to llHint
        signal -= dist

      # as a tie break, zipcodes in the US > zipcodes elsewhere
      if parse[0]['cc'] == 'US':
        signal += 1

      return -1*signal
 
    tokens = tokenize(normalize(string))
    connector_tokens = []
    query_tokens = []

    # try connectors
    if 'near' in tokens:
      i = tokens.index('near')
      connector_tokens = tokens[i:i+1]
      query_tokens = tokens[0:i]
      tokens = tokens[i+1:]

    cache = {}
    self.generate_parses(tokens, cache)
    best_key = len(tokens)
    if len(cache.keys()) > 0:
      for k in reversed(sorted(cache.keys())):
        if len(cache[k]) > 0:
          best_key = k
          break

    split_point = len(tokens) - best_key
    valid_parses = cache.get(best_key, [])
    sorted_valid_parses = sorted(valid_parses, key=rank_parse)
    connector = ''

    if len(sorted_valid_parses):
      query = ' '.join(tokens[0:split_point])
      geocode = ' '.join(tokens[split_point:])
      if connector_tokens:
        if query != '':
          query = ' '.join(query_tokens + connector_tokens + tokens[0:split_point])
        else:
          query = ' '.join(query_tokens)
          connector = ' '.join(connector_tokens)
    else:
      geocode = ''
      query = ' '.join(tokens)

    return (sorted_valid_parses[:5], {
      'query': query,
      'geocode': geocode,
      'connector': connector
    })

import tornado.ioloop
import tornado.web

class MainHandler(tornado.web.RequestHandler):
  def makeInterpretation(self, meta, geo, parentDict, ccHint=None, verbose=False, lang=None):
    resp = {
      'what': meta['query'],
      'where': meta['geocode'],
      'connector': meta['connector'],
    }
 
    for f in geo:
      f['_id'] = unicode(f['_id'])

    if len(geo):
      resp['cc'] = geo[0]['cc']
      resp['lat'] = geo[0]['lat']
      resp['lng'] = geo[0]['lng']
      if 'bb' in geo[0]:
        resp['geometry'] = {
          'bounds': geo[0]['bb']
        }
      if 'geonameid' in geo[0]:
        resp['geonameid'] = geo[0]['geonameid']

    parents = []
    if 'parents' in geo[0]:
      for p in geo[0]['parents']:
        if p in parentDict:
          parent = parentDict[p]
          parent['_id'] = unicode(parent['_id'])
          parents.append(parent)

    parents = sorted(parents, key=featureComparator)

    def rankName(preferAbbrev, n):
      score = 0
      if n['p']:
        score += 1
      if n['l'] == lang:
        score += 2
      if n['l'] == 'abbr' and preferAbbrev:
        score += 4
      return -1*score

    def getBestName(f, preferAbbrev):
      return sorted(f['displayNames'], key=functools.partial(rankName, preferAbbrev))[0]['s']
    
    bestName = getBestName(geo[0], False)
    names = [bestName]

    for i, p in enumerate(parents):
      if not 'displayNames' in p or not p['displayNames']:
        next
      if p['feature_code'].startswith('PCL') and geo[0]['cc'] == ccHint:
        next
 
      if p['feature_code'].startswith('PCL'):
        names.append(p['cc'])
      else:
        names.append(getBestName(p, True))

    resp['name'] = bestName
    resp['displayString'] = ', '.join(names)
    if verbose:
      resp['feature'] = geo[0]
      resp['parents'] = parents

    return resp

  def get(self):
    def boolArg(name, default):
      argStr = self.get_argument(name, str(default))
      return argStr in ['t', '1', 'true', 'True']
      
    query = self.get_argument("query")
    callback = self.get_argument("callback", None)
   
    tryHard = boolArg("tryHard", True)
    verbose = boolArg("verbose", False)
    
    ccHint = self.get_argument("ccHint", None)
    lang = self.get_argument("lang", "en")
    
    llHintStr = self.get_argument("llHint", None)
    llHint = None
    try:
      llHintParts = llHintStr.split(',')
      llHint = (float(llHintParts[0]), float(llHintParts[1]))
    except:
      pass

    geocoder = Geocoder(tryHard=tryHard)
    (g, meta) = geocoder.geocode(query, ccHint=ccHint, llHint=llHint, lang=lang)
    #print g
    geocodes = g[:3]

    parentids = []
    for geo in geocodes:
      if 'parents' in geo[0]:
        parentids += geo[0]['parents']
    parents = {}
    for p in geonames.find({'featureid': { '$in': parentids }}):
      parents[p['featureid']] = p
    
    interps = [self.makeInterpretation(meta, geo, parents, ccHint=ccHint, verbose=verbose, lang=lang) for geo in geocodes]
    resp = {'interpretations': interps}
   
    if callback:
      self.write(u'%s(%s)' % (callback, json.dumps(resp)))
    else: 
      self.write(resp)

if __name__ == "__main__":
  usage = ""
  parser = OptionParser(usage=usage)
  parser.add_option("-d", "--debug", dest="debug", action="store_true", default=False, help="auto-reload debug mode")
  (options, args) = parser.parse_args()

  app_settings = { 
    'debug': options.debug
  } 

  application = tornado.web.Application([
      (r"/", MainHandler),
      (r"/static/(.*)", tornado.web.StaticFileHandler, dict(path="static")),
  ], **app_settings)

  application.listen(int(args[0]))
  tornado.ioloop.IOLoop.instance().start()

