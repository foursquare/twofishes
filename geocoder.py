#!/usr/bin/python
#
# Copyright 2012 Foursquare Labs Inc.
#
# Wrapper for the geocoder for use from python.

import re, simplejson, sys, urllib, urllib2

class Geocode:
  # Initialize from a raw interpretation from the geocoder service.
  def __init__(self, interpretation):
    self.interp = interpretation

  def lat(self):
    return self.interp['feature']['geometry']['center']['lat']
  def lng(self):
    return self.interp['feature']['geometry']['center']['lng']

  def displayName(self):
    return self.interp['feature']['displayName']

  def ids(self):
    return self.interp['feature']['ids']

  def geonameid(self):
    for i in self.ids():
      if i['source'] == 'geonameid':
        return i['id']
    return None

class Geocoder:
  def __init__(self, host):
    self.host = host

  def geocode(self, query, otherParams = {}):
    otherParams['query'] = query
    url = 'http://%s/?%s' % (HOST, urllib.urlencode(query))
    request = urllib2.Request(url)
    response = simplejson.loads(urllib2.urlopen(request).read())
    if len(response['interpretations']) > 0:
      return Geocode(response['interpretations'][0])
    else:
      return None
