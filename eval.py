#!/usr/bin/env python
# aka carmendiff. cc dolapo.

import json
import urllib
import urllib2
import sys
import math


# TODO: move this to thrift

serverA = "http://prodproxy-b-2:35010"
serverB = "http://dev-blackmad:8081"

def getUrl(server, param):
  return server + param

def getResponse(server, param):
  try:
    response = urllib2.urlopen(getUrl(server, param))
    json_response = json.loads(response.read())
    return json_response
  except:
    return None

def earthDistance(lat_1, long_1, lat_2, long_2):
  dlong = long_2 - long_1
  dlat = lat_2 - lat_1
  a = (math.sin(dlat / 2))**2 + math.cos(lat_1) * math.cos(lat_2) * (math.sin(dlong / 2))**2
  c = 2 * math.asin(min(1, math.sqrt(a)))
  dist = 3956 * c
  return dist

for line in open(sys.argv[1]):
  param = line.strip()
  import urlparse
  params = urlparse.parse_qs(param[param.find('?'):])

  def evallog(message):
    print '%s: %s<br>' % (params['query'], message)
    print ' -- <a href="%s">serverA</a>' % (serverA + '/static/geocoder.html#' + params['query'][0])
    print ' - <a href="%s">serverB</a><p>' % (serverB + '/static/geocoder.html#' + params['query'][0])

  responseA = getResponse(serverA, param)
  responseB = getResponse(serverB, param)

  if (responseA == None and responseB == None):
    continue

  if (responseA == None and responseB != None):
    evallog('error from A, something from B')
  elif (responseB == None and responseA != None):
    evallog('error from B, something from A')
  elif (len(responseA['interpretations']) == 0 and
      len(responseB['interpretations']) == 1):
    evallog('geocoded B, not A')

  elif (len(responseA['interpretations']) == 1 and
      len(responseB['interpretations']) == 0):
    evallog('geocoded A, not B')

  elif (len(responseA['interpretations']) and len(responseB['interpretations'])):
    interpA = responseA['interpretations'][0]
    interpB = responseB['interpretations'][0]

    if interpA['feature']['ids'] != interpB['feature']['ids'] and \
        interpA['feature']['woeType'] != 11 and \
        interpB['feature']['woeType'] != 11:
      evallog('ids changed')
    else:
      centerA = interpA['feature']['geometry']['center']
      centerB = interpB['feature']['geometry']['center']
      distance = earthDistance(
        centerA['lat'], 
        centerA['lng'], 
        centerB['lat'], 
        centerB['lng'])
      if distance > 0.1:
        evallog('moved by %s miles' % distance)
